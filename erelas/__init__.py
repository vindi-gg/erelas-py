"""Erelas — tiny heartbeat / cron-job monitoring client.

Decorate or wrap a callable; start/complete/fail telemetry (with duration) is
emitted around each run. Pure Python and framework-agnostic — works in Celery
tasks, Django management commands, plain cron scripts, anything.

Two ways to address a monitor:

* **GUID ping key** (public, no API key) — pass ``key=`` (the monitor's
  ``ping_key``). Hits ``/p/<key>``.
* **group + name** (auto-provisions, needs an API key) — pass ``name=`` (and
  optionally ``group=``) plus an API key (``ERELAS_API_KEY`` or ``api_key=``).
  Hits ``/m/<group>/<name>`` with an ``Authorization: Bearer`` header.

Delivery is fire-and-forget: pings are queued and flushed by a daemon thread so
they never block (or break) the job. If Erelas is unreachable — not deployed,
DNS unresolvable, timing out — every ping degrades to a silent no-op.

    from erelas import erelas              # default instance, configured by env

    @shared_task(name="cron_clear_sessions")
    @erelas.job("clear-sessions", group="pz")     # innermost
    def clear_sessions():
        ...

    with erelas.monitor("nightly-export", group="pz"):
        with erelas.step("extract"):       # per-phase timing -> waterfall on the dashboard
            rows = extract()
        with erelas.step("load"):
            load(rows)

    erelas.ping(key="abc123")              # one-off success ping

Configuration (env vars, or pass to ``Erelas(...)``):
    ERELAS_BASE_URL   default http://dev.erelas.lan
    ERELAS_API_KEY    required for group/name pings (not for key= pings)
    ERELAS_DEFAULT_GROUP   default group for name pings; override per-call with group= (optional)
    ERELAS_ENVIRONMENT     environment tag (e.g. "production") shown on alerts; omitted when unset (optional)
    ERELAS_ENABLED    set false to no-op every ping (tests / off-dashboard runs)
    ERELAS_ASYNC      set false to send synchronously (simpler in tests)

The environment tag resolves per call: explicit environment= argument -> instance value ->
the ERELAS_ENVIRONMENT *OS* env var (os.environ, not your app's settings module). To reuse a
value you already have in app config, set it on the client instead of adding a second env var::

    from decouple import config
    from erelas import erelas
    erelas.environment = config("ENV")        # settings.py; or Erelas(environment=config("ENV"))
"""
import atexit
import functools
import json
import logging
import os
import queue
import re
import threading
import time
import traceback
from contextlib import contextmanager
from contextvars import ContextVar
from uuid import uuid4

import requests

__version__ = "0.4.0"
__all__ = ["Erelas", "erelas", "slugify", "step", "START", "OK", "FAIL"]

logger = logging.getLogger(__name__)

# Internal run states. Rendered differently per transport (see Erelas.ping).
START = "start"
OK = "ok"
FAIL = "fail"

# Path suffix for the GUID endpoint (/p/<key>[/<suffix>]).
_KEY_SUFFIX = {START: "start", OK: "", FAIL: "fail"}
# ?state= value for the named endpoint (/m/<group>/<slug>).
_NAMED_STATE = {START: "run", OK: "complete", FAIL: "fail"}

# Delivery retry policy for the async pinger. A dropped completion ping orphans the
# server-side run, so retry transient failures (network errors, timeouts, 5xx/429).
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF = (0.5, 1.5)  # seconds between attempts

# Per-step timing. step() blocks append {name, ms} to the active run; the list ships
# in the finish ping's POST body. Caps keep the payload bounded and nudge callers
# toward stable, low-cardinality step names so the server can aggregate them.
_STEP_NAME_MAXLEN = 80
_MAX_STEPS = 100

# The run currently being timed (set by monitor()/job()). Lets step() attach to it
# without threading a handle through the call stack — e.g. from inside a Django
# management command invoked by the decorated task. None outside a monitored run.
_active_run = ContextVar("erelas_active_run", default=None)


class _Run:
    """Accumulates per-step timings for one monitored run."""

    __slots__ = ("steps",)

    def __init__(self):
        self.steps = []

    def add(self, name, seconds):
        if len(self.steps) >= _MAX_STEPS:
            return
        self.steps.append({"name": str(name)[:_STEP_NAME_MAXLEN], "ms": int(round(seconds * 1000))})


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(value):
    """Lowercase, collapse non-alphanumerics to '-', trim. Matches the server."""
    return _SLUG_RE.sub("-", str(value).strip().lower()).strip("-")


def describe_exc(exc):
    """Compact one-line error summary: 'ValueError: bad input @ tasks.py:42'."""
    where = ""
    tb = getattr(exc, "__traceback__", None)
    frames = traceback.extract_tb(tb) if tb else []
    if frames:
        last = frames[-1]
        where = " @ %s:%d" % (last.filename.rsplit("/", 1)[-1], last.lineno)
    return "%s: %s%s" % (type(exc).__name__, exc, where)


def _env(key, default=None):
    return os.environ.get(key, default)


def _env_bool(key, default):
    raw = os.environ.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


class Erelas:
    """A configured Erelas endpoint. Construct once, reuse everywhere.

    Customize by env (see module docstring) or per-instance::

        erelas = Erelas(base_url="https://erelas.example.com",
                        group="pokemon-zone", api_key="erl_...")

    ``enabled=False`` no-ops every ping. ``async_=False`` sends synchronously.
    """

    def __init__(
        self,
        base_url=None,
        group=None,
        api_key=None,
        *,
        environment=None,
        timeout=5,
        enabled=None,
        async_=None,
        queue_maxsize=1000,
        message_maxlen=500,
    ):
        # Stored as overrides; when None the value resolves from the environment
        # lazily (at use time) so env set after import — e.g. by Django settings
        # loaded after this module — still takes effect.
        self._base_url = base_url
        self._group = group
        self._api_key = api_key
        self._environment = environment
        self._enabled = enabled
        self._async = async_
        self.timeout = timeout
        self.message_maxlen = message_maxlen
        self._queue = queue.Queue(maxsize=queue_maxsize)
        self._worker = None
        self._lock = threading.Lock()

    # -- lazily-resolved config (explicit override wins, else env) -----------
    @property
    def base_url(self):
        raw = self._base_url if self._base_url is not None else _env("ERELAS_BASE_URL", "http://dev.erelas.lan")
        return raw.rstrip("/")

    @base_url.setter
    def base_url(self, value):
        self._base_url = value

    @property
    def group(self):
        return self._group if self._group is not None else _env("ERELAS_DEFAULT_GROUP")

    @group.setter
    def group(self, value):
        self._group = value

    @property
    def environment(self):
        # Tags each ping with its environment (e.g. "production"); the server shows it on alerts.
        return self._environment if self._environment is not None else _env("ERELAS_ENVIRONMENT")

    @environment.setter
    def environment(self, value):
        self._environment = value

    @property
    def api_key(self):
        return self._api_key if self._api_key is not None else _env("ERELAS_API_KEY")

    @api_key.setter
    def api_key(self, value):
        self._api_key = value

    @property
    def enabled(self):
        return self._enabled if self._enabled is not None else _env_bool("ERELAS_ENABLED", True)

    @enabled.setter
    def enabled(self, value):
        self._enabled = value

    @property
    def async_(self):
        return self._async if self._async is not None else _env_bool("ERELAS_ASYNC", True)

    @async_.setter
    def async_(self, value):
        self._async = value

    # -- delivery -----------------------------------------------------------
    def _ensure_worker(self):
        if self._worker is not None:
            return
        with self._lock:
            if self._worker is not None:
                return
            t = threading.Thread(target=self._drain, name="erelas-pinger", daemon=True)
            t.start()
            self._worker = t
            atexit.register(self.flush)

    def _http(self, url, params, headers, body):
        if body:
            resp = requests.post(url, params=params, data=body, headers=headers, timeout=self.timeout)
        else:
            resp = requests.get(url, params=params, headers=headers, timeout=self.timeout)
        # Transient server errors are worth retrying; 4xx (bad key, etc.) are permanent.
        if resp.status_code >= 500 or resp.status_code == 429:
            raise RuntimeError("erelas server returned %d" % resp.status_code)

    def _deliver(self, url, params, headers, body):
        """Send one ping, retrying transient failures so a dropped completion ping
        doesn't orphan the run. Runs on the daemon thread — never blocks the caller."""
        for attempt in range(_MAX_ATTEMPTS):
            try:
                self._http(url, params, headers, body)
                return
            except Exception as exc:  # never let a failed ping escape
                if attempt + 1 >= _MAX_ATTEMPTS:
                    logger.debug("erelas ping gave up url=%s after %d attempts: %s", url, _MAX_ATTEMPTS, exc)
                    return
                time.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])

    def _drain(self):
        while True:
            url, params, headers, body = self._queue.get()
            try:
                self._deliver(url, params, headers, body)
            finally:
                self._queue.task_done()

    def _send(self, url, params, headers, body=None):
        if not self.async_:
            try:
                self._http(url, params, headers, body)
            except Exception as exc:
                logger.debug("erelas ping failed url=%s: %s", url, exc)
            return
        try:
            self._ensure_worker()
            self._queue.put_nowait((url, params, headers, body))
        except queue.Full:
            logger.debug("erelas queue full, dropping ping url=%s", url)
        except Exception as exc:  # enqueue must never break the caller
            logger.debug("erelas enqueue failed url=%s: %s", url, exc)

    def flush(self, timeout=5.0):
        """Best-effort: wait until queued pings are actually *delivered* (used at exit /
        in tests). Waits on unfinished_tasks — decremented only after a ping (and its
        retries) completes — so a cron process doesn't exit mid-send and drop its
        closing ping. Bounded by ``timeout`` since delivery is still best-effort."""
        deadline = time.monotonic() + timeout
        while self._queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.02)

    # -- pings --------------------------------------------------------------
    def ping(
        self,
        state=OK,
        *,
        key=None,
        name=None,
        group=None,
        series=None,
        duration=None,
        exit_code=None,
        message=None,
        period=None,
        grace=None,
        environment=None,
        traceback=None,
        steps=None,
    ):
        """Fire a single heartbeat ping. Best-effort: never raises."""
        if not self.enabled:
            return

        params = {}
        headers = {}
        # POST body (form-encoded). Traceback and per-step timings are too big / too
        # structured for query params; steps go as a compact JSON string field.
        body = {}
        if traceback:
            body["traceback"] = traceback[:8000]
        if steps:
            body["steps"] = json.dumps(steps, separators=(",", ":"))
        body = body or None
        if series is not None:
            params["series"] = series
        if duration is not None:
            params["duration"] = "%.3f" % duration
        if exit_code is not None:
            params["exit_code"] = exit_code
        if message:
            params["message"] = message[: self.message_maxlen]
        # Environment tag — only sent when set; an unset env adds nothing to the ping.
        env = environment if environment is not None else self.environment
        if env:
            params["env"] = env

        if key:
            # GUID endpoint — public, no API key. State goes in the path.
            url = "%s/p/%s" % (self.base_url, key)
            suffix = _KEY_SUFFIX[state]
            if suffix:
                url = "%s/%s" % (url, suffix)
        elif name:
            # Named endpoint — auto-provisions, needs an API key (Bearer).
            if not self.api_key:
                logger.debug("erelas: group/name ping needs an API key; skipping %r", name)
                return
            grp = group if group is not None else self.group
            slug = slugify(name)
            path = "%s/%s" % (slugify(grp), slug) if grp else slug
            url = "%s/m/%s" % (self.base_url, path)
            params["state"] = _NAMED_STATE[state]
            if period is not None:
                params["period"] = period
            if grace is not None:
                params["grace"] = grace
            headers["Authorization"] = "Bearer %s" % self.api_key
        else:
            logger.debug("erelas ping skipped: no key or name")
            return

        self._send(url, params, headers, body)

    # -- wrappers -----------------------------------------------------------
    @contextmanager
    def monitor(self, name=None, *, key=None, group=None, series=None, period=None, grace=None, environment=None):
        """Context manager: emits start, then complete or fail with timing.

        Time named sub-steps with ``step()`` to ship a per-phase breakdown
        (rendered as a waterfall on the dashboard) on the finish ping::

            with erelas.monitor("nightly-export", group="pz"):
                with erelas.step("extract"):
                    rows = extract()
                with erelas.step("load"):
                    load(rows)
        """
        ident = dict(
            key=key, name=name, group=group, series=series or uuid4().hex,
            period=period, grace=grace, environment=environment,
        )
        run = _Run()
        token = _active_run.set(run)
        started = time.monotonic()
        self.ping(START, **ident)
        try:
            yield run
        except BaseException as exc:
            self.ping(
                FAIL,
                duration=time.monotonic() - started,
                message=describe_exc(exc),
                traceback=traceback.format_exc(),
                steps=run.steps or None,
                **ident,
            )
            raise
        finally:
            _active_run.reset(token)
        self.ping(OK, duration=time.monotonic() - started, steps=run.steps or None, **ident)

    @contextmanager
    def step(self, name):
        """Time a named sub-step of the active monitored run.

        Records ``{name, ms}`` against the run started by the enclosing
        ``monitor()``/``job()``, which ships it on the finish ping. Outside a
        monitored run (or when the client is disabled) it just runs the body, so
        it's safe to leave in code that sometimes runs unmonitored::

            with erelas.step("db-query"):
                rows = run_query()
        """
        run = _active_run.get()
        started = time.monotonic()
        try:
            yield
        finally:
            if run is not None and self.enabled:
                run.add(name, time.monotonic() - started)

    def job(self, name=None, *, key=None, group=None, series=None, period=None, grace=None, environment=None):
        """Decorator, drop-in for ``@cronitor.job``. Place it innermost::

            @shared_task(...)
            @erelas.job("clear-sessions", group="pz")
            def my_task():
                ...
        """

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                with self.monitor(name, key=key, group=group, series=series,
                                  period=period, grace=grace, environment=environment):
                    return func(*args, **kwargs)

            return wrapper

        return decorator


# Default shared instance — import and use directly, like the cronitor lib.
erelas = Erelas()

# Bare handle bound to the default instance, for `from erelas import step`.
step = erelas.step
