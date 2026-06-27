# erelas (Python client)

Tiny, pure-Python heartbeat / cron-job monitoring client for [Erelas](../../). Decorate
or wrap a job; `start` / `complete` / `fail` telemetry (with duration) is sent around each
run. Fire-and-forget — pings never block or break your job, and if Erelas is unreachable
they degrade to a silent no-op.

Only dependency: `requests`. Works in Celery tasks, Django management commands, plain cron
scripts — it knows nothing about any framework.

## Install

```bash
pip install erelas                      # (once published)
pip install -e clients/python           # local / editable, from the erelas repo
```

## Use

```python
from erelas import erelas

# Decorator (drop-in for @cronitor.job) — put it innermost, under @shared_task:
@erelas.job("clear-sessions", group="pz")
def clear_sessions():
    ...

# Context manager:
with erelas.monitor("nightly-export", group="pz"):
    do_work()

# One-off pings:
erelas.ping(key="b8263...")                      # public GUID, no key needed
erelas.ping(name="clear-sessions", group="pz")   # by name (needs ERELAS_API_KEY)
```

## Step timings

Wrap phases of a job in `erelas.step("name")` to record a per-phase breakdown. The
timings ship on the finish ping and render as a waterfall on the run, so you can see
*which* phase is slow — not just the total:

```python
@erelas.job("premium-sync", period="10m")
def premium_sync():
    with erelas.step("patreon-fetch"):
        members = fetch_patreon()
    with erelas.step("resolve-users"):
        users = resolve(members)
    with erelas.step("write-cache"):
        cache.write(users)
```

`step()` finds the active run started by the enclosing `@erelas.job`/`monitor()` via a
`ContextVar`, so it works even when the steps live in a different function (e.g. a Django
management command the task calls). Outside a monitored run — or when the client is
disabled — `step()` just runs the body, so it's safe to leave in place. Keep step names
**stable and low-cardinality** (`"fetch-page"`, not `"fetch-page-3187"`); per-run names
are capped (100 steps, 80 chars each) and defeat aggregation.

## Addressing a monitor

| Mode | Pass | Endpoint | Auth |
| --- | --- | --- | --- |
| **GUID** | `key=` (the `ping_key`) | `/p/<key>` | none — the GUID is the credential |
| **group + name** | `name=` (+ `group=`) | `/m/<group>/<name>` | `ERELAS_API_KEY` (Bearer) — auto-provisions |

Name-mode auto-creates the monitor (and group) on first ping; pass `period=`/`grace=`
(e.g. `period="1d"`, `grace="1h"`) to set its schedule when provisioning.

## Configuration

Env vars (or pass to `Erelas(...)`):

| Var | Default | Purpose |
| --- | --- | --- |
| `ERELAS_BASE_URL` | `http://dev.erelas.lan` | server base URL |
| `ERELAS_API_KEY` | — | required for group/name pings |
| `ERELAS_DEFAULT_GROUP` | — | default group for name pings (override per-call with `group=`) |
| `ERELAS_ENVIRONMENT` | — | environment tag (e.g. `production`) shown on alerts; omitted when unset |
| `ERELAS_ENABLED` | `true` | set false to no-op every ping |
| `ERELAS_ASYNC` | `true` | set false to send synchronously (tests) |

```python
from erelas import Erelas
erelas = Erelas(base_url="https://erelas.example.com", group="pz", api_key="erl_...")
```

### Environment tag

Each ping can carry an environment (`production`, `staging`, …) that the server shows on
alerts. It's resolved per call: explicit `environment=` argument → instance value → the
`ERELAS_ENVIRONMENT` OS env var. When none is set, nothing is sent.

`ERELAS_ENVIRONMENT` is an **OS environment variable**, read from `os.environ` — not a setting
in your app's config module. If you already have the value in app config (e.g. Django settings
with `python-decouple`), set it on the client instead of adding a second env var:

```python
# settings.py — reuse your existing ENV, no redundant ERELAS_ENVIRONMENT needed
from decouple import config
from erelas import erelas

erelas.environment = config("ENV")                       # or: config("ENV", default="development")
```

Per-call override (wins over both the instance value and the env var):

```python
with erelas.monitor("nightly-export", group="pz", environment="production"):
    do_work()
```
