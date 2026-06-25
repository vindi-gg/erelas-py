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
| `ERELAS_ENABLED` | `true` | set false to no-op every ping |
| `ERELAS_ASYNC` | `true` | set false to send synchronously (tests) |

```python
from erelas import Erelas
erelas = Erelas(base_url="https://erelas.example.com", group="pz", api_key="erl_...")
```
