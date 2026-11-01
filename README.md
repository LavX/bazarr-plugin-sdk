# bazarr-plugin-sdk

The Python SDK for writing plugin workers that a Bazarr host runs in a
separate process. A plugin is a class with one or more operations. The SDK
gives each operation a context for progress reports, heartbeats, cost
control, checkpoints and folders. It also gives the shared error vocabulary,
the worker runtime that speaks the framed protocol, and a conformance suite
that runs your plugin against a scripted host.

Status: version 0.1.0, alpha. The package is not yet published to PyPI.

## Requirements

- Python 3.12 or newer.
- A POSIX system. Passing descriptors on Windows is not verified.
- No runtime dependencies. The package uses only the standard library.

## Install

From a checkout:

```bash
pip install .
```

## A minimal plugin

```python
from collections.abc import Mapping

from bazarr_plugin_sdk import Plugin, PluginContext, PluginError, Request, serve


class ExamplePlugin(Plugin):
    kind_api = "example.kind.v1"
    operations = ("search",)

    async def search(self, ctx: PluginContext, request: Request) -> Mapping[str, object]:
        api_key = request.secrets.get("api_key")
        if not api_key:
            raise PluginError("auth_required", "an API key is needed")
        ctx.progress(0, 1, "searching")
        return {"matches": []}


if __name__ == "__main__":
    raise SystemExit(serve(ExamplePlugin))
```

The class sets two class attributes:

- `kind_api`: the interface the plugin implements. The host asks for a kind in
  the handshake. A worker that does not serve the asked kind exits with
  status 78.
- `operations`: the method names the host may call. Each name is a method on
  the class.

Each operation takes `(ctx, request)` and returns one of:

- a mapping of JSON values, which becomes the result value;
- a `Result(value=..., body=...)`, when the result has a binary body;
- None, which returns an empty result.

An operation declared with `async def` receives a `PluginContext` and awaits
its calls. An operation declared with plain `def` runs in a worker thread and
receives a `BlockingContext`. Its awaited calls block the thread instead.
Do not call a `BlockingContext` awaited method from the event loop thread.

The host creates one instance of the class per worker process. The instance
lives as long as the worker, so state held on it must be safe to share between
operations.

## The request

`Request` has these fields:

- `method`: the operation name the host called.
- `params`: the JSON object the host sent.
- `secrets`: a mapping of credential names to text. Credentials appear only
  here. The SDK never writes them to frames or to standard error.
- `body`: the request body as bytes, or None when the call sent none.

## The context

`PluginContext` (for `async def` operations) and `BlockingContext` (for `def`
operations) share the same attributes:

| Attribute or method | Behaviour |
| --- | --- |
| `data_dir`, `temp_dir`, `artifacts_dir` | `pathlib.Path` folders. They stay the same for the life of the worker. |
| `budget` | The mapping the host sent with the call, such as a cost ceiling. Its keys belong to the kind. |
| `cancelled` | A `threading.Event` that is set when the host cancels the operation. |
| `heartbeat_interval` | Seconds the host allows between heartbeats. |
| `log` | A `logging.Logger` that writes to standard error. |
| `progress(done, total=None, message="")` | Reports progress and returns at once. |
| `heartbeat()` | Tells the host the operation is alive and returns at once. |
| `reserve(amount, model)` | Awaited. Returns a reservation id, or None when the host grants none. |
| `usage(tokens, cost, model, reservations=())` | Awaited. Records tokens and cost against the reservations. |
| `checkpoint(data)` | Awaited. Stores bytes so a later attempt can resume. At most 8 MiB. |

The awaited methods return only after the host has acknowledged them. After
`checkpoint` returns, the host has stored the bytes. After `usage` returns,
the cost is recorded.

Send a heartbeat often enough to stay inside `heartbeat_interval` during long
work. A long loop with no progress or heartbeat can be treated as stalled.

When the host cancels an operation, the `cancelled` event is set and any
awaited call in progress is interrupted. Check `ctx.cancelled.is_set()` between
steps of long work.

`OperationAbandoned` is raised by context calls after the host has discarded
the operation. Let it propagate. Retrying does not help.

`HostGone` is raised when the host connection has closed. Let it propagate.

## Errors

Raise `PluginError` with one of the nine codes. The host shows the code to the
user and decides what to do next.

| Code | Use it when |
| --- | --- |
| `auth_required` | Credentials are missing, expired or rejected. |
| `invalid_config` | Configuration is wrong. Pass `field_errors={"name": "message"}`. |
| `rate_limited` | The upstream service asked for a pause. Pass `retry_after=seconds`. |
| `quota_exhausted` | A budget or quota is used up. |
| `dependency_unavailable` | A service the plugin needs is down or unreachable. |
| `declined` | The plugin will not handle this request. Pass `reason="token"`. |
| `cancelled` | The operation stopped because the host cancelled it. |
| `partial` | Some of the work completed and some did not. |
| `internal` | Any other failure. |

Passing an optional field with another code raises `ValueError`. The message
is cut to 500 characters.

Any other exception becomes an `internal` error. The host sees the exception
class name and its text, cut to 500 characters. The traceback goes to standard
error, not to the host.

## Bodies

Results and checkpoints can carry binary bodies. The SDK splits them into
chunks of up to 1 MiB and reassembles them on the host side.

- A result body may be at most `max(1 MiB, 4 x request body)`, capped at 16 MiB.
  A larger result becomes an `internal` error.
- A checkpoint body may be at most 8 MiB.
- A request body may be at most 16 MiB.

## Exit statuses

`serve(MyPlugin)` returns the status and the entry point passes it to
`SystemExit`.

| Status | Meaning |
| --- | --- |
| 0 | Orderly shutdown, or the host closed the connection. |
| 2 | The environment is wrong, for example a missing descriptor variable or no `kind_api`. |
| 76 | Protocol error. |
| 78 | The host asked for a kind this worker does not serve. |

## Testing your plugin

The testing package runs your plugin in a child process with the same frame
descriptors a host passes. No Bazarr host is needed.

Drive one worker by hand with `FakeHost`:

```python
from bazarr_plugin_sdk.testing import FakeHost


def test_search_returns_matches(tmp_path):
    with FakeHost(ExamplePlugin, kind_api="example.kind.v1", root=tmp_path) as host:
        host.start()
        outcome = host.call("search", {"q": "x"}, secrets={"api_key": "k"})
        assert outcome.kind == "result"
```

Run the kind-agnostic conformance checks with `run_conformance`:

```python
from bazarr_plugin_sdk.testing import run_conformance

FIXTURES = {
    "valid": {"method": "search", "params": {"q": "x"}},
    "cancel": {"method": "search", "params": {"q": "slow"}},
    "heartbeat": {"method": "search", "params": {"q": "beat"}},
    "raise": {"method": "search", "params": {"q": "boom"}},
    "oversize": {"method": "search", "params": {"q": "big"}},
}


def test_conformance():
    report = run_conformance(ExamplePlugin, "example.kind.v1", FIXTURES)
    assert report.ok, report.describe()
```

The fixtures tell the suite which calls to make. A check whose fixture is
missing is reported as skipped. The checks cover the handshake, the folders,
secret handling, cancellation, heartbeats, unexpected exceptions and the result
limit.

## Protocol

The frame format, the handshake fields, the size limits and the error frames
are described in [docs/protocol.md](docs/protocol.md). Plugin authors do not
need that page for normal work.

## License

MIT. See [LICENSE](LICENSE).
