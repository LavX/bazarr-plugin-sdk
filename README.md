# bazarr-plugin-sdk

The contract between a plugin and the Bazarr+ host. Standard library only, typed.

A plugin is a class of one kind. The host starts it in its own worker process,
asks it to do work, and answers the calls it makes back (holds on spending,
usage, checkpoints). The Python interface of each kind is the contract. The wire
between worker and host is private to this package and Bazarr+.

Bazarr+ carries a copy of this package and puts it on the worker's path, so a
plugin does not list it as a dependency. Install it only to develop or test a
plugin: `pip install git+https://github.com/LavX/bazarr-plugin-sdk@v0.1.0`.

## A translator plugin

```python
from bazarr_plugin_sdk import PluginContext, PluginError
from bazarr_plugin_sdk.translator.v1 import TranslatorPlugin


class MyTranslator(TranslatorPlugin):
    max_in_flight = 2  # translate calls at once

    def describe(self):
        return {"backend_types": ["my_service"]}

    def validate(self, config):
        if not config.get("model"):
            raise PluginError("invalid_config", "Pick a model", field_errors={"model": "required"})
        return config

    async def test(self, config, secrets, ctx=None):
        return {"ok": True}

    async def translate(self, request, ctx: PluginContext):
        hold = await ctx.reserve(0.02, "my-model")      # stored before the request goes out
        ctx.progress(0, 10, "starting")
        ...                                              # call the service; stop if ctx.is_cancelled
        await ctx.usage(1200, 0.015, "my-model", [hold])  # settles the hold
        await ctx.checkpoint({"batch": 1})                # resumable state
        return {"lines": [...]}
```

The manifest's runtime entry names the class as `package.module:MyTranslator`.

Methods may be `async def` or plain `def`. A plain method runs on its own thread;
use the `_sync` forms there (`ctx.reserve_sync`, `ctx.usage_sync`,
`ctx.checkpoint_sync`) and read `ctx.is_cancelled`. In an `async def` method a
cancel also cancels the task and sets `ctx.cancelled`.

Errors: raise `PluginError(code, message, retry_after=..., field_errors=...,
reason=...)` with a code from `ERROR_CODES`: `auth_required`, `invalid_config`,
`rate_limited`, `quota_exhausted`, `dependency_unavailable`, `declined`,
`cancelled`, `partial`, `internal`. Any other exception becomes `plugin_error`
with its traceback tail.

## The context

| Member | Meaning |
| --- | --- |
| `progress(done, total, message="")` | Report progress. Thread-safe, throttled, dropped after a cancel. |
| `heartbeat()` | Keep-alive for a long step. |
| `reserve(amount, model)` / `reserve_sync` | Hold the worst-case charge of a paid call. Returns the hold id once stored. Refused after a cancel. |
| `usage(tokens, cost, model, reservations)` / `usage_sync` | Settle holds against actual use. Allowed after a cancel. |
| `checkpoint(data)` / `checkpoint_sync` | Store resumable state. Returns once stored. |
| `cancelled`, `is_cancelled`, `raise_if_cancelled()` | Cancellation. |
| `budget` | What a paid `test` may spend. |
| `data_dir`, `temp_dir`, `artifacts_dir` | The plugin's folders (from the worker's environment). |
| `heartbeat_interval` | Seconds the host expects between keep-alives. |
| `logger` | Writes to stderr, which the host collects. |

Secrets reach `options` and `test` as the `secrets` argument and `translate`
as `request["secrets"]`, for that call only.

## Cancelling a call

The host cancels one call by its request id. What that does to the call, to the
tasks it started and to the other calls in the same worker (`max_in_flight` above 1):

- **Only that call is told.** `ctx.cancelled` and `ctx.is_cancelled` change for the
  cancelled call alone. Every call has its own context, its own thread and, for an
  `async def` method, its own event loop, so a sibling call keeps running. State you
  keep on `self` or at module level is shared by all calls and is never cancelled for
  you.
- **The call itself.** An `async def` method has its task cancelled, so `await` raises
  `asyncio.CancelledError`. A plain method is not interrupted: it must check
  `ctx.is_cancelled` and return, or raise. `reserve` is refused after a cancel;
  `usage` and `checkpoint` still go out, so a call can settle its holds while it stops.
- **What the call left behind.** Once the call has ended or been cancelled, the SDK
  cancels the tasks it started with `create_task`, closes the async generators it left
  open and waits for the executor threads it used (`run_in_executor`,
  `asyncio.to_thread`). A thread cannot be interrupted, so such a thread has to end on
  its own.
- **One budget.** All of that, plus your own settle calls, shares one deadline of
  3.5 seconds from the cancel, not 3.5 seconds per step. The answer goes out when the
  work ends or the budget is spent, which is well inside the host's 5 second
  `cancel_grace`.
- **When something does not stop** (a task that swallows `CancelledError`, a generator
  or thread that outlives the budget, a cleanup step that raises, a call that ignores
  its cancel), it is left running and the worker is retired. The call is still answered
  (as cancelled, or with its result), and the worker then refuses new calls with
  `shutting_down`. Its sibling calls that are still running are cancelled with the
  reason `shutdown` and get the host's shutdown grace to finish. The worker exits and
  the host starts a fresh one. A new call never runs beside code that was told to stop
  and did not, so catch `CancelledError` only to clean up, then let it go.

## Files inside the sandbox

A worker runs in a sandbox that refuses calls which change file metadata:
`chmod`, `chown`, `utime` and the extended attribute calls fail with `EPERM`.
Several standard library functions use them without saying so, and fail in a
worker: `shutil.copy`, `copy2`, `copymode`, `copystat`, `copytree`,
`shutil.move` across filesystems, `Path.touch` on an existing file and
`venv.create`. Use `bazarr_plugin_sdk.fs` instead:

| Instead of | Use |
| --- | --- |
| `shutil.copy`, `shutil.copy2` | `fs.copy_file(src, dst)` |
| `shutil.copytree` | `fs.copy_tree(src, dst)` |
| `shutil.move` | `fs.move(src, dst)` |
| `Path.touch()` | `fs.touch(path)` |
| `Path.write_text`, `write_bytes` | `fs.write_file(path, data)` (also atomic) |

They move bytes and names only, so a copy gets the default mode of a new file
and never the mode, owner or times of its source. Writing and renaming inside the
plugin's data and temp folders works as usual. Do not create virtual
environments or run installers from a worker.

## Testing

```python
from bazarr_plugin_sdk.testing import run_conformance

def test_conformance():
    run_conformance(
        MyTranslator,
        kind_api="bazarr.translator.v1",
        samples={"translate": {"args": {"request": {...}}, "secrets": {...}}},
    )
```

`LoopbackHost` (in process) and `ProcessHost` (a real worker process) are a
small fake host: they answer the handshake, `reserve`, `usage` and
`checkpoint`, and give each request's progress, result and error.

## Running a worker

```
python -I bazarr_plugin_sdk/_bootstrap.py --entry package.module:Class --path /path/to/bundle
```

Frames travel on the original stdin and stdout. The worker moves descriptor 1
to stderr and descriptor 0 to the null device before any plugin code is
imported, so a stray `print()` cannot corrupt them.

## Layout

`_protocol/frames.py` and `_protocol/worker_channel.py` are copies of the host's
files and are not edited here. Everything else is this package's own.

MIT licensed.
