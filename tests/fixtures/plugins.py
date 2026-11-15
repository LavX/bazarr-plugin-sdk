"""Plugins the tests serve. Importable by entry (``tests.fixtures.plugins:Name``)."""

import asyncio
import logging
import os
import sys
import threading
import time

from bazarr_plugin_sdk import PluginError
from bazarr_plugin_sdk.translator.v1 import TranslatorPlugin


if os.environ.get("BAZARR_TEST_IMPORT_NOISE"):
    print("noise while importing")


class Echo(TranslatorPlugin):
    max_in_flight = 4

    def describe(self):
        return {"backend_types": ["echo"], "pid": os.getpid()}

    def validate(self, config):
        if "bad" in config:
            raise PluginError("invalid_config", "bad setting", field_errors={"bad": "no"})
        return {"normalized": True, **config}

    def migrate(self, from_version, settings, connections):
        return {"settings": {**settings, "v": from_version + 1}, "connections": connections}

    async def options(self, field, config, secrets, ctx=None):
        return [{"value": field, "secret_seen": secrets.get("key")}]

    async def test(self, config, secrets, ctx=None):
        return {"ok": True, "key": secrets.get("key")}

    async def translate(self, request, ctx):
        return {"lines": request.get("lines", []), "secret_seen": request.get("secrets", {}).get("key")}


class Spender(Echo):
    async def translate(self, request, ctx):
        ctx.progress(0, 2, "start")
        hold = await ctx.reserve(0.5, "m1")
        ctx.progress(1, 2, "sent")
        await ctx.usage(10, 0.4, "m1", [hold])
        await ctx.checkpoint({"batch": 1})
        ctx.progress(2, 2, "done")
        return {"hold": hold}


class Hanging(Echo):
    async def translate(self, request, ctx):
        try:
            await ctx.reserve(0.1, "m1")
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # A forced cancel must not keep spending.
            try:
                await ctx.reserve(0.1, "m1")
            except PluginError:
                pass
            raise


class Interruptible(Echo):
    """Takes a second and a half, and stops at once when cancelled."""

    async def translate(self, request, ctx):
        for step in range(30):
            await asyncio.sleep(0.05)
            ctx.progress(step, 30)
        return {"done": True}


class WatchesEvent(Echo):
    async def translate(self, request, ctx):
        ctx.heartbeat()  # tells the test the request is running
        await ctx.cancelled.wait()
        await asyncio.sleep(0)
        return {"unreachable": False}


class Blocking(Echo):
    """A synchronous implementation: runs in a thread and polls the cancel flag."""

    def translate(self, request, ctx):
        for _ in range(int(request.get("steps", 500))):
            if ctx.is_cancelled:
                ctx.raise_if_cancelled()
            ctx.progress(0, 1)
            time.sleep(0.01)
        return {"done": True}


class ThreadHolds(Echo):
    def translate(self, request, ctx):
        hold = ctx.reserve_sync(1.0, "m")
        ctx.usage_sync(1, 1.0, "m", [hold])
        return {"hold": hold, "thread": threading.current_thread().name}


class Failing(Echo):
    async def translate(self, request, ctx):
        kind = request.get("kind")
        if kind == "rate":
            raise PluginError("rate_limited", "slow down", retry_after=12)
        if kind == "declined":
            raise PluginError("declined", "no", reason="language_unsupported")
        raise RuntimeError("boom with a secret value")


class BigResult(Echo):
    async def translate(self, request, ctx):
        return {"lines": ["x" * 1000] * int(request["count"])}


class BigShapes(Echo):
    """Results that are not objects and are too large to travel inline."""

    async def translate(self, request, ctx):
        if request["shape"] == "list":
            return ["line %d" % number * 5 for number in range(60000)]
        if request["shape"] == "text":
            return "x" * 700_000
        return {"value": "y" * 700_000, "other": 1}


class Shapes(Echo):
    """Result objects that look like the SDK's own wrappers, or really use its keys."""

    async def translate(self, request, ctx):
        shape = request["shape"]
        if shape == "value":
            return {"value": 1}
        if shape == "marker":
            return {"json_in_data": True, "x": 1}
        if shape == "both":
            return {"value": 1, "json_in_data": True}
        if shape == "empty":
            return {}
        if shape == "large_value":
            return {"value": "y" * 700_000}
        return {request["key"]: 1}


class LeaksOnCancel(Echo):
    """Leaves a task behind that raises the request's secret when it is cancelled, and logs it too."""

    async def translate(self, request, ctx):
        key = request["secrets"]["key"]
        if request.get("log"):
            logging.getLogger("tests.plugin").warning("using %s", key)

        async def background():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise RuntimeError(f"bad key {key}") from None

        asyncio.get_running_loop().create_task(background())
        await asyncio.sleep(0.05)
        return {"ok": True}


class Surrogate(Echo):
    async def translate(self, request, ctx):
        ctx.progress(1, 2, "half \ud800 way")
        return {"ok": True}


class NotJson(Echo):
    async def translate(self, request, ctx):
        return {"bad": object()}


class Noisy(Echo):
    """Writes to stdout and fd 1 and reads stdin; none of it may touch the frames."""

    async def translate(self, request, ctx):
        print("hello from print")
        sys.stdout.write("raw write\n")
        os.write(1, b"native write to fd 1\n")
        sys.stderr.write("to stderr\n")
        try:
            got = sys.stdin.read()
        except Exception:  # pragma: no cover
            got = "error"
        return {"stdin": got, "ok": True}


class Slow(Echo):
    max_in_flight = 2

    async def translate(self, request, ctx):
        await asyncio.sleep(float(request.get("seconds", 0.3)))
        return {"slept": True}


class Leaky(Echo):
    async def translate(self, request, ctx):
        key = request["secrets"]["key"]
        if request["kind"] == "plugin":
            raise PluginError("auth_required", f"the key {key} was refused")
        raise RuntimeError(f"bad key {key}")


class Stubborn(Echo):
    """Sleeps in a thread and never looks at the cancel flag."""

    def translate(self, request, ctx):
        ctx.heartbeat()  # tells the test the request is running
        time.sleep(120)
        return {}


class Budgeted(Echo):
    async def test(self, config, secrets, ctx=None):
        return {"budget": ctx.budget}


class Broken:
    """Not a TranslatorPlugin."""


class Exploding(Echo):
    def __init__(self):
        raise RuntimeError("constructor failed")


# Whatever the stalling plugins below wait on; a test sets it when it is done.
RELEASE = threading.Event()
_KEPT = []


class Stalls(Echo):
    """A request that stalls in one step of ending or being cancelled, named by ``request["step"]``."""

    async def translate(self, request, ctx):
        step = request["step"]
        ctx.heartbeat()  # tells the test the request is running
        if step == "swallows_cancel":
            while True:
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    pass
        if step == "blocks_the_loop":
            RELEASE.wait(120)
            return {}
        if step == "settle_after_cancel":
            try:
                await asyncio.sleep(120)
            finally:
                await ctx.usage(1, 0.1, "m", [])
        if step == "leftover_task":
            asyncio.get_running_loop().create_task(self._swallow())
        elif step == "open_generator":
            generator = self._generator(blocking=False)
            _KEPT.append(generator)
            await generator.__anext__()
        elif step == "blocking_generator":
            generator = self._generator(blocking=True)
            _KEPT.append(generator)
            await generator.__anext__()
        elif step == "stuck_executor":
            asyncio.get_running_loop().run_in_executor(None, RELEASE.wait, 120)
        elif step == "large_with_thread":
            asyncio.get_running_loop().run_in_executor(None, RELEASE.wait, 120)
            await asyncio.sleep(0.05)
            return {"lines": ["x" * 1000] * 1500}
        elif step == "to_thread":
            asyncio.get_running_loop().create_task(asyncio.to_thread(RELEASE.wait, 120))
        elif step == "all_three":
            asyncio.get_running_loop().create_task(self._swallow())
            generator = self._generator(blocking=False)
            _KEPT.append(generator)
            await generator.__anext__()
            asyncio.get_running_loop().run_in_executor(None, RELEASE.wait, 120)
        await asyncio.sleep(0.05)
        return {"ended": step}

    @staticmethod
    async def _swallow():
        while True:
            try:
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                pass

    @staticmethod
    async def _generator(blocking):
        try:
            yield 1
        finally:
            if blocking:
                RELEASE.wait(120)
            else:
                await asyncio.sleep(120)


class StallsSync(Echo):
    """A plain method that ignores its cancel."""

    started = []

    def translate(self, request, ctx):
        type(self).started.append(1)
        ctx.heartbeat()
        RELEASE.wait(120)
        return {}
