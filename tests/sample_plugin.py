"""A small plugin used by the SDK's own tests.

Each operation exercises one part of the runtime. The module is imported by
name in the worker child process, so it must stay importable from tests/.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from bazarr_plugin_sdk import Plugin, PluginContext, PluginError, Request, Result
from bazarr_plugin_sdk.errors import DECLINED, INVALID_CONFIG

MIB = 1024 * 1024


class SamplePlugin(Plugin):
    kind_api = "sample.v1"
    operations = (
        "echo",
        "slow",
        "blocking_heartbeat",
        "boom",
        "blocking_boom",
        "oversize",
        "stray_print",
        "reserve_usage",
        "declined",
        "invalid_config",
        "folders",
    )

    async def echo(self, ctx: PluginContext, request: Request) -> Result:
        value = {
            "text": request.params.get("text"),
            "body_length": len(request.body or b""),
        }
        return Result(value=value, body=request.body)

    async def slow(self, ctx: PluginContext, request: Request) -> None:
        ctx.progress(0, 10, "starting")
        while True:
            await asyncio.sleep(0.05)
            ctx.heartbeat()

    def blocking_heartbeat(self, ctx: Any, request: Request) -> dict[str, Any]:
        for _ in range(6):
            time.sleep(0.25)
            ctx.heartbeat()
        reservation = ctx.reserve(0.01, "model-a")
        ctx.usage(10, 0.001, "model-a", [reservation] if reservation else [])
        ctx.checkpoint(b"hello")
        return {"reservation": reservation}

    async def boom(self, ctx: PluginContext, request: Request) -> None:
        raise ValueError("boom " + "x" * 600)

    def blocking_boom(self, ctx: Any, request: Request) -> None:
        raise KeyError("missing")

    async def oversize(self, ctx: PluginContext, request: Request) -> Result:
        return Result(body=b"x" * (2 * MIB))

    async def stray_print(self, ctx: PluginContext, request: Request) -> dict[str, bool]:
        print("this line goes to stderr")
        return {"printed": True}

    async def reserve_usage(self, ctx: PluginContext, request: Request) -> dict[str, Any]:
        reservation = await ctx.reserve(0.01, "model-a")
        await ctx.usage(10, 0.001, "model-a", [reservation] if reservation else [])
        await ctx.checkpoint(b"hello")
        return {"reservation": reservation}

    async def declined(self, ctx: PluginContext, request: Request) -> None:
        raise PluginError(DECLINED, "language not supported", reason="unsupported_language")

    async def invalid_config(self, ctx: PluginContext, request: Request) -> None:
        raise PluginError(
            INVALID_CONFIG, "settings incomplete", field_errors={"api_key": "required"}
        )

    async def folders(self, ctx: PluginContext, request: Request) -> dict[str, str]:
        return {
            "data_dir": str(ctx.data_dir),
            "temp_dir": str(ctx.temp_dir),
            "artifacts_dir": str(ctx.artifacts_dir),
        }
