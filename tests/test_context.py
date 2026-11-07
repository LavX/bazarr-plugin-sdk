import threading

import pytest

from bazarr_plugin_sdk import PluginContext
from bazarr_plugin_sdk import context as context_module
from bazarr_plugin_sdk._protocol.worker_channel import HostCallFailed
from bazarr_plugin_sdk.errors import PluginError


class FakeOperation:
    op = "translate"

    def __init__(self):
        self.cancelled = threading.Event()
        self.progress_calls = []
        self.calls = []
        self.answer = {"hold": "hold-1"}
        self.cancellable = []

    def progress(self, fraction=None, message=None, data=None):
        self.progress_calls.append({"fraction": fraction, "message": message, "data": data})

    def call(self, name, params=None, timeout=60.0, cancellable=True):
        self.calls.append((name, params))
        self.cancellable.append(cancellable)
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


def make():
    operation = FakeOperation()
    return PluginContext(operation, kind_api="bazarr.translator.v1", heartbeat_interval=15), operation


def test_progress_is_throttled_but_the_last_report_is_kept(monkeypatch):
    ctx, operation = make()
    clock = iter([100.0, 100.01, 100.02, 100.03, 100.5])
    monkeypatch.setattr(context_module.time, "monotonic", lambda: next(clock))
    ctx.progress(0, 3)
    ctx.progress(1, 3)
    ctx.progress(2, 3)
    ctx.progress(3, 3)  # the final report always goes
    ctx.progress(0, 3)
    assert [p["data"]["done"] for p in operation.progress_calls] == [0, 3, 0]
    assert operation.progress_calls[1] == {"fraction": 1.0, "message": None, "data": {"done": 3, "total": 3}}


def test_progress_with_no_total_has_no_fraction():
    ctx, operation = make()
    ctx.progress(4, 0, "working")
    assert operation.progress_calls[0]["fraction"] is None and operation.progress_calls[0]["message"] == "working"


def test_heartbeat_is_a_bare_progress_frame():
    ctx, operation = make()
    ctx.heartbeat()
    assert operation.progress_calls == [{"fraction": None, "message": None, "data": None}]


def test_a_cancelled_context_stops_reporting_and_new_spending():
    ctx, operation = make()
    operation.cancelled.set()
    assert ctx.is_cancelled
    ctx.progress(1, 2)
    ctx.heartbeat()
    assert operation.progress_calls == []
    with pytest.raises(PluginError) as caught:
        ctx.raise_if_cancelled()
    assert caught.value.code == "cancelled"
    with pytest.raises(PluginError):
        ctx.reserve_sync(1, "m")
    with pytest.raises(PluginError):
        ctx.checkpoint_sync({})
    # A call already sent has spent its money: usage still goes through.
    ctx.usage_sync(1, 0.1, "m", ["h"])
    assert [name for name, _ in operation.calls] == ["usage"]


def test_reserve_returns_the_hold_and_validates_the_amount():
    ctx, operation = make()
    assert ctx.reserve_sync(0.25, "m") == "hold-1"
    for bad in (float("nan"), float("inf"), -1):
        with pytest.raises(ValueError):
            ctx.reserve_sync(bad, "m")
    assert operation.calls == [("reserve", {"amount": 0.25, "model": "m"})]


def test_the_awaitable_forms_do_the_same_work():
    import asyncio

    ctx, operation = make()

    async def run():
        hold = await ctx.reserve(1, "m")
        await ctx.usage(5, 0.5, "m", [hold])
        await ctx.checkpoint({"batch": 1})
        return hold

    assert asyncio.run(run()) == "hold-1"
    assert [name for name, _ in operation.calls] == ["reserve", "usage", "checkpoint"]
    assert operation.calls[1][1] == {"tokens": 5, "cost": 0.5, "model": "m", "reservations": ["hold-1"]}


def test_a_refused_host_call_becomes_a_plugin_error():
    ctx, operation = make()
    operation.answer = HostCallFailed("quota_exhausted", "month spent")
    with pytest.raises(PluginError) as caught:
        ctx.reserve_sync(1, "m")
    assert (caught.value.code, caught.value.message) == ("quota_exhausted", "month spent")
    operation.answer = HostCallFailed("budget_exhausted", "no")
    with pytest.raises(PluginError) as caught:
        ctx.reserve_sync(1, "m")
    assert (caught.value.code, caught.value.reason) == ("internal", "budget_exhausted")


def test_the_folders_come_from_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("BAZARR_PLUGIN_DATA_DIR", str(tmp_path / "d"))
    monkeypatch.delenv("BAZARR_PLUGIN_TEMP_DIR", raising=False)
    ctx, _ = make()
    assert ctx.data_dir == tmp_path / "d" and ctx.temp_dir is None


def test_only_a_reserve_gives_up_when_the_host_cancels():
    ctx, operation = make()
    ctx.reserve_sync(1, "m")
    ctx.usage_sync(1, 1.0, "m", ["h"])
    ctx.checkpoint_sync({})
    assert operation.cancellable == [True, False, False]


def test_a_host_call_cut_short_by_a_cancel_is_a_cancelled_plugin_error():
    from bazarr_plugin_sdk._protocol.worker_channel import OperationCancelled

    ctx, operation = make()
    operation.answer = OperationCancelled("cancelled")
    with pytest.raises(PluginError) as caught:
        ctx.reserve_sync(1, "m")
    assert caught.value.code == "cancelled"
