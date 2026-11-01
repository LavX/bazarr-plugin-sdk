"""Tests for the plugin context against a fake channel."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from bazarr_plugin_sdk.context import (
    BlockingContext,
    Folders,
    OperationState,
    PluginContext,
    Request,
    Result,
    raise_for_ack,
)
from bazarr_plugin_sdk.errors import (
    CANCELLED,
    INTERNAL,
    OperationAbandoned,
    PluginError,
)
from bazarr_plugin_sdk.wire import Limits


class FakeChannel:
    """Records frames and answers awaited requests with queued acks."""

    def __init__(self, acks: list[Mapping[str, Any]] | None = None) -> None:
        self.sent: list[Mapping[str, Any]] = []
        self.requests: list[tuple[Mapping[str, Any], bytes | None]] = []
        self._acks = list(acks or [])

    def send(self, frame: Mapping[str, Any]) -> None:
        self.sent.append(dict(frame))

    async def request(
        self, frame: Mapping[str, Any], body: bytes | None = None
    ) -> Mapping[str, Any]:
        self.requests.append((dict(frame), body))
        if self._acks:
            return self._acks.pop(0)
        return {"t": "ack", "op": frame["op"], "req": "1", "ok": True}


def make_context(
    channel: FakeChannel,
    *,
    state: OperationState | None = None,
    limits: Limits | None = None,
    budget: Mapping[str, Any] | None = None,
    folders: Folders | None = None,
) -> PluginContext:
    return PluginContext(
        operation_id="op-1",
        channel=channel,
        state=state or OperationState(),
        folders=folders
        or Folders(
            data_dir=Path("/srv/plugin/data"),
            temp_dir=Path("/srv/plugin/tmp"),
            artifacts_dir=Path("/srv/plugin/artifacts"),
        ),
        heartbeat_interval=10.0,
        budget=budget,
        limits=limits or Limits(),
    )


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_progress_returns_at_once_and_sends_one_frame() -> None:
    channel = FakeChannel()
    context = make_context(channel)
    context.progress(3, 10, "working")
    assert channel.sent == [
        {"t": "progress", "op": "op-1", "done": 3, "total": 10, "message": "working"}
    ]
    assert channel.requests == []


def test_progress_total_and_message_are_optional() -> None:
    channel = FakeChannel()
    make_context(channel).progress(1)
    assert channel.sent[0] == {
        "t": "progress",
        "op": "op-1",
        "done": 1,
        "total": None,
        "message": "",
    }


def test_progress_rejects_wrong_types() -> None:
    context = make_context(FakeChannel())
    with pytest.raises(TypeError):
        context.progress(1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        context.progress(-1)


def test_heartbeat_sends_one_frame_and_does_not_wait() -> None:
    channel = FakeChannel()
    make_context(channel).heartbeat()
    assert channel.sent == [{"t": "heartbeat", "op": "op-1"}]


def test_heartbeat_after_abandonment_raises_and_sends_nothing() -> None:
    channel = FakeChannel()
    state = OperationState()
    state.abandoned.set()
    context = make_context(channel, state=state)
    with pytest.raises(OperationAbandoned):
        context.heartbeat()
    with pytest.raises(OperationAbandoned):
        context.progress(1, 2)
    assert channel.sent == []


def test_cancelled_is_the_state_event() -> None:
    state = OperationState()
    context = make_context(FakeChannel(), state=state)
    assert context.cancelled is state.cancelled
    assert not context.cancelled.is_set()
    state.cancelled.set()
    assert context.cancelled.is_set()


def test_folders_and_heartbeat_interval_are_exposed() -> None:
    context = make_context(FakeChannel())
    assert context.data_dir == Path("/srv/plugin/data")
    assert context.temp_dir == Path("/srv/plugin/tmp")
    assert context.artifacts_dir == Path("/srv/plugin/artifacts")
    assert context.heartbeat_interval == 10.0


def test_budget_is_a_copy_of_the_host_object() -> None:
    source = {"max_cost": 0.5}
    context = make_context(FakeChannel(), budget=source)
    source["max_cost"] = 9
    assert context.budget == {"max_cost": 0.5}
    assert make_context(FakeChannel()).budget == {}


def test_reserve_returns_the_reservation_id_from_the_ack() -> None:
    channel = FakeChannel(
        [{"t": "ack", "op": "op-1", "req": "1", "ok": True, "reservation": "res-7"}]
    )
    result = run(make_context(channel).reserve(0.25, "model-a"))
    assert result == "res-7"
    frame, body = channel.requests[0]
    assert frame == {"t": "reserve", "op": "op-1", "amount": 0.25, "model": "model-a"}
    assert body is None


def test_reserve_returns_none_when_no_reservation_is_granted() -> None:
    channel = FakeChannel([{"t": "ack", "op": "op-1", "req": "1", "ok": True, "reservation": None}])
    assert run(make_context(channel).reserve(0.25, "model-a")) is None


def test_reserve_refused_as_cancelled_raises_the_cancelled_code() -> None:
    channel = FakeChannel(
        [
            {
                "t": "ack",
                "op": "op-1",
                "req": "1",
                "ok": False,
                "error": {"code": "cancelled", "message": "cancelled by the user"},
            }
        ]
    )
    with pytest.raises(PluginError) as info:
        run(make_context(channel).reserve(0.25, "model-a"))
    assert info.value.code == CANCELLED
    assert info.value.message == "cancelled by the user"


def test_reserve_rejects_negative_and_non_finite_amounts() -> None:
    context = make_context(FakeChannel())
    with pytest.raises(ValueError):
        run(context.reserve(-1, "model-a"))
    with pytest.raises(ValueError):
        run(context.reserve(float("nan"), "model-a"))
    with pytest.raises(TypeError):
        run(context.reserve("1", "model-a"))  # type: ignore[arg-type]


def test_reserve_with_a_malformed_reservation_is_internal() -> None:
    channel = FakeChannel([{"t": "ack", "op": "op-1", "req": "1", "ok": True, "reservation": 5}])
    with pytest.raises(PluginError) as info:
        run(make_context(channel).reserve(0.1, "model-a"))
    assert info.value.code == INTERNAL


def test_usage_sends_tokens_cost_model_and_reservations() -> None:
    channel = FakeChannel()
    run(make_context(channel).usage(120, 0.002, "model-a", ["res-7"]))
    frame, body = channel.requests[0]
    assert frame == {
        "t": "usage",
        "op": "op-1",
        "tokens": 120,
        "cost": 0.002,
        "model": "model-a",
        "reservations": ["res-7"],
    }
    assert body is None


def test_usage_refused_by_the_host_raises() -> None:
    channel = FakeChannel([{"t": "ack", "op": "op-1", "req": "1", "ok": False}])
    with pytest.raises(PluginError) as info:
        run(make_context(channel).usage(1, 0.0, "model-a"))
    assert info.value.code == INTERNAL


def test_checkpoint_sends_the_bytes_as_a_body() -> None:
    channel = FakeChannel()
    run(make_context(channel).checkpoint(bytearray(b"state")))
    frame, body = channel.requests[0]
    assert frame == {"t": "checkpoint", "op": "op-1"}
    assert body == b"state"


def test_checkpoint_over_the_limit_is_refused_before_sending() -> None:
    channel = FakeChannel()
    context = make_context(channel, limits=Limits(checkpoint_bytes=4))
    with pytest.raises(ValueError, match="exceeds"):
        run(context.checkpoint(b"12345"))
    assert channel.requests == []


def test_checkpoint_requires_bytes() -> None:
    with pytest.raises(TypeError):
        run(make_context(FakeChannel()).checkpoint("text"))  # type: ignore[arg-type]


def test_raise_for_ack_passes_ok_acks_through() -> None:
    raise_for_ack({"t": "ack", "op": "op-1", "req": "1", "ok": True})


def test_raise_for_ack_maps_unknown_codes_to_internal() -> None:
    with pytest.raises(PluginError) as info:
        raise_for_ack({"ok": False, "error": {"code": "mystery", "message": "x"}})
    assert info.value.code == INTERNAL


def test_raise_for_ack_without_detail_is_internal() -> None:
    with pytest.raises(PluginError) as info:
        raise_for_ack({"ok": False})
    assert info.value.code == INTERNAL


def test_request_and_result_have_sensible_defaults() -> None:
    request = Request(method="echo", params={}, secrets={})
    assert request.body is None
    result = Result()
    assert result.value is None
    assert result.body is None


def test_blocking_context_runs_awaited_calls_on_the_loop_thread() -> None:
    channel = FakeChannel(
        [
            {"t": "ack", "op": "op-1", "req": "1", "ok": True, "reservation": "res-1"},
            {"t": "ack", "op": "op-1", "req": "2", "ok": True},
            {"t": "ack", "op": "op-1", "req": "3", "ok": True},
        ]
    )
    context = make_context(channel)
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        blocking = BlockingContext(context, loop)
        outcome: dict[str, Any] = {}

        def worker() -> None:
            outcome["reservation"] = blocking.reserve(0.1, "model-a")
            blocking.usage(5, 0.0, "model-a", ["res-1"])
            blocking.checkpoint(b"data")
            blocking.progress(1, 2, "half")
            blocking.heartbeat()

        runner = threading.Thread(target=worker)
        runner.start()
        runner.join(timeout=5)
        assert not runner.is_alive()
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()

    assert outcome["reservation"] == "res-1"
    assert [frame["t"] for frame, _ in channel.requests] == ["reserve", "usage", "checkpoint"]
    assert channel.requests[2][1] == b"data"
    assert [frame["t"] for frame in channel.sent] == ["progress", "heartbeat"]


def test_blocking_context_shares_folders_and_cancel_event() -> None:
    state = OperationState()
    context = make_context(FakeChannel(), state=state)
    loop = asyncio.new_event_loop()
    blocking = BlockingContext(context, loop)
    loop.close()
    assert blocking.data_dir == context.data_dir
    assert blocking.temp_dir == context.temp_dir
    assert blocking.artifacts_dir == context.artifacts_dir
    assert blocking.cancelled is state.cancelled
