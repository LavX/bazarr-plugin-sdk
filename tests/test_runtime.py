"""Tests for the worker runtime, run in a child process through FakeHost."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from sample_plugin import MIB, SamplePlugin

from bazarr_plugin_sdk.testing import CallOutcome, FakeHost
from bazarr_plugin_sdk.testing.fake_host import plugin_spec


@pytest.fixture
def host(tmp_path: Path) -> Iterator[FakeHost]:
    with FakeHost(
        SamplePlugin,
        kind_api="sample.v1",
        root=tmp_path,
        heartbeat_interval=1.0,
        timeout=10.0,
    ) as fake:
        yield fake


def test_handshake_reports_kind_operations_folders_and_temp_environment(host: FakeHost) -> None:
    ack = host.start()
    assert ack is not None
    assert ack["protocol"] == 1
    assert ack["kind_apis"] == ["sample.v1"]
    assert "echo" in ack["operations"]
    assert ack["folders"] == {
        "data_dir": str(host.data_dir),
        "temp_dir": str(host.temp_dir),
        "artifacts_dir": str(host.artifacts_dir),
    }
    assert ack["environment"]["TMPDIR"] == str(host.temp_dir)


def test_echo_returns_params_and_body(host: FakeHost) -> None:
    host.start()
    outcome = host.call("echo", {"text": "hi"}, body=b"abc")
    assert outcome.kind == "result"
    assert outcome.frame is not None
    assert outcome.frame["value"] == {"text": "hi", "body_length": 3}
    assert outcome.body == b"abc"


def test_large_bodies_travel_as_chunks_in_both_directions(host: FakeHost) -> None:
    host.start()
    payload = bytes(range(256)) * (MIB // 256) * 2  # 2 MiB of varied bytes
    outcome = host.call("echo", {"text": "big"}, body=payload)
    assert outcome.kind == "result"
    assert outcome.body == payload


def test_stray_print_goes_to_stderr_and_does_not_corrupt_frames(host: FakeHost) -> None:
    host.start()
    outcome = host.call("stray_print")
    assert outcome.kind == "result"
    assert outcome.frame is not None
    assert outcome.frame["value"] == {"printed": True}
    assert host.close() == 0
    assert "this line goes to stderr" in host.stderr_text
    assert host.stdout_text == ""


def test_cancel_mid_call_ends_with_cancelled_and_the_worker_keeps_serving(host: FakeHost) -> None:
    host.start()
    op = "cancel-me"
    timers: list[threading.Timer] = []

    def arm(fake: FakeHost) -> None:
        timer = threading.Timer(0.3, fake.cancel, args=(op,))
        timers.append(timer)
        timer.start()

    outcome = host.call("slow", op=op, after_send=arm)
    for timer in timers:
        timer.cancel()
    assert outcome.kind == "error"
    assert outcome.code == "cancelled"
    follow_up = host.call("echo", {"text": "after"})
    assert follow_up.kind == "result"
    assert follow_up.frame is not None
    assert follow_up.frame["value"]["text"] == "after"


def test_blocking_operation_keeps_heartbeats_flowing(host: FakeHost) -> None:
    host.start()
    outcome = host.call("blocking_heartbeat", op="blocking-1")
    assert outcome.kind == "result"
    beats = [
        arrival
        for arrival in host.arrivals
        if arrival.frame.get("t") == "heartbeat" and arrival.frame.get("op") == "blocking-1"
    ]
    assert len(beats) >= 2


def test_blocking_reserve_usage_and_checkpoint_are_acknowledged(host: FakeHost) -> None:
    host.start()
    outcome = host.call("blocking_heartbeat")
    assert outcome.kind == "result"
    assert outcome.frame is not None
    assert outcome.frame["value"] == {"reservation": "res-1"}
    assert len(host.reserve_frames) == 1
    assert len(host.usage_frames) == 1
    assert host.checkpoints == [b"hello"]


def test_async_reserve_usage_and_checkpoint_are_acknowledged(host: FakeHost) -> None:
    host.start()
    outcome = host.call("reserve_usage")
    assert outcome.kind == "result"
    assert outcome.frame is not None
    assert outcome.frame["value"] == {"reservation": "res-1"}
    assert host.usage_frames[0]["reservations"] == ["res-1"]
    assert host.checkpoints == [b"hello"]


def test_reservation_refused_when_the_host_grants_none(tmp_path: Path) -> None:
    with FakeHost(
        SamplePlugin,
        kind_api="sample.v1",
        root=tmp_path,
        grant_reservations=False,
        timeout=10.0,
    ) as fake:
        fake.start()
        outcome = fake.call("reserve_usage")
    assert outcome.kind == "result"
    assert outcome.frame is not None
    assert outcome.frame["value"] == {"reservation": None}


def test_unexpected_exception_maps_to_internal_and_traceback_goes_to_stderr(
    host: FakeHost,
) -> None:
    host.start()
    outcome = host.call("boom")
    assert outcome.kind == "error"
    assert outcome.code == "internal"
    assert outcome.frame is not None
    message = outcome.frame["message"]
    assert message.startswith("ValueError: boom")
    assert len(message) <= 500
    assert "Traceback" not in message
    host.close()
    assert "Traceback" in host.stderr_text


def test_exception_in_a_blocking_operation_maps_to_internal(host: FakeHost) -> None:
    host.start()
    outcome = host.call("blocking_boom")
    assert outcome.kind == "error"
    assert outcome.code == "internal"
    assert outcome.frame is not None
    assert outcome.frame["message"] == "KeyError: 'missing'"


def test_declined_and_invalid_config_carry_their_optional_fields(host: FakeHost) -> None:
    host.start()
    declined = host.call("declined")
    assert declined.code == "declined"
    assert declined.frame is not None
    assert declined.frame["reason"] == "unsupported_language"

    invalid = host.call("invalid_config")
    assert invalid.code == "invalid_config"
    assert invalid.frame is not None
    assert invalid.frame["field_errors"] == {"api_key": "required"}


def test_oversize_result_is_refused_with_internal(host: FakeHost) -> None:
    host.start()
    outcome: CallOutcome = host.call("oversize")
    assert outcome.kind == "error"
    assert outcome.code == "internal"
    assert outcome.body is None


def test_unknown_operation_is_internal(host: FakeHost) -> None:
    host.start()
    outcome = host.call("no_such_operation")
    assert outcome.kind == "error"
    assert outcome.code == "internal"
    assert outcome.frame is not None
    assert "unknown operation" in outcome.frame["message"]


def test_every_operation_sees_the_folders_from_hello(host: FakeHost) -> None:
    ack = host.start()
    assert ack is not None
    expected = ack["folders"]
    for _ in range(2):
        outcome = host.call("folders")
        assert outcome.kind == "result"
        assert outcome.frame is not None
        assert outcome.frame["value"] == expected


def test_end_of_input_makes_the_worker_exit_zero(host: FakeHost) -> None:
    host.start()
    assert host.close() == 0


def test_shutdown_frame_makes_the_worker_exit_zero(host: FakeHost) -> None:
    host.start()
    host.send_shutdown()
    assert host.close() == 0


def test_handshake_for_a_kind_the_worker_does_not_serve_exits_78(tmp_path: Path) -> None:
    fake = FakeHost(SamplePlugin, kind_api="other.v9", root=tmp_path, timeout=10.0)
    try:
        assert fake.start() is None
    finally:
        assert fake.close() == 78


def test_oversize_frame_from_the_host_exits_76(host: FakeHost) -> None:
    host.start()
    host.send_raw((2_000_000).to_bytes(4, "big"))
    assert host.close() == 76


def test_plugin_defined_in_a_function_cannot_run_as_a_worker(tmp_path: Path) -> None:
    class Local(SamplePlugin):
        pass

    with pytest.raises(ValueError, match="module level"):
        plugin_spec(Local)
    assert plugin_spec(SamplePlugin) == "sample_plugin:SamplePlugin"
