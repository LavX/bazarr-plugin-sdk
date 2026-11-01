"""Tests for the worker runtime, run in a child process through FakeHost."""

from __future__ import annotations

import fcntl
import json
import os
import resource
import select
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest
from sample_plugin import MIB, SamplePlugin

from bazarr_plugin_sdk.runtime import ENV_FD_IN, ENV_FD_OUT
from bazarr_plugin_sdk.testing import CallOutcome, FakeHost
from bazarr_plugin_sdk.testing.fake_host import WORKER_MODULE, plugin_spec
from bazarr_plugin_sdk.wire import (
    DEFAULT_LIMITS,
    PROTOCOL_VERSION,
    FrameReader,
    encode_frame,
)

# Payloads that the JSON decoder refuses although they fit in one control frame.
UNDECODABLE_PAYLOADS = {
    "integer-over-digit-limit": b'{"t":"progress","op":"op-1","done":' + b"1" * 5000 + b"}",
    "nesting-too-deep": b'{"t":"progress","op":"op-1","message":'
    + b"[" * 30_000
    + b"]" * 30_000
    + b"}",
}

# Seconds a test waits for a worker process. A worker that does not exit in time fails the test.
PROCESS_TIMEOUT = 10.0


def framed(payload: bytes) -> bytes:
    return len(payload).to_bytes(4, "big") + payload


def hello_fields(
    root: Path, *, remove: tuple[str, ...] = (), **changes: object
) -> dict[str, object]:
    fields: dict[str, object] = {
        "t": "hello",
        "op": "",
        "protocol": PROTOCOL_VERSION,
        "kind_api": "sample.v1",
        "host_version": "test",
        "limits": DEFAULT_LIMITS.to_dict(),
        "heartbeat_interval": 1.0,
        "max_in_flight": 1,
        "folders": {
            "data_dir": str(root / "data"),
            "temp_dir": str(root / "tmp"),
            "artifacts_dir": str(root / "artifacts"),
        },
    }
    for name in remove:
        del fields[name]
    fields.update(changes)
    return fields


def hello_frame(root: Path, *, remove: tuple[str, ...] = (), **changes: object) -> bytes:
    return encode_frame(hello_fields(root, remove=remove, **changes))


def read_bounded(fd: int, count: int) -> bytes:
    """Read from a pipe, failing rather than waiting past the process timeout."""
    ready, _, _ = select.select([fd], [], [], PROCESS_TIMEOUT)
    if not ready:
        raise TimeoutError("the worker sent nothing within the timeout")
    return os.read(fd, count)


def worker_env(**extra: str) -> dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": os.pathsep.join(entry for entry in sys.path if entry),
        "PYTHONDONTWRITEBYTECODE": "1",
        "LANG": "C.UTF-8",
    }
    env.update(extra)
    return env


def spawn_worker(
    root: Path,
    env: dict[str, str],
    pass_fds: tuple[int, ...],
    stdin: int,
) -> subprocess.Popen[bytes]:
    with (root / "worker-stderr.txt").open("wb") as stderr:
        return subprocess.Popen(
            [sys.executable, "-B", "-m", WORKER_MODULE, "sample_plugin:SamplePlugin"],
            stdin=stdin,
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            pass_fds=pass_fds,
            env=env,
            cwd=str(root),
        )


@contextmanager
def launched(process: subprocess.Popen[bytes]) -> Iterator[subprocess.Popen[bytes]]:
    """Kill the worker at the end of the block if it is still running."""
    try:
        yield process
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()


@dataclass
class RawWorker:
    """A worker process whose frames the test writes by hand."""

    process: subprocess.Popen[bytes]
    frames_in: int
    stdin: int

    def send(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            view = view[os.write(self.frames_in, view) :]

    def close_frames(self) -> None:
        if self.frames_in >= 0:
            os.close(self.frames_in)
            self.frames_in = -1

    def close_stdin(self) -> None:
        if self.stdin >= 0:
            os.close(self.stdin)
            self.stdin = -1

    def wait(self, timeout: float = PROCESS_TIMEOUT) -> int:
        return self.process.wait(timeout=timeout)


@contextmanager
def raw_worker(root: Path) -> Iterator[RawWorker]:
    in_read, in_write = os.pipe()
    out_read, out_write = os.pipe()
    stdin_read, stdin_write = os.pipe()
    env = worker_env(**{ENV_FD_IN: str(in_read), ENV_FD_OUT: str(out_write)})
    process = spawn_worker(root, env, (in_read, out_write), stdin_read)
    os.close(in_read)
    os.close(out_write)
    os.close(stdin_read)
    worker = RawWorker(process, in_write, stdin_write)
    try:
        yield worker
    finally:
        worker.close_stdin()
        worker.close_frames()
        os.close(out_read)
        if process.poll() is None:
            process.kill()
        process.wait()


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


def test_unknown_operation_message_repeats_at_most_64_characters_of_the_name(
    host: FakeHost,
) -> None:
    host.start()
    outcome = host.call("no_such_" + "x" * 5000)
    assert outcome.kind == "error"
    assert outcome.frame is not None
    message = outcome.frame["message"]
    assert "unknown operation" in message
    assert "x" * 65 not in message


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
    assert host.wait_for_exit(timeout=5.0) == 0
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


@pytest.mark.parametrize("payload", UNDECODABLE_PAYLOADS.values(), ids=UNDECODABLE_PAYLOADS)
def test_handshake_frame_the_decoder_refuses_exits_76(tmp_path: Path, payload: bytes) -> None:
    with raw_worker(tmp_path) as worker:
        worker.send(framed(payload))
        assert worker.wait() == 76


@pytest.mark.parametrize("payload", UNDECODABLE_PAYLOADS.values(), ids=UNDECODABLE_PAYLOADS)
def test_steady_state_frame_the_decoder_refuses_exits_76(host: FakeHost, payload: bytes) -> None:
    host.start()
    host.send_raw(framed(payload))
    assert host.close() == 76


def test_end_of_standard_input_before_hello_exits_zero(tmp_path: Path) -> None:
    with raw_worker(tmp_path) as worker:
        worker.close_stdin()
        assert worker.wait() == 0


def test_end_of_the_frame_stream_before_hello_exits_zero(tmp_path: Path) -> None:
    with raw_worker(tmp_path) as worker:
        worker.close_frames()
        assert worker.wait() == 0


def test_a_directory_as_the_frame_descriptor_exits_2(tmp_path: Path) -> None:
    directory = os.open(tmp_path, os.O_RDONLY)
    out_read, out_write = os.pipe()
    try:
        env = worker_env(**{ENV_FD_IN: str(directory), ENV_FD_OUT: str(out_write)})
        process = spawn_worker(tmp_path, env, (directory, out_write), subprocess.DEVNULL)
        with launched(process):
            assert process.wait(timeout=PROCESS_TIMEOUT) == 2
    finally:
        os.close(directory)
        os.close(out_read)
        os.close(out_write)


def test_a_closed_frame_descriptor_exits_2_within_a_bounded_time(tmp_path: Path) -> None:
    closed_read, closed_write = os.pipe()
    os.close(closed_read)
    os.close(closed_write)
    out_read, out_write = os.pipe()
    try:
        env = worker_env(**{ENV_FD_IN: str(closed_read), ENV_FD_OUT: str(out_write)})
        process = spawn_worker(tmp_path, env, (out_write,), subprocess.DEVNULL)
        with launched(process):
            assert process.wait(timeout=PROCESS_TIMEOUT) == 2
    finally:
        os.close(out_read)
        os.close(out_write)


def test_a_frame_descriptor_at_or_above_1024_is_served(tmp_path: Path) -> None:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = 1100
    if hard != resource.RLIM_INFINITY and hard < target:
        pytest.skip(f"the hard descriptor limit {hard} is below {target}")
    if soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    fds: list[int] = []
    try:
        in_read, in_write = os.pipe()
        out_read, out_write = os.pipe()
        stdin_read, stdin_write = os.pipe()
        fds.extend([in_write, out_read, stdin_write])
        high_in = fcntl.fcntl(in_read, fcntl.F_DUPFD, 1024)
        os.close(in_read)
        high_out = fcntl.fcntl(out_write, fcntl.F_DUPFD, 1024)
        os.close(out_write)
        fds.extend([high_in, high_out])
        env = worker_env(**{ENV_FD_IN: str(high_in), ENV_FD_OUT: str(high_out)})
        process = spawn_worker(tmp_path, env, (high_in, high_out), stdin_read)
        os.close(stdin_read)
        worker = RawWorker(process, in_write, stdin_write)
        with launched(process):
            worker.send(hello_frame(tmp_path))
            ack = FrameReader(lambda count: read_bounded(out_read, count)).read_frame()
            assert ack is not None
            assert ack["t"] == "hello_ack"
            worker.send(encode_frame({"t": "shutdown", "op": ""}))
            assert process.wait(timeout=PROCESS_TIMEOUT) == 0
    finally:
        for fd in fds:
            os.close(fd)
        if (soft, hard) != resource.getrlimit(resource.RLIMIT_NOFILE):
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


def test_heartbeat_interval_of_1e400_exits_76(tmp_path: Path) -> None:
    text = json.dumps(hello_fields(tmp_path), separators=(",", ":"))
    payload = text.replace('"heartbeat_interval":1.0', '"heartbeat_interval":1e400')
    assert payload != text
    with raw_worker(tmp_path) as worker:
        worker.send(framed(payload.encode("utf-8")))
        worker.close_stdin()
        assert worker.wait() == 76


def test_heartbeat_interval_with_400_digits_exits_76(tmp_path: Path) -> None:
    with raw_worker(tmp_path) as worker:
        worker.send(hello_frame(tmp_path, heartbeat_interval=int("1" * 400)))
        worker.close_stdin()
        assert worker.wait() == 76


@pytest.mark.parametrize("protocol", [1.0, "1", None])
def test_hello_with_a_non_integer_protocol_exits_76(tmp_path: Path, protocol: object) -> None:
    with raw_worker(tmp_path) as worker:
        worker.send(hello_frame(tmp_path, protocol=protocol))
        worker.close_stdin()
        assert worker.wait() == 76


def test_hello_with_a_boolean_protocol_exits_76(tmp_path: Path) -> None:
    with raw_worker(tmp_path) as worker:
        worker.send(hello_frame(tmp_path, protocol=True))
        worker.close_stdin()
        assert worker.wait() == 76


def test_hello_without_protocol_exits_76(tmp_path: Path) -> None:
    with raw_worker(tmp_path) as worker:
        worker.send(hello_frame(tmp_path, remove=("protocol",)))
        worker.close_stdin()
        assert worker.wait() == 76


def test_hello_without_max_in_flight_exits_76(tmp_path: Path) -> None:
    with raw_worker(tmp_path) as worker:
        worker.send(hello_frame(tmp_path, remove=("max_in_flight",)))
        worker.close_stdin()
        assert worker.wait() == 76


def test_protocol_error_wins_over_end_of_standard_input_five_times(tmp_path: Path) -> None:
    # The host closes standard input as it closes the frame stream. The protocol
    # error must still decide the status on every attempt.
    for attempt in range(5):
        with FakeHost(
            SamplePlugin,
            kind_api="sample.v1",
            root=tmp_path / f"race-{attempt}",
            timeout=10.0,
        ) as fake:
            fake.start()
            fake.send_raw(b"\x00\x00\x00\x00")
            assert fake.close() == 76


def test_plugin_defined_in_a_function_cannot_run_as_a_worker(tmp_path: Path) -> None:
    class Local(SamplePlugin):
        pass

    with pytest.raises(ValueError, match="module level"):
        plugin_spec(Local)
    assert plugin_spec(SamplePlugin) == "sample_plugin:SamplePlugin"
