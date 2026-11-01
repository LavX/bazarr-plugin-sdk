"""A scripted host that drives a plugin worker process.

``FakeHost`` starts the worker in a child process with the same two frame
descriptors a real host passes, performs the handshake, sends calls and answers
the worker's awaited requests. Every frame it receives is kept in ``received``
so tests can inspect the wire.

The host holds the child's standard input open. Closing that pipe is how the
host tells the worker to exit, as a real host does at shutdown.
"""

from __future__ import annotations

import os
import select
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any

from ..plugin import Plugin
from ..runtime import ENV_FD_IN, ENV_FD_OUT
from ..wire import (
    DEFAULT_LIMITS,
    PROTOCOL_VERSION,
    BodyAssembler,
    FrameError,
    FrameReader,
    Limits,
    encode_frame,
    iter_body_frames,
)

__all__ = ["Arrival", "CallOutcome", "FakeHost", "plugin_spec"]

WORKER_MODULE = "bazarr_plugin_sdk.testing.worker_child"


def plugin_spec(plugin_class: type[Plugin]) -> str:
    """Return the ``module:qualname`` spec the worker child uses to import a plugin class.

    The class must be defined at module level so that a child process can import it.
    """
    qualname = plugin_class.__qualname__
    if "<locals>" in qualname:
        raise ValueError("the plugin class must be defined at module level to run in a worker")
    return f"{plugin_class.__module__}:{qualname}"


@dataclass(frozen=True)
class Arrival:
    """A frame from the worker and the monotonic time it was read."""

    frame: dict[str, Any]
    at: float


@dataclass(frozen=True)
class CallOutcome:
    """How one call ended.

    ``kind`` is ``result`` (a result frame, with its body when it has one),
    ``error`` (an error frame), ``breach`` (the worker sent a frame or body that
    broke a limit or the framing), or ``closed`` (the worker's stream ended).
    """

    op: str
    kind: str
    frame: dict[str, Any] | None
    body: bytes | None
    started_at: float
    finished_at: float
    detail: str = ""

    @property
    def code(self) -> str | None:
        if self.kind == "error" and self.frame is not None:
            value = self.frame.get("code")
            return value if isinstance(value, str) else None
        return None


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        view = view[written:]


class FakeHost:
    """A host for one worker process. Use it as a context manager to close it reliably.

    Typical use::

        with FakeHost(MyPlugin, kind_api="sample.v1", root=tmp_path) as host:
            host.start()
            outcome = host.call("echo", {"text": "hi"})
    """

    def __init__(
        self,
        plugin: type[Plugin] | str,
        *,
        kind_api: str,
        root: str | Path,
        heartbeat_interval: float = 10.0,
        limits: Limits = DEFAULT_LIMITS,
        timeout: float = 10.0,
        grant_reservations: bool = True,
        max_in_flight: int = 1,
        host_version: str = "fake-host",
    ) -> None:
        self.spec = plugin if isinstance(plugin, str) else plugin_spec(plugin)
        self.kind_api = kind_api
        self.root = Path(root)
        self.data_dir = self.root / "data"
        self.temp_dir = self.root / "tmp"
        self.artifacts_dir = self.root / "artifacts"
        self.heartbeat_interval = heartbeat_interval
        self.limits = limits
        self.timeout = timeout
        self.grant_reservations = grant_reservations
        self.max_in_flight = max_in_flight
        self.host_version = host_version

        self.hello_ack: dict[str, Any] | None = None
        self.received: list[dict[str, Any]] = []
        self.arrivals: list[Arrival] = []
        self.sent: list[dict[str, Any]] = []
        self.calls: list[dict[str, Any]] = []
        self.reserve_frames: list[dict[str, Any]] = []
        self.usage_frames: list[dict[str, Any]] = []
        self.checkpoints: list[bytes] = []
        self.cancel_times: dict[str, float] = {}
        self.stderr_text = ""
        self.stdout_text = ""
        self.exit_code: int | None = None

        self._process: subprocess.Popen[bytes] | None = None
        self._in_fd: int | None = None
        self._out_fd: int | None = None
        self._stdin_fd: int | None = None
        self._reader: FrameReader | None = None
        self._backlog: deque[dict[str, Any]] = deque()
        self._write_lock = threading.Lock()
        self._drains: list[threading.Thread] = []
        self._stderr_chunks: list[bytes] = []
        self._stdout_chunks: list[bytes] = []
        self._reserve_counter = 0
        self._op_counter = 0
        self._deadline: float | None = None
        self._closed = False

    # ----- lifecycle ------------------------------------------------------

    def __enter__(self) -> FakeHost:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def start(self) -> dict[str, Any] | None:
        """Start the worker and send hello. Returns hello_ack, or None if the worker exits first."""
        for folder in (self.data_dir, self.temp_dir, self.artifacts_dir):
            folder.mkdir(parents=True, exist_ok=True)

        in_read, in_write = os.pipe()
        out_read, out_write = os.pipe()
        stdin_read, stdin_write = os.pipe()
        env = {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": os.pathsep.join(entry for entry in sys.path if entry),
            "PYTHONDONTWRITEBYTECODE": "1",
            "LANG": "C.UTF-8",
            "TMPDIR": str(self.temp_dir),
            "TMP": str(self.temp_dir),
            "TEMP": str(self.temp_dir),
            ENV_FD_IN: str(in_read),
            ENV_FD_OUT: str(out_write),
        }
        self._process = subprocess.Popen(
            [sys.executable, "-B", "-m", WORKER_MODULE, self.spec],
            stdin=stdin_read,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=(in_read, out_write),
            env=env,
            cwd=str(self.root),
        )
        os.close(in_read)
        os.close(out_write)
        os.close(stdin_read)
        self._in_fd = in_write
        self._out_fd = out_read
        self._stdin_fd = stdin_write

        process = self._process
        self._drains = [
            threading.Thread(target=self._drain, args=(process.stdout, self._stdout_chunks)),
            threading.Thread(target=self._drain, args=(process.stderr, self._stderr_chunks)),
        ]
        for thread in self._drains:
            thread.daemon = True
            thread.start()

        self._reader = FrameReader(self._read_worker)  # the handshake uses the default limits
        self._send(
            {
                "t": "hello",
                "op": "",
                "protocol": PROTOCOL_VERSION,
                "kind_api": self.kind_api,
                "host_version": self.host_version,
                "limits": self.limits.to_dict(),
                "heartbeat_interval": self.heartbeat_interval,
                "max_in_flight": self.max_in_flight,
                "folders": {
                    "data_dir": str(self.data_dir),
                    "temp_dir": str(self.temp_dir),
                    "artifacts_dir": str(self.artifacts_dir),
                },
            }
        )
        frame = self._next_frame()
        if frame is not None and frame.get("t") == "hello_ack":
            self.hello_ack = frame
            # The host's limits are the ones the worker accepted in hello.
            self._reader = FrameReader(self._read_worker, limits=self.limits)
            return frame
        return None

    def close(self, timeout: float = 5.0) -> int:
        """Close the worker's input and wait for it to exit. Returns the exit status."""
        if self._closed or self._process is None:
            return self.exit_code if self.exit_code is not None else 0
        self._closed = True
        if self._stdin_fd is not None:
            os.close(self._stdin_fd)
        if self._in_fd is not None:
            os.close(self._in_fd)
        try:
            code = self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._process.kill()
            code = self._process.wait()
        for thread in self._drains:
            thread.join(timeout)
        if self._out_fd is not None:
            os.close(self._out_fd)
        self.stderr_text = b"".join(self._stderr_chunks).decode("utf-8", "replace")
        self.stdout_text = b"".join(self._stdout_chunks).decode("utf-8", "replace")
        self.exit_code = code
        return code

    def wait_for_exit(self, timeout: float = 5.0) -> int:
        """Wait for the worker to exit on its own, leaving its inputs open. Returns the status.

        Raises ``TimeoutError`` when the worker is still running after ``timeout``
        seconds. The worker is then killed, so a test never hangs on it.
        """
        if self._process is None:
            raise RuntimeError("the worker has not been started")
        try:
            return self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            self._process.kill()
            self._process.wait()
            raise TimeoutError(f"the worker did not exit within {timeout} seconds") from exc

    @staticmethod
    def _drain(stream: Any, sink: list[bytes]) -> None:
        if stream is None:
            return
        sink.append(stream.read())

    # ----- operations -----------------------------------------------------

    def new_operation_id(self) -> str:
        self._op_counter += 1
        return f"op-{self._op_counter}"

    def call(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        body: bytes | None = None,
        secrets: Mapping[str, str] | None = None,
        budget: Mapping[str, Any] | None = None,
        op: str | None = None,
        on_frame: Callable[[FakeHost, dict[str, Any]], None] | None = None,
        after_send: Callable[[FakeHost], None] | None = None,
        deadline: float | None = None,
    ) -> CallOutcome:
        """Send one call and answer the worker until it returns a result or an error.

        ``on_frame`` sees every frame from the worker during the call.
        ``after_send`` runs once the call frame and its body are written, which
        is the place to start a timer that cancels the call. ``deadline`` is an
        absolute ``time.monotonic()`` value that bounds every frame read of the
        call together. A read that reaches it raises ``TimeoutError``.
        """
        operation = op or self.new_operation_id()
        frame: dict[str, Any] = {
            "t": "call",
            "op": operation,
            "method": method,
            "params": dict(params or {}),
            "secrets": dict(secrets or {}),
            "budget": dict(budget or {}),
            "body": body is not None,
        }
        started = time.monotonic()
        self.calls.append(frame)
        self._send(frame, body)
        if after_send is not None:
            after_send(self)
        self._deadline = deadline
        try:
            return self._exchange(operation, started, len(body or b""), on_frame)
        finally:
            self._deadline = None

    def _exchange(
        self,
        operation: str,
        started: float,
        request_length: int,
        on_frame: Callable[[FakeHost, dict[str, Any]], None] | None,
    ) -> CallOutcome:
        while True:
            try:
                incoming = self._next_frame()
            except FrameError as exc:
                return self._outcome(operation, "breach", None, None, started, str(exc))
            if incoming is None:
                return self._outcome(operation, "closed", None, None, started, "worker closed")
            if on_frame is not None:
                on_frame(self, incoming)
            kind = incoming.get("t")
            if kind == "reserve":
                self._answer_reserve(incoming)
                continue
            if kind == "usage":
                self.usage_frames.append(incoming)
                self._ack(incoming)
                continue
            if kind == "checkpoint":
                try:
                    data = self._read_body(incoming, self.limits.checkpoint_bytes)
                except FrameError as exc:
                    return self._outcome(operation, "breach", None, None, started, str(exc))
                self.checkpoints.append(data)
                self._ack(incoming)
                continue
            if incoming.get("op") != operation:
                continue
            if kind == "result":
                result_body: bytes | None = None
                if incoming.get("body") is True:
                    limit = self.limits.result_limit(request_length)
                    try:
                        result_body = self._read_body(incoming, limit)
                    except FrameError as exc:
                        return self._outcome(operation, "breach", None, None, started, str(exc))
                return self._outcome(operation, "result", incoming, result_body, started, "")
            if kind == "error":
                return self._outcome(operation, "error", incoming, None, started, "")

    def cancel(self, op: str, *, abandon: bool = False) -> None:
        """Send a cancel for ``op``. Safe to call from another thread.

        The time is recorded once the frame is written, so a cancel that could
        not be written has no time.
        """
        self._send({"t": "cancel", "op": op, "abandon": abandon})
        self.cancel_times[op] = time.monotonic()

    def send_shutdown(self) -> None:
        self._send({"t": "shutdown", "op": ""})

    def send_raw(self, data: bytes) -> None:
        """Write raw bytes to the worker. Use only to test how a worker refuses bad framing."""
        if self._in_fd is None:
            raise RuntimeError("the worker has not been started")
        with self._write_lock:
            _write_all(self._in_fd, data)

    # ----- internals ------------------------------------------------------

    def _outcome(
        self,
        op: str,
        kind: str,
        frame: dict[str, Any] | None,
        body: bytes | None,
        started: float,
        detail: str,
    ) -> CallOutcome:
        return CallOutcome(
            op=op,
            kind=kind,
            frame=frame,
            body=body,
            started_at=started,
            finished_at=time.monotonic(),
            detail=detail,
        )

    def _send(self, frame: dict[str, Any], body: bytes | None = None) -> None:
        if self._in_fd is None:
            raise RuntimeError("the worker has not been started")
        self.sent.append(frame)
        with self._write_lock:
            _write_all(self._in_fd, encode_frame(frame, limits=self.limits))
            if body is not None:
                for chunk in iter_body_frames(
                    of=str(frame["t"]),
                    op=str(frame["op"]),
                    data=body,
                    req=frame.get("req"),
                    limits=self.limits,
                ):
                    _write_all(self._in_fd, encode_frame(chunk, limits=self.limits))

    def _read_worker(self, count: int) -> bytes:
        if self._out_fd is None:
            return b""
        deadline = self._deadline
        wait = self.timeout
        if deadline is not None:
            wait = max(0.0, min(wait, deadline - time.monotonic()))
        ready, _, _ = select.select([self._out_fd], [], [], wait)
        if not ready:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("the call did not end before its deadline")
            raise TimeoutError(f"the worker sent nothing for {self.timeout} seconds")
        return os.read(self._out_fd, count)

    def _receive(self) -> dict[str, Any] | None:
        if self._reader is None:
            raise RuntimeError("the worker has not been started")
        frame = self._reader.read_frame()
        if frame is None:
            return None
        self.received.append(frame)
        self.arrivals.append(Arrival(frame=frame, at=time.monotonic()))
        return frame

    def _next_frame(self) -> dict[str, Any] | None:
        if self._backlog:
            return self._backlog.popleft()
        return self._receive()

    def _read_body(self, header: Mapping[str, Any], limit: int) -> bytes:
        assembler = BodyAssembler(
            of=str(header["t"]),
            op=str(header["op"]),
            req=header.get("req") if isinstance(header.get("req"), str) else None,
            limit=limit,
            limits=self.limits,
        )
        while True:
            frame = self._receive()
            if frame is None:
                raise FrameError("the worker closed inside a body")
            if frame.get("t") == "chunk" and assembler.accepts(frame):
                if assembler.add(frame):
                    return assembler.body()
            else:
                self._backlog.append(frame)

    def _answer_reserve(self, frame: dict[str, Any]) -> None:
        self.reserve_frames.append(frame)
        reservation: str | None = None
        if self.grant_reservations:
            self._reserve_counter += 1
            reservation = f"res-{self._reserve_counter}"
        self._send(
            {
                "t": "ack",
                "op": frame["op"],
                "req": frame["req"],
                "ok": True,
                "reservation": reservation,
            }
        )

    def _ack(self, frame: dict[str, Any]) -> None:
        self._send({"t": "ack", "op": frame["op"], "req": frame["req"], "ok": True})
