"""Worker side of the protocol: the handshake, the frame loop and operation dispatch.

A plugin's entry point calls ``serve(MyPlugin)`` and exits with the status it
returns. The host passes the two frame descriptors in ``BAZARR_PLUGIN_FD_IN``
(frames from the host) and ``BAZARR_PLUGIN_FD_OUT`` (frames to the host).
Standard output is redirected to standard error from the moment ``serve()``
starts, so a stray ``print`` made by plugin code cannot corrupt a frame.

The worker exits when the host closes its frame stream or the standard input
pipe, which the host keeps open for the life of the worker.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import select
import stat
import sys
import threading
import traceback
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._version import __version__
from .context import (
    LOGGER_NAME,
    BlockingContext,
    Folders,
    OperationState,
    PluginContext,
    Request,
    Result,
)
from .errors import (
    CANCELLED,
    INTERNAL,
    HostGone,
    OperationAbandoned,
    PluginError,
    internal_error,
    truncate_message,
)
from .plugin import Plugin
from .wire import (
    DEFAULT_LIMITS,
    PROTOCOL_VERSION,
    BodyAssembler,
    FrameError,
    FrameReader,
    FrameTooLarge,
    Limits,
    encode_frame,
    iter_body_frames,
)

__all__ = [
    "ENV_FD_IN",
    "ENV_FD_OUT",
    "EXIT_CONTRACT_NOT_SERVED",
    "EXIT_ENVIRONMENT",
    "EXIT_OK",
    "EXIT_PROTOCOL",
    "serve",
]

ENV_FD_IN = "BAZARR_PLUGIN_FD_IN"
ENV_FD_OUT = "BAZARR_PLUGIN_FD_OUT"
_STDIN_FD = 0
_MAX_DESCRIPTOR = 2**31 - 1

EXIT_OK = 0
EXIT_ENVIRONMENT = 2
EXIT_PROTOCOL = 76
EXIT_CONTRACT_NOT_SERVED = 78

RESULT_LIMIT_MESSAGE = "the result body exceeds the result limit"

# Seconds the worker gives its operations to end once shutdown begins. The
# watchdog ends the process at this bound, whatever is still running.
_TEARDOWN_SECONDS = 2.0
# Seconds the stdin watcher waits for the reader thread before it reports end
# of input, so that a protocol error the reader holds is not overtaken.
_READER_JOIN_SECONDS = 2.0
# Characters of host text that an error message may repeat.
_ECHO_LIMIT = 64

_READABLE = select.POLLIN | select.POLLHUP
_BROKEN = select.POLLNVAL | select.POLLERR

_TEMP_VARIABLES = ("TMPDIR", "TMP", "TEMP")


def _report(text: str) -> None:
    try:
        sys.stderr.write(text if text.endswith("\n") else text + "\n")
        sys.stderr.flush()
    except (OSError, ValueError):
        pass


class _DescriptorError(Exception):
    """A read failed on the frame or standard input descriptor. The worker exits with status 2."""


def _descriptor(name: str) -> int:
    raw = os.environ.get(name)
    if raw is None:
        raise ValueError(f"{name} is not set")
    try:
        fd = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} is not a descriptor number") from exc
    if fd < 3:
        raise ValueError(f"{name} must be a descriptor above 2")
    if fd > _MAX_DESCRIPTOR:
        raise ValueError(f"{name} must be at most {_MAX_DESCRIPTOR}")
    try:
        status = os.fstat(fd)
    except OSError as exc:
        raise ValueError(f"{name} is not an open descriptor: {exc}") from exc
    if stat.S_ISDIR(status.st_mode):
        raise ValueError(f"{name} is a directory, not a pipe")
    return fd


def _folders_from(raw: object) -> Folders:
    if not isinstance(raw, Mapping):
        raise ValueError("folders must be a JSON object")
    paths: dict[str, Path] = {}
    for key in ("data_dir", "temp_dir", "artifacts_dir"):
        value = raw.get(key)
        if not isinstance(value, str) or not os.path.isabs(value):
            raise ValueError(f"folder {key} must be an absolute path")
        paths[key] = Path(value)
    return Folders(**paths)


def _folders_json(folders: Folders) -> dict[str, str]:
    return {
        "data_dir": str(folders.data_dir),
        "temp_dir": str(folders.temp_dir),
        "artifacts_dir": str(folders.artifacts_dir),
    }


def _environment_report() -> dict[str, str]:
    return {name: os.environ[name] for name in _TEMP_VARIABLES if name in os.environ}


def _heartbeat_seconds(raw: object) -> float:
    """Return the heartbeat interval as seconds: a finite number greater than zero."""
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError("heartbeat_interval must be a positive number")
    try:
        seconds = float(raw)
    except OverflowError as exc:
        raise ValueError("heartbeat_interval must be a finite number of seconds") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("heartbeat_interval must be a finite number of seconds above zero")
    return seconds


def _unknown_operation(method: str) -> PluginError:
    # The host's text is cut before it is repeated in an error frame.
    return PluginError(INTERNAL, f"unknown operation {method[:_ECHO_LIMIT]!r}")


def _post(loop: asyncio.AbstractEventLoop, function: Callable[..., Any], *args: Any) -> None:
    try:
        loop.call_soon_threadsafe(function, *args)
    except RuntimeError:
        pass  # the loop has already stopped; the worker is exiting


def _coerce_result(value: object) -> Result:
    if value is None:
        return Result()
    if isinstance(value, Result):
        result = value
    elif isinstance(value, Mapping):
        result = Result(value=value)
    else:
        raise PluginError(INTERNAL, "the operation returned a value the protocol cannot carry")
    body = result.body
    if body is not None:
        if not isinstance(body, (bytes, bytearray, memoryview)):
            raise PluginError(INTERNAL, "the result body must be bytes")
        body = bytes(body)
    return Result(value=result.value, body=body)


class _Writer:
    """Writes whole frames to the host descriptor. One lock keeps frames from interleaving.

    The lock is re-entrant so that a frame and the chunks of its body can be
    written as one unit with ``batch``.
    """

    def __init__(self, fd: int) -> None:
        self._fd = fd
        self._lock = threading.RLock()

    @contextmanager
    def batch(self) -> Iterator[None]:
        """Hold the lock while the caller writes several frames as one unit."""
        with self._lock:
            yield

    def write(self, data: bytes) -> None:
        view = memoryview(data)
        with self._lock:
            try:
                while view:
                    written = os.write(self._fd, view)
                    view = view[written:]
            except (OSError, ValueError) as exc:
                raise HostGone(f"the host pipe is closed: {exc}") from exc


@dataclass
class _Active:
    state: OperationState
    task: asyncio.Task[None] | None = None
    started: bool = False


class _Worker:
    """One worker process: the handshake, the reader thread and the operation tasks.

    The worker satisfies the ``Channel`` protocol of the context module. Its
    ``send`` and ``request`` methods may be called from the event loop thread
    or from an operation's worker thread.
    """

    def __init__(self, plugin_class: type[Plugin], fd_in: int, fd_out: int) -> None:
        self._plugin_class = plugin_class
        self._fd_in = fd_in
        self._writer = _Writer(fd_out)
        self._limits: Limits = DEFAULT_LIMITS
        self._folders: Folders | None = None
        self._heartbeat_interval = 0.0
        self._max_in_flight: int  # set by the handshake, which requires the hello key
        self._plugin: Plugin | None = None
        self._active: dict[str, _Active] = {}
        self._pending: dict[str, asyncio.Future[Mapping[str, Any]]] = {}
        self._request_counter = 0
        self._exit_code = EXIT_OK
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop: asyncio.Event | None = None
        self._reader: FrameReader | None = None
        self._reader_thread: threading.Thread | None = None
        self._teardown_done = threading.Event()

    # ----- start-up -------------------------------------------------------

    def run(self) -> int:
        handshake_reader = FrameReader(self._read_before_hello)
        status = self._handshake(handshake_reader)
        if status is not None:
            return status
        self._reader = FrameReader(self._read_input, limits=self._limits)
        try:
            return asyncio.run(self._serve())
        finally:
            self._teardown_done.set()

    def _read_input(self, count: int) -> bytes:
        return os.read(self._fd_in, count)

    def _read_before_hello(self, count: int) -> bytes:
        """Read the handshake from the frame descriptor while watching standard input.

        End of standard input before hello ends the worker, as it does after
        hello. Bytes that arrive on standard input are discarded. A failed read
        on either descriptor raises ``_DescriptorError``.
        """
        poller = select.poll()
        poller.register(self._fd_in, _READABLE)
        poller.register(_STDIN_FD, _READABLE)
        while True:
            try:
                ready = poller.poll()
            except OSError as exc:
                raise _DescriptorError(f"waiting for the descriptors failed: {exc}") from exc
            events = dict(ready)
            frame_events = events.get(self._fd_in, 0)
            if frame_events & _BROKEN:
                raise _DescriptorError("the frame descriptor cannot be read")
            if frame_events & _READABLE:
                try:
                    return os.read(self._fd_in, count)
                except OSError as exc:
                    raise _DescriptorError(f"reading the frame descriptor failed: {exc}") from exc
            stdin_events = events.get(_STDIN_FD, 0)
            if stdin_events & _BROKEN:
                raise _DescriptorError("standard input cannot be read")
            if stdin_events & _READABLE:
                try:
                    discarded = os.read(_STDIN_FD, 1)
                except OSError as exc:
                    raise _DescriptorError(f"reading standard input failed: {exc}") from exc
                if not discarded:
                    raise HostGone("standard input closed before hello")

    def _handshake(self, reader: FrameReader) -> int | None:
        try:
            hello = reader.read_frame()
        except FrameError as exc:
            _report(f"plugin worker: protocol error before hello: {exc}")
            return EXIT_PROTOCOL
        except HostGone as exc:
            _report(f"plugin worker: {exc}")
            return EXIT_OK
        except _DescriptorError as exc:
            _report(f"plugin worker: {exc}")
            return EXIT_ENVIRONMENT
        if hello is None:
            return EXIT_OK  # the frame stream ended before hello
        if hello.get("t") != "hello":
            _report("plugin worker: the first frame must be hello")
            return EXIT_PROTOCOL
        protocol = hello.get("protocol")
        if isinstance(protocol, bool) or not isinstance(protocol, int):
            _report("plugin worker: malformed hello: protocol must be an integer")
            return EXIT_PROTOCOL
        if protocol != PROTOCOL_VERSION:
            _report(f"plugin worker: host protocol {protocol} is not served")
            return EXIT_CONTRACT_NOT_SERVED
        kind_api = self._plugin_class.kind_api
        if hello.get("kind_api") != kind_api:
            _report(
                f"plugin worker: host asked for {hello.get('kind_api')!r}, "
                f"worker serves {kind_api!r}"
            )
            return EXIT_CONTRACT_NOT_SERVED
        try:
            limits = Limits.from_dict(hello.get("limits"))
            if not limits.within(DEFAULT_LIMITS):
                raise ValueError("host limits exceed the limits this SDK supports")
            interval = _heartbeat_seconds(hello.get("heartbeat_interval"))
            max_in_flight = hello.get("max_in_flight")
            if isinstance(max_in_flight, bool) or not isinstance(max_in_flight, int):
                raise ValueError("max_in_flight must be a positive integer")
            if max_in_flight < 1:
                raise ValueError("max_in_flight must be a positive integer")
            folders = _folders_from(hello.get("folders"))
        except ValueError as exc:
            _report(f"plugin worker: malformed hello: {exc}")
            return EXIT_PROTOCOL

        self._limits = limits
        self._folders = folders
        self._heartbeat_interval = interval
        self._max_in_flight = max_in_flight
        try:
            self._plugin = self._plugin_class()
        except Exception:
            traceback.print_exc(file=sys.stderr)
            return EXIT_ENVIRONMENT

        ack = {
            "t": "hello_ack",
            "op": "",
            "protocol": PROTOCOL_VERSION,
            "sdk_version": __version__,
            "kind_apis": [kind_api],
            "operations": list(self._plugin_class.operations),
            "folders": _folders_json(folders),
            "environment": _environment_report(),
        }
        try:
            self._writer.write(encode_frame(ack, limits=limits))
        except HostGone as exc:
            _report(f"plugin worker: could not send hello_ack: {exc}")
            return EXIT_OK
        except FrameError as exc:
            _report(f"plugin worker: could not send hello_ack: {exc}")
            return EXIT_PROTOCOL
        return None

    async def _serve(self) -> int:
        self._loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        reader = threading.Thread(
            target=self._read_frames, name="bazarr-plugin-reader", daemon=True
        )
        self._reader_thread = reader
        reader.start()
        threading.Thread(target=self._watch_input, name="bazarr-plugin-stdin", daemon=True).start()
        await self._stop.wait()
        threading.Thread(
            target=self._bound_teardown, name="bazarr-plugin-watchdog", daemon=True
        ).start()
        tasks = [active.task for active in self._active.values() if active.task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=_TEARDOWN_SECONDS)
        return self._exit_code

    def _bound_teardown(self) -> None:
        """End the process when teardown outlasts its bound, even if tasks ignore cancellation."""
        if self._teardown_done.wait(_TEARDOWN_SECONDS):
            return
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except (OSError, ValueError):
                pass
        os._exit(self._exit_code)

    # ----- reader thread --------------------------------------------------

    def _read_frames(self) -> None:
        reader = self._reader
        loop = self._loop
        if reader is None or loop is None:  # pragma: no cover - set before the thread starts
            return
        headers: dict[str, dict[str, Any]] = {}
        assemblers: dict[str, BodyAssembler] = {}
        late_cancels: dict[str, dict[str, Any]] = {}
        try:
            while True:
                frame = reader.read_frame()
                if frame is None:
                    _post(loop, self._finish, EXIT_OK)
                    return
                kind = frame["t"]
                if kind == "chunk":
                    op = str(frame["op"])
                    assembler = assemblers.get(op)
                    if assembler is None or not assembler.accepts(frame):
                        raise FrameError("chunk frame without a call that expects it")
                    if assembler.add(frame):
                        del assemblers[op]
                        header = headers.pop(op)
                        _post(loop, self._dispatch, header, assembler.body())
                        cancel = late_cancels.pop(op, None)
                        if cancel is not None:
                            _post(loop, self._dispatch, cancel, None)
                    continue
                if kind == "call":
                    if not isinstance(frame.get("body", False), bool):
                        raise FrameError("call frame field 'body' must be a boolean")
                    if frame.get("body") is True:
                        op = str(frame["op"])
                        if op in headers:
                            raise FrameError("second call for an operation whose body is pending")
                        headers[op] = frame
                        assemblers[op] = BodyAssembler(
                            of="call",
                            op=op,
                            limit=self._limits.request_body_bytes,
                            limits=self._limits,
                        )
                        continue
                if kind == "cancel" and str(frame["op"]) in headers:
                    # The call body is still arriving: hold the cancel until the call is dispatched.
                    late_cancels[str(frame["op"])] = frame
                    continue
                _post(loop, self._dispatch, frame, None)
        except FrameError as exc:
            _report(f"plugin worker: protocol error: {exc}")
            _post(loop, self._finish, EXIT_PROTOCOL)
        except OSError as exc:
            _report(f"plugin worker: the host frame pipe failed: {exc}")
            _post(loop, self._finish, EXIT_ENVIRONMENT)
        except Exception as exc:
            # Any other failure must still end the worker, or it would wait forever.
            _report(f"plugin worker: the frame reader failed: {type(exc).__name__}")
            _post(loop, self._finish, EXIT_PROTOCOL)

    def _watch_input(self) -> None:
        loop = self._loop
        if loop is None:  # pragma: no cover - set before the thread starts
            return
        try:
            while os.read(_STDIN_FD, 1):
                pass
            code = EXIT_OK
        except OSError as exc:
            _report(f"plugin worker: standard input failed: {exc}")
            code = EXIT_ENVIRONMENT
        # The reader may hold a protocol error it has not posted yet. Waiting for
        # it keeps that error ahead of the clean exit.
        reader = self._reader_thread
        if reader is not None:
            reader.join(timeout=_READER_JOIN_SECONDS)
        _post(loop, self._finish, code)

    # ----- loop-thread dispatch -------------------------------------------

    def _finish(self, code: int) -> None:
        if code != EXIT_OK and self._exit_code == EXIT_OK:
            self._exit_code = code
        for future in self._pending.values():
            if not future.done():
                future.set_exception(HostGone("the host connection closed"))
        self._pending.clear()
        if self._stop is not None:
            self._stop.set()

    def _dispatch(self, frame: dict[str, Any], body: bytes | None) -> None:
        if self._stop is None or self._stop.is_set():
            return
        kind = frame["t"]
        op = str(frame["op"])
        if kind == "call":
            self._start_call(op, frame, body)
        elif kind == "cancel":
            abandon = frame.get("abandon", False)
            if not isinstance(abandon, bool):
                _report("plugin worker: cancel field 'abandon' must be a boolean")
                self._finish(EXIT_PROTOCOL)
                return
            self._cancel(op, abandon)
        elif kind == "ack":
            future = self._pending.pop(str(frame["req"]), None)
            if future is not None and not future.done():
                future.set_result(frame)
        elif kind == "shutdown":
            self._finish(EXIT_OK)
        else:
            _report(f"plugin worker: unexpected frame type {kind!r} from the host")
            self._finish(EXIT_PROTOCOL)

    def _start_call(self, op: str, frame: dict[str, Any], body: bytes | None) -> None:
        if op in self._active:
            _report(f"plugin worker: operation {op!r} was started twice")
            self._finish(EXIT_PROTOCOL)
            return
        if len(self._active) >= self._max_in_flight:
            self._send_error(
                OperationState(), op, PluginError(INTERNAL, "too many operations in flight")
            )
            return
        try:
            request, budget = self._build_request(frame, body)
        except ValueError as exc:
            _report(f"plugin worker: malformed call: {exc}")
            self._finish(EXIT_PROTOCOL)
            return
        if self._folders is None or self._loop is None:  # pragma: no cover - set by handshake
            self._finish(EXIT_PROTOCOL)
            return

        state = OperationState()
        active = _Active(state=state)
        self._active[op] = active
        context = PluginContext(
            operation_id=op,
            channel=self,
            state=state,
            folders=self._folders,
            heartbeat_interval=self._heartbeat_interval,
            budget=budget,
            limits=self._limits,
            log=logging.getLogger(LOGGER_NAME),
        )
        active.task = self._loop.create_task(self._run_operation(op, request, active, context))

    def _build_request(
        self, frame: dict[str, Any], body: bytes | None
    ) -> tuple[Request, dict[str, Any]]:
        method = frame.get("method")
        params = frame.get("params", {})
        secrets = frame.get("secrets", {})
        budget = frame.get("budget", {})
        if not isinstance(method, str) or not method:
            raise ValueError("call needs a method name")
        if not isinstance(params, dict) or not isinstance(budget, dict):
            raise ValueError("call fields 'params' and 'budget' must be objects")
        if not isinstance(secrets, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in secrets.items()
        ):
            raise ValueError("call field 'secrets' must map names to text")
        return Request(method=method, params=params, secrets=secrets, body=body), budget

    def _cancel(self, op: str, abandon: bool) -> None:
        active = self._active.get(op)
        if active is None:
            return
        active.state.cancelled.set()
        if abandon:
            active.state.abandoned.set()
        if active.started and active.task is not None:
            active.task.cancel()

    def _resolve(self, method: str) -> Callable[..., Any]:
        if self._plugin is None or method not in self._plugin_class.operations:
            raise _unknown_operation(method)
        function = getattr(self._plugin, method, None)
        if not callable(function):
            raise _unknown_operation(method)
        return function

    async def _run_operation(
        self,
        op: str,
        request: Request,
        active: _Active,
        context: PluginContext,
    ) -> None:
        active.started = True
        try:
            if active.state.cancelled.is_set():
                raise PluginError(CANCELLED, "operation cancelled")
            function = self._resolve(request.method)
            if inspect.iscoroutinefunction(function):
                value = await function(context, request)
            else:
                blocking = BlockingContext(context, asyncio.get_running_loop())
                value = await self._run_in_thread(function, blocking, request)
            if active.state.cancelled.is_set():
                raise PluginError(CANCELLED, "operation cancelled")
            try:
                self._send_result(active.state, op, value, request)
            except FrameTooLarge as exc:
                raise PluginError(INTERNAL, "the result is too large to send") from exc
            except FrameError as exc:
                # The result cannot be encoded, so the stream ends here.
                _report(f"plugin worker: the result cannot be encoded: {exc}")
                self._finish(EXIT_PROTOCOL)
        except asyncio.CancelledError:
            self._send_error(active.state, op, PluginError(CANCELLED, "operation cancelled"))
        except OperationAbandoned:
            pass
        except HostGone as exc:
            _report(f"plugin worker: {exc}")
            self._finish(EXIT_OK)
        except PluginError as exc:
            self._send_error(active.state, op, exc)
        except Exception as exc:
            traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
            sys.stderr.flush()
            self._send_error(active.state, op, internal_error(exc))
        finally:
            # The final frame has been sent by now in every path that sends one, and
            # nothing yields between the two, so the slot is free for a reused id.
            self._release(op, active)

    def _release(self, op: str, active: _Active) -> None:
        """Free the operation's slot, unless a later call has taken the same id."""
        if self._active.get(op) is active:
            del self._active[op]

    async def _run_in_thread(
        self,
        function: Callable[..., Any],
        context: BlockingContext,
        request: Request,
    ) -> object:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[object] = loop.create_future()

        def deliver(value: object, error: BaseException | None) -> None:
            if future.done():
                return
            if error is not None:
                future.set_exception(error)
            else:
                future.set_result(value)

        def run() -> None:
            try:
                value = function(context, request)
            except Exception as exc:
                _post(loop, deliver, None, exc)
            except BaseException as exc:
                _post(loop, deliver, None, RuntimeError(f"operation raised {type(exc).__name__}"))
            else:
                _post(loop, deliver, value, None)

        threading.Thread(target=run, name="bazarr-plugin-operation", daemon=True).start()
        return await future

    # ----- writing --------------------------------------------------------

    def write_frame(self, frame: Mapping[str, Any], body: bytes | None = None) -> None:
        """Write one frame, then its body as chunks when a body is given.

        The frame and its chunks are written under one lock, so no other frame
        can fall between them.
        """
        with self._writer.batch():
            self._writer.write(encode_frame(frame, limits=self._limits))
            if body is not None:
                owner = str(frame["t"])
                request_id = frame.get("req")
                for chunk in iter_body_frames(
                    of=owner,
                    op=str(frame["op"]),
                    data=body,
                    req=request_id if isinstance(request_id, str) else None,
                    limits=self._limits,
                ):
                    self._writer.write(encode_frame(chunk, limits=self._limits))

    def send(self, frame: Mapping[str, Any]) -> None:
        """Channel method: write a frame without waiting for an answer."""
        self.write_frame(frame)

    async def request(
        self, frame: Mapping[str, Any], body: bytes | None = None
    ) -> Mapping[str, Any]:
        """Channel method: write an awaited frame and return the host's ack."""
        loop = self._loop
        if loop is None or (self._stop is not None and self._stop.is_set()):
            raise HostGone("the host connection is closed")
        self._request_counter += 1
        req = str(self._request_counter)
        message = dict(frame)
        message["req"] = req
        if body is not None:
            message["body"] = True
        future: asyncio.Future[Mapping[str, Any]] = loop.create_future()
        self._pending[req] = future
        try:
            self.write_frame(message, body)
            return await future
        finally:
            self._pending.pop(req, None)

    def _send_terminal(
        self,
        state: OperationState,
        frame: dict[str, Any],
        body: bytes | None = None,
    ) -> None:
        """Send the final frame of an operation, at most once.

        FrameError is raised before any byte is written when the frame cannot be
        encoded, so the caller may send a different final frame instead. A body
        chunk that cannot be encoded after the frame is written leaves the
        stream unusable, so the worker ends with a protocol error.
        """
        with state.send_lock:
            if state.finished.is_set():
                return
            data = encode_frame(frame, limits=self._limits)
            with self._writer.batch():
                self._writer.write(data)
                if body is not None:
                    try:
                        for chunk in iter_body_frames(
                            of=str(frame["t"]),
                            op=str(frame["op"]),
                            data=body,
                            limits=self._limits,
                        ):
                            self._writer.write(encode_frame(chunk, limits=self._limits))
                    except FrameError as exc:
                        _report(f"plugin worker: a body chunk cannot be encoded: {exc}")
                        self._finish(EXIT_PROTOCOL)
            state.finished.set()

    def _send_result(
        self,
        state: OperationState,
        op: str,
        value: object,
        request: Request,
    ) -> None:
        result = _coerce_result(value)
        body = result.body
        if body is not None and len(body) > self._limits.result_limit(len(request.body or b"")):
            raise PluginError(INTERNAL, RESULT_LIMIT_MESSAGE)
        frame: dict[str, Any] = {
            "t": "result",
            "op": op,
            "value": None if result.value is None else dict(result.value),
            "body": body is not None,
        }
        self._send_terminal(state, frame, body)

    def _send_error(self, state: OperationState, op: str, error: PluginError) -> None:
        frame: dict[str, Any] = {
            "t": "error",
            "op": op,
            "code": error.code,
            "message": truncate_message(error.message),
            **error.wire_fields(),
        }
        try:
            try:
                self._send_terminal(state, frame)
            except FrameError:
                self._send_terminal(
                    state,
                    {
                        "t": "error",
                        "op": op,
                        "code": INTERNAL,
                        "message": "the error details are too large to send",
                    },
                )
        except FrameError:
            # Even the fallback cannot be encoded, so no final frame can be sent.
            _report("plugin worker: the error response cannot be encoded")
            self._finish(EXIT_PROTOCOL)
        except HostGone:
            self._finish(EXIT_OK)


def _configure_logging() -> None:
    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def serve(plugin_class: type[Plugin]) -> int:
    """Run ``plugin_class`` as a plugin worker and return the exit status.

    Typical use in a plugin's entry point::

        if __name__ == "__main__":
            raise SystemExit(serve(MyPlugin))

    The status is:

    - 0 for end of input: an orderly shutdown, the end of the frame stream, the
      end of standard input (before or after hello), or a closed host pipe while
      the worker writes.
    - 2 when the environment is wrong (a descriptor variable is missing,
      malformed, not open or a directory, or the class sets no ``kind_api``), or
      when a read fails on the frame or standard input descriptor.
    - 76 on a protocol error.
    - 78 when the host asks for a protocol or a kind this worker does not serve.

    After shutdown begins the worker has two seconds to finish. When it does
    not, the process ends with the status recorded so far.
    """
    kind_api = getattr(plugin_class, "kind_api", "")
    if not isinstance(kind_api, str) or not kind_api:
        _report("plugin worker: the plugin class must set a non-empty kind_api")
        return EXIT_ENVIRONMENT
    try:
        fd_in = _descriptor(ENV_FD_IN)
        fd_out = _descriptor(ENV_FD_OUT)
    except ValueError as exc:
        _report(f"plugin worker: {exc}")
        return EXIT_ENVIRONMENT
    try:
        os.dup2(2, 1)
    except OSError as exc:
        _report(f"plugin worker: could not redirect standard output: {exc}")
        return EXIT_ENVIRONMENT
    sys.stdout = sys.stderr
    _configure_logging()
    return _Worker(plugin_class, fd_in, fd_out).run()
