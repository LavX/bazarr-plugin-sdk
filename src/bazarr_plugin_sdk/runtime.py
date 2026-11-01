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
import os
import select
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

EXIT_OK = 0
EXIT_ENVIRONMENT = 2
EXIT_PROTOCOL = 76
EXIT_CONTRACT_NOT_SERVED = 78

RESULT_LIMIT_MESSAGE = "the result body exceeds the result limit"

_TEMP_VARIABLES = ("TMPDIR", "TMP", "TEMP")


def _report(text: str) -> None:
    try:
        sys.stderr.write(text if text.endswith("\n") else text + "\n")
        sys.stderr.flush()
    except (OSError, ValueError):
        pass


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

    # ----- start-up -------------------------------------------------------

    def run(self) -> int:
        handshake_reader = FrameReader(self._read_before_hello)
        status = self._handshake(handshake_reader)
        if status is not None:
            return status
        self._reader = FrameReader(self._read_input, limits=self._limits)
        return asyncio.run(self._serve())

    def _read_input(self, count: int) -> bytes:
        return os.read(self._fd_in, count)

    def _read_before_hello(self, count: int) -> bytes:
        """Read the handshake from the frame descriptor while watching standard input.

        End of standard input before hello ends the worker, as it does after
        hello. Bytes that arrive on standard input are discarded.
        """
        while True:
            try:
                ready, _, _ = select.select([self._fd_in, _STDIN_FD], [], [])
                if self._fd_in in ready:
                    return os.read(self._fd_in, count)
                if not os.read(_STDIN_FD, 1):
                    raise HostGone("standard input closed before hello")
            except OSError as exc:
                raise HostGone(f"standard input failed before hello: {exc}") from exc

    def _handshake(self, reader: FrameReader) -> int | None:
        try:
            hello = reader.read_frame()
        except FrameError as exc:
            _report(f"plugin worker: protocol error before hello: {exc}")
            return EXIT_PROTOCOL
        except HostGone as exc:
            _report(f"plugin worker: {exc}")
            return EXIT_OK
        if hello is None or hello.get("t") != "hello":
            _report("plugin worker: the first frame must be hello")
            return EXIT_PROTOCOL
        protocol = hello.get("protocol")
        if isinstance(protocol, bool):
            _report("plugin worker: malformed hello: protocol must be an integer")
            return EXIT_PROTOCOL
        if protocol != PROTOCOL_VERSION:
            _report(f"plugin worker: host protocol {protocol!r} is not served")
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
            interval = hello.get("heartbeat_interval")
            if isinstance(interval, bool) or not isinstance(interval, (int, float)):
                raise ValueError("heartbeat_interval must be a positive number")
            if interval <= 0:
                raise ValueError("heartbeat_interval must be a positive number")
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
        self._heartbeat_interval = float(interval)
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
        except (HostGone, FrameError) as exc:
            _report(f"plugin worker: could not send hello_ack: {exc}")
            return EXIT_PROTOCOL
        return None

    async def _serve(self) -> int:
        self._loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        threading.Thread(target=self._read_frames, name="bazarr-plugin-reader", daemon=True).start()
        threading.Thread(target=self._watch_input, name="bazarr-plugin-stdin", daemon=True).start()
        await self._stop.wait()
        tasks = [active.task for active in self._active.values() if active.task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=2.0)
        return self._exit_code

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
            _report(f"plugin worker: the host pipe failed: {exc}")
            _post(loop, self._finish, EXIT_OK)

    def _watch_input(self) -> None:
        loop = self._loop
        if loop is None:  # pragma: no cover - set before the thread starts
            return
        try:
            while os.read(0, 1):
                pass
        except OSError:
            pass
        _post(loop, self._finish, EXIT_OK)

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
            self._send_error(op, PluginError(INTERNAL, "too many operations in flight"))
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
            raise PluginError(INTERNAL, f"unknown operation {method!r}")
        function = getattr(self._plugin, method, None)
        if not callable(function):
            raise PluginError(INTERNAL, f"unknown operation {method!r}")
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
            self._send_result(op, value, request)
        except asyncio.CancelledError:
            self._send_error(op, PluginError(CANCELLED, "operation cancelled"))
        except OperationAbandoned:
            pass
        except HostGone as exc:
            _report(f"plugin worker: {exc}")
            self._finish(EXIT_OK)
        except PluginError as exc:
            self._send_error(op, exc)
        except Exception as exc:
            traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
            sys.stderr.flush()
            self._send_error(op, internal_error(exc))
        finally:
            self._active.pop(op, None)

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
        except BaseException:
            self._pending.pop(req, None)
            raise
        return await future

    def _send_result(self, op: str, value: object, request: Request) -> None:
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
        self.write_frame(frame, body)

    def _send_error(self, op: str, error: PluginError) -> None:
        frame: dict[str, Any] = {
            "t": "error",
            "op": op,
            "code": error.code,
            "message": truncate_message(error.message),
            **error.wire_fields(),
        }
        try:
            try:
                data = encode_frame(frame, limits=self._limits)
            except FrameError:
                data = encode_frame(
                    {
                        "t": "error",
                        "op": op,
                        "code": INTERNAL,
                        "message": "the error details are too large to send",
                    },
                    limits=self._limits,
                )
            self._writer.write(data)
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

    The status is 0 after an orderly shutdown or when the host closes its
    streams, 2 when the environment is wrong, 76 on a protocol error, and 78
    when the host asks for a kind this worker does not serve.
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

    os.dup2(2, 1)
    sys.stdout = sys.stderr
    _configure_logging()
    return _Worker(plugin_class, fd_in, fd_out).run()
