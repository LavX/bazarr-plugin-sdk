"""The context a plugin operation receives, and the request and result types.

``progress`` and ``heartbeat`` return at once. ``reserve``, ``usage`` and
``checkpoint`` return only after the host has acknowledged them, so a plugin
can rely on a stored checkpoint or a recorded cost once the call returns.

Operations written as ``async def`` receive a ``PluginContext`` and await the
awaited calls. Operations written as plain ``def`` run in a worker thread and
receive a ``BlockingContext``, whose awaited calls block the thread instead.
"""

from __future__ import annotations

import asyncio
import logging
import math
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .errors import CODES, INTERNAL, OperationAbandoned, PluginError
from .wire import DEFAULT_LIMITS, Limits

__all__ = [
    "BlockingContext",
    "Channel",
    "Folders",
    "OperationState",
    "PluginContext",
    "Request",
    "Result",
    "raise_for_ack",
]

LOGGER_NAME = "bazarr_plugin_sdk.plugin"


@dataclass(frozen=True)
class Folders:
    """The three folders of one worker. They do not change for the life of the worker."""

    data_dir: Path
    temp_dir: Path
    artifacts_dir: Path


@dataclass(frozen=True)
class Request:
    """One call from the host.

    ``params`` is the JSON object the host sent. ``secrets`` holds the stored
    credentials for this call and is the only place they appear. ``body`` is
    the request body in bytes, or None when the call sent none.
    """

    method: str
    params: Mapping[str, Any]
    secrets: Mapping[str, str]
    body: bytes | None = None


@dataclass(frozen=True)
class Result:
    """What an operation returns when it has a body or wants to be explicit.

    Returning a plain mapping is the same as ``Result(value=mapping)``.
    Returning None is the same as ``Result()``.
    """

    value: Mapping[str, Any] | None = None
    body: bytes | None = None


@dataclass
class OperationState:
    """Cancellation flags shared between the reader and one operation."""

    cancelled: threading.Event = field(default_factory=threading.Event)
    abandoned: threading.Event = field(default_factory=threading.Event)


class Channel(Protocol):
    """What a context needs from the worker to talk to the host."""

    def send(self, frame: Mapping[str, Any]) -> None:
        """Write one frame without waiting for an answer."""

    async def request(
        self, frame: Mapping[str, Any], body: bytes | None = None
    ) -> Mapping[str, Any]:
        """Write one awaited frame and return the host's ack."""


def raise_for_ack(ack: Mapping[str, Any]) -> None:
    """Raise the error the host reported in an ack that is not ok."""
    if ack.get("ok") is True:
        return
    error = ack.get("error")
    if isinstance(error, Mapping):
        code = error.get("code")
        message = str(error.get("message", ""))
    else:
        code = INTERNAL
        message = "the host refused the request"
    if code not in CODES:
        code = INTERNAL
    raise PluginError(str(code), message)


def _count(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be zero or more")
    return value


def _number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{name} must be a finite number of zero or more")
    return number


def _text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _as_bytes(data: object) -> bytes:
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data)
    raise TypeError("checkpoint data must be bytes")


class PluginContext:
    """What an ``async`` operation uses to talk to the host.

    ``data_dir``, ``temp_dir`` and ``artifacts_dir`` are fixed for the life of
    the worker, so every operation on one worker sees the same folders. Do not
    cache results in a way that assumes a different folder.

    ``budget`` is the object the host sent with the call (for example a cost
    ceiling). Its keys belong to the kind. ``cancelled`` is set when the host
    cancels the operation. ``heartbeat_interval`` is the number of seconds the
    host allows between heartbeats. ``log`` writes to the worker's stderr.
    """

    def __init__(
        self,
        *,
        operation_id: str,
        channel: Channel,
        state: OperationState,
        folders: Folders,
        heartbeat_interval: float,
        budget: Mapping[str, Any] | None = None,
        limits: Limits = DEFAULT_LIMITS,
        log: logging.Logger | None = None,
    ) -> None:
        self._operation_id = operation_id
        self._channel = channel
        self._state = state
        self._limits = limits
        self.data_dir: Path = folders.data_dir
        self.temp_dir: Path = folders.temp_dir
        self.artifacts_dir: Path = folders.artifacts_dir
        self.heartbeat_interval: float = float(heartbeat_interval)
        self.budget: Mapping[str, Any] = dict(budget or {})
        self.cancelled: threading.Event = state.cancelled
        self.log: logging.Logger = log or logging.getLogger(LOGGER_NAME)

    def _ensure_open(self) -> None:
        if self._state.abandoned.is_set():
            raise OperationAbandoned("the host abandoned this operation")

    def progress(self, done: int, total: int | None = None, message: str = "") -> None:
        """Report progress. Returns at once without waiting for the host."""
        self._ensure_open()
        frame: dict[str, Any] = {
            "t": "progress",
            "op": self._operation_id,
            "done": _count(done, "done"),
            "total": None if total is None else _count(total, "total"),
            "message": _text(message, "message"),
        }
        self._channel.send(frame)

    def heartbeat(self) -> None:
        """Tell the host the operation is alive. Returns at once without waiting.

        Raises ``HostGone`` when the host connection has closed, and
        ``OperationAbandoned`` when the host has abandoned the operation.
        """
        self._ensure_open()
        self._channel.send({"t": "heartbeat", "op": self._operation_id})

    async def reserve(self, amount: float, model: str) -> str | None:
        """Reserve ``amount`` of spend for ``model`` and return the reservation id.

        Returns None when the host does not grant a reservation. Raises
        ``PluginError`` with the code the host reports when it refuses the
        reservation.
        """
        self._ensure_open()
        frame = {
            "t": "reserve",
            "op": self._operation_id,
            "amount": _number(amount, "amount"),
            "model": _text(model, "model"),
        }
        ack = await self._channel.request(frame)
        raise_for_ack(ack)
        reservation = ack.get("reservation")
        if reservation is None:
            return None
        if not isinstance(reservation, str):
            raise PluginError(INTERNAL, "the host returned a malformed reservation")
        return reservation

    async def usage(
        self,
        tokens: int,
        cost: float,
        model: str,
        reservations: Sequence[str] = (),
    ) -> None:
        """Record the tokens and cost of one call against its reservations."""
        self._ensure_open()
        frame = {
            "t": "usage",
            "op": self._operation_id,
            "tokens": _count(tokens, "tokens"),
            "cost": _number(cost, "cost"),
            "model": _text(model, "model"),
            "reservations": [_text(item, "reservation") for item in reservations],
        }
        ack = await self._channel.request(frame)
        raise_for_ack(ack)

    async def checkpoint(self, data: bytes) -> None:
        """Store ``data`` so a later attempt can resume from it.

        Returns once the host has stored it. A checkpoint may be at most
        ``checkpoint_bytes`` long, which is 8 MiB under the default limits.
        """
        self._ensure_open()
        payload = _as_bytes(data)
        if len(payload) > self._limits.checkpoint_bytes:
            raise ValueError(
                f"checkpoint of {len(payload)} bytes exceeds {self._limits.checkpoint_bytes}"
            )
        ack = await self._channel.request({"t": "checkpoint", "op": self._operation_id}, payload)
        raise_for_ack(ack)


class BlockingContext:
    """The context for a plain ``def`` operation that runs in a worker thread.

    ``progress`` and ``heartbeat`` behave as they do on ``PluginContext``.
    ``reserve``, ``usage`` and ``checkpoint`` are ordinary blocking calls that
    return once the host has acknowledged them. Do not call them from the event
    loop thread: they wait for that loop and would deadlock.
    """

    def __init__(self, context: PluginContext, loop: asyncio.AbstractEventLoop) -> None:
        self._context = context
        self._loop = loop
        self.data_dir = context.data_dir
        self.temp_dir = context.temp_dir
        self.artifacts_dir = context.artifacts_dir
        self.heartbeat_interval = context.heartbeat_interval
        self.budget = context.budget
        self.cancelled = context.cancelled
        self.log = context.log

    def progress(self, done: int, total: int | None = None, message: str = "") -> None:
        self._context.progress(done, total, message)

    def heartbeat(self) -> None:
        self._context.heartbeat()

    def reserve(self, amount: float, model: str) -> str | None:
        return self._wait(self._context.reserve(amount, model))

    def usage(
        self,
        tokens: int,
        cost: float,
        model: str,
        reservations: Sequence[str] = (),
    ) -> None:
        self._wait(self._context.usage(tokens, cost, model, reservations))

    def checkpoint(self, data: bytes) -> None:
        self._wait(self._context.checkpoint(data))

    def _wait(self, awaitable: Any) -> Any:
        future = asyncio.run_coroutine_threadsafe(awaitable, self._loop)
        return future.result()
