"""Frame codec: length-prefixed JSON frames and chunked bodies.

A frame is a four-byte big-endian length followed by a UTF-8 JSON object. The
object has a string type field ``t`` and an operation id ``op``. The empty
string is the operation id of connection-level frames (``hello``, ``hello_ack``
and ``shutdown``).

A body that is too large for one frame travels as a run of ``chunk`` frames.
Each chunk names the frame it belongs to in ``of``, carries the sequence
number ``seq``, a ``last`` flag and base64 ``data``. A chunk may carry ``req``
when the body belongs to a request id, as a checkpoint does.

This module uses only the standard library so that the host can import it.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any

from .limits import DEFAULT_LIMITS, LENGTH_PREFIX_BYTES, Limits

__all__ = [
    "BODY_OWNERS",
    "CHUNK_TYPE",
    "CONNECTION_TYPES",
    "FRAME_TYPES",
    "BodyAssembler",
    "FrameError",
    "FrameReader",
    "FrameTooLarge",
    "decode_chunk_data",
    "decode_payload",
    "encode_frame",
    "iter_body_frames",
    "max_frame_bytes",
    "validate_frame",
]


class FrameError(Exception):
    """A frame is malformed or does not follow the framing rules."""


class FrameTooLarge(FrameError):
    """A frame or a body exceeds a size limit."""


CHUNK_TYPE = "chunk"
CONNECTION_TYPES = frozenset({"hello", "hello_ack", "shutdown"})
REQUEST_TYPES = frozenset({"hello", "call", "cancel", "ack", "shutdown"})
# Frames the worker may send. ``log`` and ``host_call`` are reserved names that
# neither side sends in version 1, so they are not frame types at all.
WORKER_TYPES = frozenset(
    {
        "hello_ack",
        "progress",
        "heartbeat",
        "reserve",
        "usage",
        "checkpoint",
        "result",
        "error",
    }
)
FRAME_TYPES = REQUEST_TYPES | WORKER_TYPES | {CHUNK_TYPE}
BODY_OWNERS = frozenset({"call", "checkpoint", "result"})
REQUEST_ID_TYPES = frozenset({"ack", "reserve", "usage", "checkpoint"})
# Operation ids and request ids are text of at most this many characters.
_MAX_IDENTIFIER_CHARS = 256
# Arrays and objects may nest this deep. The frame object itself is level 1.
_MAX_NESTING_DEPTH = 64


def max_frame_bytes(frame_type: str, limits: Limits = DEFAULT_LIMITS) -> int:
    """Return the largest JSON payload allowed for a frame of ``frame_type``."""
    if frame_type == CHUNK_TYPE:
        return limits.chunk_frame_bytes
    return limits.control_frame_bytes


def decode_chunk_data(data: str, *, limits: Limits = DEFAULT_LIMITS) -> bytes:
    """Decode the base64 ``data`` of one chunk, enforcing the chunk size limit."""
    try:
        raw = base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise FrameError("chunk data is not valid base64") from exc
    if len(raw) > limits.chunk_bytes:
        raise FrameTooLarge(f"chunk of {len(raw)} bytes exceeds {limits.chunk_bytes}")
    return raw


def _is_identifier(value: object) -> bool:
    """Return True for text of at most 256 characters that encodes as UTF-8."""
    if not isinstance(value, str) or len(value) > _MAX_IDENTIFIER_CHARS:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _validate_chunk(frame: Mapping[str, Any], limits: Limits) -> None:
    of = frame.get("of")
    if not isinstance(of, str) or of not in BODY_OWNERS:
        raise FrameError("chunk frame must name the frame it belongs to in 'of'")
    seq = frame.get("seq")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
        raise FrameError("chunk frame needs a non-negative integer 'seq'")
    if not isinstance(frame.get("last"), bool):
        raise FrameError("chunk frame needs a boolean 'last'")
    data = frame.get("data")
    if not isinstance(data, str):
        raise FrameError("chunk frame needs string 'data'")
    if "req" in frame and not _is_identifier(frame["req"]):
        raise FrameError("chunk frame field 'req' must be text of at most 256 characters")
    decode_chunk_data(data, limits=limits)


def validate_frame(frame: Mapping[str, Any], *, limits: Limits = DEFAULT_LIMITS) -> None:
    """Check the structure of a decoded frame. Size limits are checked elsewhere."""
    frame_type = frame.get("t")
    if not isinstance(frame_type, str) or frame_type not in FRAME_TYPES:
        # The value is never repeated: it came from the peer and may be large.
        raise FrameError("unknown frame type")
    op = frame.get("op")
    if not _is_identifier(op):
        raise FrameError(
            f"{frame_type} frame needs a string operation id 'op' of at most 256 characters"
        )
    if frame_type in CONNECTION_TYPES:
        if op != "":
            raise FrameError(f"{frame_type} frame must use the empty operation id")
    elif not op:
        raise FrameError(f"{frame_type} frame needs a non-empty operation id")
    if frame_type in REQUEST_ID_TYPES and not _is_identifier(frame.get("req")):
        raise FrameError(f"{frame_type} frame needs a string field 'req' of at most 256 characters")
    if frame_type == CHUNK_TYPE:
        _validate_chunk(frame, limits)


def _check_nesting(frame: Mapping[str, Any]) -> None:
    """Refuse a frame whose arrays or objects nest deeper than the limit.

    The frame object is level 1. The walk keeps its own stack, so a deeply
    nested value cannot exhaust the interpreter's recursion limit, and it stops
    at the first container that is too deep.
    """
    pending: list[tuple[Any, int]] = [(frame, 1)]
    while pending:
        node, depth = pending.pop()
        children: Iterable[Any] = node.values() if isinstance(node, Mapping) else node
        for child in children:
            if isinstance(child, (Mapping, list, tuple)):
                if depth >= _MAX_NESTING_DEPTH:
                    raise FrameError(f"frame nests deeper than {_MAX_NESTING_DEPTH} levels")
                pending.append((child, depth + 1))


def _dump(frame: Mapping[str, Any]) -> bytes:
    _check_nesting(frame)
    try:
        text = json.dumps(dict(frame), separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise FrameError(f"frame is not JSON serialisable: {exc}") from exc
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise FrameError("frame contains text that is not valid UTF-8") from exc


def encode_frame(frame: Mapping[str, Any], *, limits: Limits = DEFAULT_LIMITS) -> bytes:
    """Serialise one frame, including its four-byte length prefix.

    Raises ``FrameTooLarge`` when the JSON payload exceeds the limit for its type.
    """
    validate_frame(frame, limits=limits)
    payload = _dump(frame)
    frame_type = str(frame["t"])
    limit = max_frame_bytes(frame_type, limits)
    if len(payload) > limit:
        raise FrameTooLarge(f"{frame_type} frame of {len(payload)} bytes exceeds {limit}")
    return len(payload).to_bytes(LENGTH_PREFIX_BYTES, "big") + payload


def _reject_constant(name: str) -> None:
    raise FrameError(f"frame contains the non-standard constant {name}")


def decode_payload(payload: bytes, *, limits: Limits = DEFAULT_LIMITS) -> dict[str, Any]:
    """Decode the JSON payload of one frame and check its structure and size.

    A payload the decoder refuses, such as an integer with more digits than
    Python converts, is a frame error. So is nesting deeper than 64 levels,
    whether or not the decoder accepts it.
    """
    try:
        frame = json.loads(payload.decode("utf-8"), parse_constant=_reject_constant)
    except (ValueError, RecursionError) as exc:
        raise FrameError(f"frame is not UTF-8 JSON that the decoder accepts: {exc}") from exc
    if not isinstance(frame, dict):
        raise FrameError("frame must be a JSON object")
    _check_nesting(frame)
    validate_frame(frame, limits=limits)
    limit = max_frame_bytes(str(frame["t"]), limits)
    if len(payload) > limit:
        raise FrameTooLarge(f"{frame['t']} frame of {len(payload)} bytes exceeds {limit}")
    return frame


ReadFunction = Callable[[int], bytes]


def _read_exact(read: ReadFunction, count: int, *, allow_eof: bool) -> bytes | None:
    buffer = bytearray()
    while len(buffer) < count:
        piece = read(count - len(buffer))
        if not piece:
            if not buffer and allow_eof:
                return None
            raise FrameError("the connection closed inside a frame")
        buffer.extend(piece)
    return bytes(buffer)


class FrameReader:
    """Reads frames from a byte source.

    ``read(n)`` must return between one and ``n`` bytes, or ``b""`` at end of
    input. The length prefix is checked against the channel limit before the
    body is read, so an oversize frame never allocates its body.
    """

    def __init__(self, read: ReadFunction, *, limits: Limits = DEFAULT_LIMITS) -> None:
        self._read = read
        self._limits = limits

    def read_frame(self) -> dict[str, Any] | None:
        """Return the next frame, or None when the input ends between frames."""
        header = _read_exact(self._read, LENGTH_PREFIX_BYTES, allow_eof=True)
        if header is None:
            return None
        length = int.from_bytes(header, "big")
        if length == 0:
            raise FrameError("empty frame")
        channel_limit = self._limits.chunk_frame_bytes
        if length > channel_limit:
            raise FrameTooLarge(
                f"frame of {length} bytes exceeds the channel limit of {channel_limit}; "
                "refused before the body was read"
            )
        payload = _read_exact(self._read, length, allow_eof=False)
        if payload is None:  # pragma: no cover - allow_eof is False above
            raise FrameError("the connection closed inside a frame")
        return decode_payload(payload, limits=self._limits)


def iter_body_frames(
    *,
    of: str,
    op: str,
    data: bytes,
    req: str | None = None,
    limits: Limits = DEFAULT_LIMITS,
) -> Iterator[dict[str, Any]]:
    """Split a body into chunk frames. An empty body yields one empty chunk marked last."""
    if of not in BODY_OWNERS:
        raise FrameError(f"a body cannot belong to a {of!r} frame")
    size = limits.chunk_bytes
    count = max(1, -(-len(data) // size))
    for seq in range(count):
        piece = data[seq * size : (seq + 1) * size]
        frame: dict[str, Any] = {
            "t": CHUNK_TYPE,
            "op": op,
            "of": of,
            "seq": seq,
            "last": seq == count - 1,
            "data": base64.b64encode(piece).decode("ascii"),
        }
        if req is not None:
            frame["req"] = req
        yield frame


class BodyAssembler:
    """Collects the chunks of one body.

    Chunks must arrive in sequence, must belong to the same owner frame and
    must keep the decoded total within ``limit``.
    """

    def __init__(
        self,
        *,
        of: str,
        op: str,
        limit: int,
        req: str | None = None,
        limits: Limits = DEFAULT_LIMITS,
    ) -> None:
        self.key = (op, of, req)
        self._limit = limit
        self._limits = limits
        self._parts: list[bytes] = []
        self._size = 0
        self._next_seq = 0
        self._complete = False

    @property
    def complete(self) -> bool:
        return self._complete

    def accepts(self, frame: Mapping[str, Any]) -> bool:
        """Return True when ``frame`` is a chunk of this body."""
        return (frame.get("op"), frame.get("of"), frame.get("req")) == self.key

    def add(self, frame: Mapping[str, Any]) -> bool:
        """Add one chunk. Returns True when it was the last chunk."""
        if self._complete:
            raise FrameError("chunk received after the last chunk of its body")
        if frame.get("t") != CHUNK_TYPE or not self.accepts(frame):
            raise FrameError("chunk does not belong to the body being assembled")
        if frame.get("seq") != self._next_seq:
            raise FrameError(f"chunk sequence {frame.get('seq')} out of order")
        raw = decode_chunk_data(str(frame.get("data")), limits=self._limits)
        if self._size + len(raw) > self._limit:
            raise FrameTooLarge(f"body exceeds the limit of {self._limit} bytes")
        self._parts.append(raw)
        self._size += len(raw)
        self._next_seq += 1
        if frame.get("last") is True:
            self._complete = True
        return self._complete

    def body(self) -> bytes:
        """Return the assembled body. Only valid once the last chunk has arrived."""
        if not self._complete:
            raise FrameError("body is not complete")
        return b"".join(self._parts)
