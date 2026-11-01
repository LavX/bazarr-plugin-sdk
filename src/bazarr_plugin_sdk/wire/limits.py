"""Size limits for frames and bodies, shared by the host and the plugin runtime.

Every body limit counts decoded bytes. Frame limits apply to the UTF-8 JSON
payload that follows the four-byte length prefix.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

PROTOCOL_VERSION = 1
LENGTH_PREFIX_BYTES = 4

KIB = 1024
MIB = 1024 * KIB

CONTROL_FRAME_BYTES = 64 * KIB
CHUNK_BYTES = 1 * MIB
REQUEST_BODY_BYTES = 16 * MIB
RESULT_BODY_BYTES = 16 * MIB
RESULT_FLOOR_BYTES = 1 * MIB
RESULT_FACTOR = 4
CHECKPOINT_BYTES = 8 * MIB


def base64_length(raw_length: int) -> int:
    """Return the length of the base64 text that encodes ``raw_length`` bytes."""
    return 4 * ((raw_length + 2) // 3)


def _is_plain_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class Limits:
    """The frame and body limits in force for one worker.

    The host sends these values in ``hello``. A worker may use smaller values
    than the defaults but never larger ones.
    """

    control_frame_bytes: int = CONTROL_FRAME_BYTES
    chunk_bytes: int = CHUNK_BYTES
    request_body_bytes: int = REQUEST_BODY_BYTES
    result_body_bytes: int = RESULT_BODY_BYTES
    result_floor_bytes: int = RESULT_FLOOR_BYTES
    result_factor: int = RESULT_FACTOR
    checkpoint_bytes: int = CHECKPOINT_BYTES

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if not _is_plain_int(value) or value < 1:
                raise ValueError(f"limit {field.name} must be a positive integer")

    @property
    def chunk_frame_bytes(self) -> int:
        """The largest JSON payload a chunk frame can have under these limits."""
        return base64_length(self.chunk_bytes) + self.control_frame_bytes

    def result_limit(self, request_body_length: int) -> int:
        """Return the largest result body allowed after a request of the given body length.

        The rule is ``max(result_floor_bytes, result_factor * request)``, capped at
        ``result_body_bytes``.
        """
        scaled = self.result_factor * max(0, request_body_length)
        return min(self.result_body_bytes, max(self.result_floor_bytes, scaled))

    def within(self, maximum: Limits) -> bool:
        """Return True when every limit is at or below the matching limit of ``maximum``."""
        return all(getattr(self, f.name) <= getattr(maximum, f.name) for f in fields(self))

    def to_dict(self) -> dict[str, int]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Limits:
        """Build limits from the ``limits`` object of a ``hello`` frame.

        Every field must be present, must be a positive integer and no other key is allowed.
        """
        if not isinstance(data, Mapping):
            raise ValueError("limits must be a JSON object")
        names = {f.name for f in fields(cls)}
        unknown = set(data) - names
        missing = names - set(data)
        if unknown:
            raise ValueError(f"unknown limit fields: {sorted(unknown)}")
        if missing:
            raise ValueError(f"missing limit fields: {sorted(missing)}")
        values: dict[str, int] = {}
        for name in names:
            value = data[name]
            if not _is_plain_int(value):
                raise ValueError(f"limit {name} must be an integer")
            values[name] = value
        return cls(**values)


DEFAULT_LIMITS = Limits()
