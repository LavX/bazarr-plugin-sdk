"""Wire protocol between the Bazarr host and a plugin worker.

The package is standard-library only. It holds the frame codec, the chunked
body helpers and the size limits. The specification is in ``docs/protocol.md``.
"""

from __future__ import annotations

from .frames import (
    BODY_OWNERS,
    CHUNK_TYPE,
    CONNECTION_TYPES,
    FRAME_TYPES,
    BodyAssembler,
    FrameError,
    FrameReader,
    FrameTooLarge,
    decode_chunk_data,
    decode_payload,
    encode_frame,
    iter_body_frames,
    max_frame_bytes,
    validate_frame,
)
from .limits import (
    CHECKPOINT_BYTES,
    CHUNK_BYTES,
    CONTROL_FRAME_BYTES,
    DEFAULT_LIMITS,
    PROTOCOL_VERSION,
    REQUEST_BODY_BYTES,
    RESULT_BODY_BYTES,
    Limits,
)

__all__ = [
    "BODY_OWNERS",
    "CHECKPOINT_BYTES",
    "CHUNK_BYTES",
    "CHUNK_TYPE",
    "CONNECTION_TYPES",
    "CONTROL_FRAME_BYTES",
    "DEFAULT_LIMITS",
    "FRAME_TYPES",
    "PROTOCOL_VERSION",
    "REQUEST_BODY_BYTES",
    "RESULT_BODY_BYTES",
    "BodyAssembler",
    "FrameError",
    "FrameReader",
    "FrameTooLarge",
    "Limits",
    "decode_chunk_data",
    "decode_payload",
    "encode_frame",
    "iter_body_frames",
    "max_frame_bytes",
    "validate_frame",
]
