"""Tests for the frame codec, the chunked bodies and the size limits."""

from __future__ import annotations

import base64
import io
import json
import struct
from typing import Any

import pytest

from bazarr_plugin_sdk.wire import (
    CHECKPOINT_BYTES,
    CHUNK_BYTES,
    CONTROL_FRAME_BYTES,
    DEFAULT_LIMITS,
    PROTOCOL_VERSION,
    REQUEST_BODY_BYTES,
    RESULT_BODY_BYTES,
    BodyAssembler,
    FrameError,
    FrameReader,
    FrameTooLarge,
    Limits,
    decode_payload,
    encode_frame,
    iter_body_frames,
    max_frame_bytes,
    validate_frame,
)
from bazarr_plugin_sdk.wire.limits import base64_length

MIB = 1024 * 1024


def reader_for(data: bytes, limits: Limits = DEFAULT_LIMITS) -> FrameReader:
    return FrameReader(io.BytesIO(data).read, limits=limits)


def test_protocol_and_limit_constants_match_the_specification() -> None:
    assert PROTOCOL_VERSION == 1
    assert CONTROL_FRAME_BYTES == 64 * 1024
    assert CHUNK_BYTES == MIB
    assert REQUEST_BODY_BYTES == 16 * MIB
    assert RESULT_BODY_BYTES == 16 * MIB
    assert CHECKPOINT_BYTES == 8 * MIB
    assert DEFAULT_LIMITS.chunk_frame_bytes == base64_length(MIB) + 64 * 1024


def test_base64_length_matches_the_encoder() -> None:
    for size in (0, 1, 2, 3, 4, 1000, MIB):
        encoded = base64.b64encode(b"x" * size)
        assert base64_length(size) == len(encoded)


def test_encode_writes_a_big_endian_length_prefix_then_compact_json() -> None:
    frame = {"t": "heartbeat", "op": "op-1"}
    data = encode_frame(frame)
    payload = json.dumps(frame, separators=(",", ":")).encode("utf-8")
    assert data[:4] == struct.pack(">I", len(payload))
    assert data[4:] == payload


def test_round_trip_for_connection_and_operation_frames() -> None:
    frames = [
        {"t": "hello", "op": "", "protocol": 1, "kind_api": "sample.v1"},
        {"t": "call", "op": "op-1", "method": "echo", "params": {"a": 1}, "body": False},
        {"t": "result", "op": "op-1", "value": {"text": "héllo"}, "body": False},
        {"t": "ack", "op": "op-1", "req": "7", "ok": True, "reservation": None},
        {"t": "shutdown", "op": ""},
    ]
    stream = b"".join(encode_frame(frame) for frame in frames)
    reader = reader_for(stream)
    decoded = [reader.read_frame() for _ in frames]
    assert decoded == frames
    assert reader.read_frame() is None


def test_end_of_input_between_frames_returns_none() -> None:
    assert reader_for(b"").read_frame() is None


def test_end_of_input_inside_a_frame_is_an_error() -> None:
    data = encode_frame({"t": "heartbeat", "op": "op-1"})
    with pytest.raises(FrameError, match="inside a frame"):
        reader_for(data[:-3]).read_frame()
    with pytest.raises(FrameError, match="inside a frame"):
        reader_for(data[:2]).read_frame()


def test_unknown_frame_type_is_refused() -> None:
    with pytest.raises(FrameError, match="unknown frame type"):
        encode_frame({"t": "teleport", "op": "op-1"})


def test_missing_operation_id_is_refused() -> None:
    with pytest.raises(FrameError, match="operation id"):
        validate_frame({"t": "heartbeat"})


def test_empty_operation_id_is_only_allowed_for_connection_frames() -> None:
    with pytest.raises(FrameError, match="non-empty operation id"):
        encode_frame({"t": "heartbeat", "op": ""})
    encode_frame({"t": "hello", "op": ""})
    encode_frame({"t": "shutdown", "op": ""})


def test_request_id_is_required_for_awaited_frames() -> None:
    for frame_type in ("ack", "reserve", "usage", "checkpoint"):
        with pytest.raises(FrameError, match="'req'"):
            encode_frame({"t": frame_type, "op": "op-1"})


def test_non_json_values_are_refused() -> None:
    with pytest.raises(FrameError, match="not JSON"):
        encode_frame({"t": "result", "op": "op-1", "value": {"x": object()}, "body": False})
    with pytest.raises(FrameError, match="not JSON"):
        encode_frame({"t": "result", "op": "op-1", "value": {"x": float("nan")}, "body": False})


def test_control_frame_over_the_limit_is_refused_when_encoded() -> None:
    frame = {"t": "progress", "op": "op-1", "done": 0, "total": 1, "message": "x" * 70_000}
    with pytest.raises(FrameTooLarge, match="exceeds"):
        encode_frame(frame)


def test_control_frame_over_the_limit_is_refused_when_decoded() -> None:
    payload = json.dumps(
        {"t": "progress", "op": "op-1", "done": 0, "total": 1, "message": "x" * 70_000},
        separators=(",", ":"),
    ).encode("utf-8")
    stream = len(payload).to_bytes(4, "big") + payload
    with pytest.raises(FrameTooLarge):
        reader_for(stream).read_frame()


def test_oversize_length_prefix_is_refused_before_the_body_is_read() -> None:
    source = io.BytesIO((2_000_000).to_bytes(4, "big") + b"x" * 2_000_000)
    reader = FrameReader(source.read)
    with pytest.raises(FrameTooLarge, match="before the body was read"):
        reader.read_frame()
    assert source.tell() == 4


def test_zero_length_frame_is_refused() -> None:
    with pytest.raises(FrameError, match="empty frame"):
        reader_for(b"\x00\x00\x00\x00").read_frame()


def test_non_object_json_is_refused() -> None:
    payload = b"[1, 2, 3]"
    with pytest.raises(FrameError, match="JSON object"):
        decode_payload(payload)


def test_invalid_utf8_is_refused() -> None:
    with pytest.raises(FrameError, match="UTF-8"):
        decode_payload(b'{"t":"heartbeat","op":"\xff"}')


def test_non_standard_json_constants_are_refused() -> None:
    with pytest.raises(FrameError, match="non-standard constant"):
        decode_payload(b'{"t":"progress","op":"op-1","done":NaN}')


def test_integer_beyond_the_decoder_digit_limit_is_a_frame_error() -> None:
    payload = b'{"t":"progress","op":"op-1","done":' + b"1" * 5000 + b"}"
    assert len(payload) < CONTROL_FRAME_BYTES
    with pytest.raises(FrameError, match="UTF-8 JSON"):
        decode_payload(payload)


def nested_result(levels: int) -> dict[str, Any]:
    """A result frame whose deepest container is at ``levels``, the frame being level 1."""
    value: list[Any] = []
    for _ in range(levels - 2):
        value = [value]
    return {"t": "result", "op": "op-1", "value": value, "body": False}


def test_nesting_of_64_levels_is_accepted_on_encode_and_decode() -> None:
    frame = nested_result(64)
    assert reader_for(encode_frame(frame)).read_frame() == frame


def test_nesting_of_65_levels_is_refused_on_encode() -> None:
    with pytest.raises(FrameError, match="64 levels"):
        encode_frame(nested_result(65))


def test_nesting_of_65_levels_is_refused_on_decode() -> None:
    payload = json.dumps(nested_result(65), separators=(",", ":")).encode("utf-8")
    with pytest.raises(FrameError, match="64 levels"):
        decode_payload(payload)


def test_nesting_of_30000_levels_is_refused_on_decode() -> None:
    depth = 30_000
    payload = b'{"t":"progress","op":"op-1","message":' + b"[" * depth + b"]" * depth + b"}"
    assert len(payload) < CONTROL_FRAME_BYTES
    with pytest.raises(FrameError):
        decode_payload(payload)


def test_nesting_of_1000_levels_is_refused_with_the_nesting_message() -> None:
    depth = 1_000
    payload = b'{"t":"progress","op":"op-1","message":' + b"[" * depth + b"]" * depth + b"}"
    assert len(payload) < CONTROL_FRAME_BYTES
    with pytest.raises(FrameError, match="frame nests deeper than 64 levels"):
        decode_payload(payload)


def test_unknown_frame_type_message_never_repeats_the_value() -> None:
    for value in ("teleport", ["secret-token"], {"k": "secret-token"}, 7, None):
        with pytest.raises(FrameError) as info:
            validate_frame({"t": value, "op": "op-1"})
        assert str(info.value) == "unknown frame type"


def test_chunk_owner_that_is_not_text_is_a_frame_error() -> None:
    base = {"t": "chunk", "op": "op-1", "of": "result", "seq": 0, "last": True, "data": ""}
    for value in ([], {}, ["result"], None, 3):
        with pytest.raises(FrameError, match="'of'"):
            validate_frame({**base, "of": value})


def test_operation_and_request_ids_are_at_most_256_characters_of_utf8() -> None:
    validate_frame({"t": "heartbeat", "op": "x" * 256})
    with pytest.raises(FrameError, match="256"):
        validate_frame({"t": "heartbeat", "op": "x" * 257})
    with pytest.raises(FrameError, match="256"):
        validate_frame({"t": "heartbeat", "op": "\ud800"})
    with pytest.raises(FrameError, match="256"):
        decode_payload(b'{"t":"heartbeat","op":"\\ud800"}')
    validate_frame({"t": "ack", "op": "op-1", "req": "r" * 256, "ok": True})
    with pytest.raises(FrameError, match="'req'"):
        validate_frame({"t": "ack", "op": "op-1", "req": "r" * 257, "ok": True})
    with pytest.raises(FrameError, match="'req'"):
        validate_frame({"t": "ack", "op": "op-1", "req": "\ud800", "ok": True})


def test_connection_frames_must_use_the_empty_operation_id() -> None:
    for frame_type in ("hello", "hello_ack", "shutdown"):
        with pytest.raises(FrameError, match="empty operation id"):
            validate_frame({"t": frame_type, "op": "op-1"})
    validate_frame({"t": "hello_ack", "op": ""})


def test_log_and_host_call_are_reserved_and_not_frame_types() -> None:
    for frame_type in ("log", "host_call"):
        with pytest.raises(FrameError, match="unknown frame type"):
            validate_frame({"t": frame_type, "op": "op-1"})
    with pytest.raises(FrameError, match="unknown frame type"):
        decode_payload(b'{"t":"log","op":"op-1"}')


def test_max_frame_bytes_depends_on_frame_type() -> None:
    assert max_frame_bytes("chunk") == DEFAULT_LIMITS.chunk_frame_bytes
    assert max_frame_bytes("call") == CONTROL_FRAME_BYTES


def test_iter_body_frames_splits_into_bounded_chunks() -> None:
    data = bytes(range(256)) * (10 * 1024)  # 2.5 MiB
    frames = list(iter_body_frames(of="result", op="op-1", data=data))
    assert len(frames) == 3
    assert [frame["seq"] for frame in frames] == [0, 1, 2]
    assert [frame["last"] for frame in frames] == [False, False, True]
    for frame in frames:
        assert frame["t"] == "chunk"
        assert frame["of"] == "result"
        assert "req" not in frame
        assert len(base64.b64decode(frame["data"])) <= CHUNK_BYTES
        encode_frame(frame)


def test_empty_body_is_one_empty_chunk_marked_last() -> None:
    frames = list(iter_body_frames(of="checkpoint", op="op-1", req="3", data=b""))
    assert len(frames) == 1
    assert frames[0]["data"] == ""
    assert frames[0]["last"] is True
    assert frames[0]["req"] == "3"


def test_body_cannot_belong_to_a_non_body_frame() -> None:
    with pytest.raises(FrameError, match="cannot belong"):
        list(iter_body_frames(of="progress", op="op-1", data=b"x"))


def test_body_assembler_rebuilds_the_bytes_in_order() -> None:
    data = b"abcdefgh" * 400_000  # about 3.2 MB
    frames = list(iter_body_frames(of="call", op="op-1", data=data, limits=DEFAULT_LIMITS))
    assembler = BodyAssembler(of="call", op="op-1", limit=REQUEST_BODY_BYTES)
    for frame in frames:
        assert assembler.accepts(frame)
        assembler.add(frame)
    assert assembler.complete
    assert assembler.body() == data


def test_body_assembler_refuses_out_of_order_chunks() -> None:
    frames = list(iter_body_frames(of="call", op="op-1", data=b"x" * (2 * MIB)))
    assembler = BodyAssembler(of="call", op="op-1", limit=REQUEST_BODY_BYTES)
    with pytest.raises(FrameError, match="out of order"):
        assembler.add(frames[1])


def test_body_assembler_refuses_chunks_of_another_body() -> None:
    frame = next(iter_body_frames(of="result", op="op-1", data=b"x"))
    assembler = BodyAssembler(of="call", op="op-1", limit=REQUEST_BODY_BYTES)
    with pytest.raises(FrameError, match="does not belong"):
        assembler.add(frame)


def test_body_assembler_enforces_the_byte_limit() -> None:
    frames = list(iter_body_frames(of="result", op="op-1", data=b"x" * (3 * MIB)))
    assembler = BodyAssembler(of="result", op="op-1", limit=2 * MIB)
    assembler.add(frames[0])
    assembler.add(frames[1])
    with pytest.raises(FrameTooLarge, match="exceeds the limit"):
        assembler.add(frames[2])


def test_body_assembler_rejects_chunks_after_the_last() -> None:
    frame = next(iter_body_frames(of="result", op="op-1", data=b"x"))
    assembler = BodyAssembler(of="result", op="op-1", limit=MIB)
    assert assembler.add(frame) is True
    with pytest.raises(FrameError, match="after the last chunk"):
        assembler.add(frame)


def test_body_assembler_body_before_completion_is_an_error() -> None:
    assembler = BodyAssembler(of="result", op="op-1", limit=MIB)
    with pytest.raises(FrameError, match="not complete"):
        assembler.body()


def test_chunk_with_bad_base64_is_refused() -> None:
    frame = {"t": "chunk", "op": "op-1", "of": "result", "seq": 0, "last": True, "data": "@@@"}
    with pytest.raises(FrameError, match="base64"):
        validate_frame(frame)


def test_chunk_with_bad_fields_is_refused() -> None:
    base = {"t": "chunk", "op": "op-1", "of": "result", "seq": 0, "last": True, "data": ""}
    with pytest.raises(FrameError, match="'of'"):
        validate_frame({**base, "of": "progress"})
    with pytest.raises(FrameError, match="'seq'"):
        validate_frame({**base, "seq": -1})
    with pytest.raises(FrameError, match="'last'"):
        validate_frame({**base, "last": "yes"})
    with pytest.raises(FrameError, match="'data'"):
        validate_frame({**base, "data": 5})


def test_decoded_chunk_larger_than_the_chunk_limit_is_refused() -> None:
    small = Limits(chunk_bytes=8)
    data = base64.b64encode(b"x" * 9).decode("ascii")
    frame = {"t": "chunk", "op": "op-1", "of": "result", "seq": 0, "last": True, "data": data}
    with pytest.raises(FrameTooLarge):
        validate_frame(frame, limits=small)


def test_limits_result_limit_follows_the_floor_factor_and_cap() -> None:
    limits = DEFAULT_LIMITS
    assert limits.result_limit(0) == MIB
    assert limits.result_limit(100_000) == MIB
    assert limits.result_limit(MIB) == 4 * MIB
    assert limits.result_limit(10 * MIB) == RESULT_BODY_BYTES


def test_limits_round_trip_through_a_dict() -> None:
    assert Limits.from_dict(DEFAULT_LIMITS.to_dict()) == DEFAULT_LIMITS


def test_limits_from_dict_rejects_missing_unknown_and_non_integer_fields() -> None:
    data = DEFAULT_LIMITS.to_dict()
    missing = dict(data)
    del missing["chunk_bytes"]
    with pytest.raises(ValueError, match="missing"):
        Limits.from_dict(missing)
    with pytest.raises(ValueError, match="unknown"):
        Limits.from_dict({**data, "extra": 1})
    with pytest.raises(ValueError, match="integer"):
        Limits.from_dict({**data, "chunk_bytes": True})
    with pytest.raises(ValueError, match="integer"):
        Limits.from_dict({**data, "chunk_bytes": "1"})
    with pytest.raises(ValueError, match="JSON object"):
        Limits.from_dict([])  # type: ignore[arg-type]


def test_limits_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        Limits(chunk_bytes=0)


def test_limits_within_compares_every_field() -> None:
    assert DEFAULT_LIMITS.within(DEFAULT_LIMITS)
    assert Limits(chunk_bytes=1024).within(DEFAULT_LIMITS)
    assert not Limits(chunk_bytes=2 * MIB).within(DEFAULT_LIMITS)


def test_reader_uses_the_limits_it_is_given() -> None:
    small = Limits(control_frame_bytes=64)
    frame = {"t": "heartbeat", "op": "op-1"}
    data = encode_frame(frame, limits=small)
    assert reader_for(data, limits=small).read_frame() == frame
    big = {"t": "progress", "op": "op-1", "done": 0, "total": 1, "message": "x" * 100}
    with pytest.raises(FrameTooLarge):
        encode_frame(big, limits=small)
