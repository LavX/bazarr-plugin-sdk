"""Replays the shared transcripts in tests/transcripts through the frame codec.

The same JSON files are the test vectors for the host. Each frame must survive
an encode followed by a decode, each body must reassemble, and each refusal
must happen after exactly the bytes the transcript names.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest

from bazarr_plugin_sdk.wire import (
    BodyAssembler,
    FrameReader,
    FrameTooLarge,
    encode_frame,
)

TRANSCRIPT_DIR = Path(__file__).parent / "transcripts"


def load(name: str) -> dict[str, Any]:
    return json.loads((TRANSCRIPT_DIR / name).read_text(encoding="utf-8"))


def transcript_files() -> list[Path]:
    return sorted(TRANSCRIPT_DIR.glob("*.json"))


@pytest.mark.parametrize("path", transcript_files(), ids=lambda p: p.stem)
def test_transcript_frames_round_trip_through_the_codec(path: Path) -> None:
    transcript = json.loads(path.read_text(encoding="utf-8"))
    if transcript["kind"] != "frames":
        pytest.skip("refusal transcript")
    stream = b"".join(encode_frame(entry["frame"]) for entry in transcript["frames"])
    reader = FrameReader(io.BytesIO(stream).read)
    decoded = []
    for _ in transcript["frames"]:
        frame = reader.read_frame()
        assert frame is not None
        decoded.append(frame)
    assert reader.read_frame() is None
    assert decoded == [entry["frame"] for entry in transcript["frames"]]


@pytest.mark.parametrize("path", transcript_files(), ids=lambda p: p.stem)
def test_transcript_bodies_reassemble_to_their_bytes(path: Path) -> None:
    transcript = json.loads(path.read_text(encoding="utf-8"))
    if transcript["kind"] != "frames":
        pytest.skip("refusal transcript")
    assemblers: dict[tuple[Any, Any, Any], BodyAssembler] = {}
    bodies: list[bytes] = []
    for entry in transcript["frames"]:
        frame = entry["frame"]
        if frame["t"] == "call" and frame.get("body") is True:
            assemblers[(frame["op"], "call", None)] = BodyAssembler(
                of="call", op=frame["op"], limit=16 * 1024 * 1024
            )
        elif frame["t"] == "result" and frame.get("body") is True:
            assemblers[(frame["op"], "result", None)] = BodyAssembler(
                of="result", op=frame["op"], limit=16 * 1024 * 1024
            )
        elif frame["t"] == "checkpoint":
            assemblers[(frame["op"], "checkpoint", frame["req"])] = BodyAssembler(
                of="checkpoint", op=frame["op"], req=frame["req"], limit=8 * 1024 * 1024
            )
        elif frame["t"] == "chunk":
            assembler = assemblers[(frame["op"], frame["of"], frame.get("req"))]
            if assembler.add(frame):
                bodies.append(assembler.body())
    assert not any(not assembler.complete for assembler in assemblers.values())
    for body in bodies:
        assert body == b"hello"


def test_chunked_body_transcript_reassembles_hello() -> None:
    transcript = load("chunked_body.json")
    chunks = [entry["frame"] for entry in transcript["frames"] if entry["frame"]["t"] == "chunk"]
    host = [c for c in chunks if c["of"] == "call"]
    worker = [c for c in chunks if c["of"] == "result"]
    for group in (host, worker):
        assembler = BodyAssembler(of=group[0]["of"], op=group[0]["op"], limit=1024)
        for chunk in group:
            assembler.add(chunk)
        assert assembler.body() == b"hello"


def test_oversize_header_transcript_is_refused_after_four_bytes() -> None:
    transcript = load("oversize_header_refused.json")
    data = bytes.fromhex(transcript["header_hex"]) + b"\x00" * 2_000_000
    source = io.BytesIO(data)
    with pytest.raises(FrameTooLarge):
        FrameReader(source.read).read_frame()
    assert source.tell() == transcript["expect"]["bytes_consumed"]


def test_handshake_mismatch_transcript_names_the_exit_code() -> None:
    transcript = load("handshake_mismatch.json")
    assert transcript["expect"]["exit_code"] == 78
    assert transcript["expect"]["worker_frames"] == []
    assert transcript["frames"][0]["frame"]["kind_api"] != "sample.v1"
