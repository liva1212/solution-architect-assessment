"""Incremental UTF-8 and SSE frame parser with a finite frame limit."""

from __future__ import annotations

import codecs
from collections.abc import AsyncIterator

MAX_FRAME_BYTES = 16384


class StreamFormatError(Exception):
    """Unsafe or unsupported upstream event stream."""


async def data_events(chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    line = ""
    lines: list[str] = []
    frame_bytes = 0
    after_cr = False
    at_start = True

    def accept(character: str) -> str | None:
        nonlocal line, lines, frame_bytes, after_cr, at_start
        if at_start:
            at_start = False
            if character == "\ufeff":
                return None
        if after_cr:
            after_cr = False
            if character == "\n":
                return None
        if character not in "\r\n":
            line += character
            frame_bytes += len(character.encode("utf-8"))
            if frame_bytes > MAX_FRAME_BYTES:
                raise StreamFormatError("SSE frame too large")
            return None
        after_cr = character == "\r"
        frame_bytes += 1
        if frame_bytes > MAX_FRAME_BYTES:
            raise StreamFormatError("SSE frame too large")
        if line:
            lines.append(line)
            line = ""
            return None
        event_lines = lines
        lines = []
        frame_bytes = 0
        data = []
        for event_line in event_lines:
            if event_line.startswith("event:"):
                event_type = event_line[6:]
                event_type = event_type[1:] if event_type.startswith(" ") else event_type
                if event_type != "message":
                    raise StreamFormatError("Unsupported SSE event type")
            if event_line == "data":
                data.append("")
            elif event_line.startswith("data:"):
                value = event_line[5:]
                data.append(value[1:] if value.startswith(" ") else value)
        return "\n".join(data) if data else None

    async for chunk in chunks:
        # Slice only after the network read. HTTPX's aiter_raw(chunk_size=...)
        # waits to fill its requested size and can destroy time-to-first-token.
        for offset in range(0, len(chunk), 4096):
            try:
                decoded = decoder.decode(chunk[offset:offset + 4096])
            except UnicodeDecodeError as exc:
                raise StreamFormatError("Invalid UTF-8") from exc
            for character in decoded:
                event = accept(character)
                if event is not None:
                    yield event
    try:
        tail = decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise StreamFormatError("Invalid UTF-8") from exc
    for character in tail:
        event = accept(character)
        if event is not None:
            yield event
    if line or lines:
        raise StreamFormatError("Incomplete SSE frame")


def content_event(text: str) -> bytes:
    import json
    payload = {"choices": [{"delta": {"content": text}}]}
    return ("data: " + json.dumps(payload, ensure_ascii=False) + "\n\n").encode("utf-8")


DONE = b"data: [DONE]\n\n"
ERROR = b'event: error\ndata: {"error":"Upstream stream failed"}\n\n'
