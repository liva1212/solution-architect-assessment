"""Incremental UTF-8 and SSE frame parser with a finite frame limit."""

from __future__ import annotations

import codecs
from collections.abc import AsyncIterator

MAX_FRAME_BYTES = 16384


class StreamFormatError(Exception):
    """Unsafe or unsupported upstream event stream."""


async def data_events(chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    buffer = ""
    async for chunk in chunks:
        # Slice only after the network read. HTTPX's aiter_raw(chunk_size=...)
        # waits to fill its requested size and can destroy time-to-first-token.
        for offset in range(0, len(chunk), 4096):
            try:
                buffer += decoder.decode(chunk[offset:offset + 4096])
            except UnicodeDecodeError as exc:
                raise StreamFormatError("Invalid UTF-8") from exc
            # Keep a trailing CR until the next read so a split CRLF is one newline.
            trailing_cr = buffer.endswith("\r")
            body = buffer[:-1] if trailing_cr else buffer
            buffer = body.replace("\r\n", "\n").replace("\r", "\n") + ("\r" if trailing_cr else "")
            while "\n\n" in buffer:
                frame, buffer = buffer.split("\n\n", 1)
                if len(frame.encode("utf-8")) > MAX_FRAME_BYTES:
                    raise StreamFormatError("SSE frame too large")
                lines = []
                for line in frame.split("\n"):
                    if line.startswith("data:"):
                        value = line[5:]
                        lines.append(value[1:] if value.startswith(" ") else value)
                if lines:
                    yield "\n".join(lines)
            if len(buffer.encode("utf-8")) > MAX_FRAME_BYTES:
                raise StreamFormatError("SSE frame too large")
    try:
        buffer += decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise StreamFormatError("Invalid UTF-8") from exc
    if buffer.strip():
        raise StreamFormatError("Incomplete SSE frame")


def content_event(text: str) -> bytes:
    import json
    payload = {"choices": [{"delta": {"content": text}}]}
    return ("data: " + json.dumps(payload, ensure_ascii=False) + "\n\n").encode("utf-8")


DONE = b"data: [DONE]\n\n"
ERROR = b'event: error\ndata: {"error":"Upstream stream failed"}\n\n'
