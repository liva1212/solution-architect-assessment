"""Adversarial Task 3 cases with independently specified expected outputs."""

from __future__ import annotations

import asyncio
import json
import random
import tracemalloc

import httpx
import pytest

from task3.gateway import create_app as create_gateway
from task3.provider import create_app as create_provider
from task3.redactor import MAX_PENDING, Redactor
from task3.sse import data_events


CASES = [
    ("jane@example.com", "[REDACTED]", ("jane", "@example", "example.com")),
    ("123-45-6789", "[REDACTED]", ("123-45", "45-6789")),
    ("4111 1111 1111 1111", "[REDACTED]", ("4111 1111", "1111 1111")),
]


def redacted_from_parts(parts: list[str], fragments: tuple[str, ...]) -> tuple[str, int]:
    guard = Redactor()
    emitted = []
    for part in parts:
        piece = guard.feed(part)
        assert all(fragment not in piece for fragment in fragments)
        emitted.append(piece)
    emitted.append(guard.finish())
    return "".join(emitted), guard.max_pending_seen


@pytest.mark.parametrize("sensitive,expected,fragments", CASES)
def test_every_single_split_and_one_character_chunks(sensitive, expected, fragments):
    text = "Before " + sensitive + " after"
    for boundary in range(1, len(text)):
        actual, maximum = redacted_from_parts([text[:boundary], text[boundary:]], fragments)
        assert actual == "Before " + expected + " after"
        assert maximum <= MAX_PENDING
    actual, _ = redacted_from_parts(list(text), fragments)
    assert actual == "Before " + expected + " after"


@pytest.mark.parametrize("sensitive,expected,fragments", CASES)
def test_hundreds_of_seeded_multi_chunk_partitions(sensitive, expected, fragments):
    text = "Start: " + sensitive + ", then " + sensitive + "!"
    rng = random.Random(20261106)
    for _ in range(300):
        pieces = []
        offset = 0
        while offset < len(text):
            size = rng.randint(1, 9)
            pieces.append(text[offset:offset + size])
            offset += size
        actual, maximum = redacted_from_parts(pieces, fragments)
        assert actual == "Start: " + expected + ", then " + expected + "!"
        assert maximum <= MAX_PENDING


@pytest.mark.parametrize("sensitive,expected,fragments", CASES)
def test_sensitive_at_start_and_end(sensitive, expected, fragments):
    for text, result in [(sensitive + " after", expected + " after"), ("Before " + sensitive, "Before " + expected)]:
        actual, _ = redacted_from_parts(list(text), fragments)
        assert actual == result


def test_candidate_limits_255_256_257_and_incomplete_termination():
    for length in (255, 256):
        guard = Redactor()
        assert guard.feed("a" * length) == ""
        assert guard.max_pending_seen == length
        assert guard.finish() == "a" * length
    guard = Redactor()
    assert guard.feed("a" * 257) == "[REDACTED]"
    assert guard.max_pending_seen == 256
    assert guard.finish() == ""


@pytest.mark.parametrize("partial", ["jane@exam", "123-45-", "4111 1111 1111"])
def test_incomplete_sensitive_candidates_at_termination_fail_closed(partial):
    guard = Redactor()
    assert guard.feed(partial) == ""
    assert guard.finish() == "[REDACTED]"


def test_long_repeated_pii_keeps_pending_state_bounded():
    guard = Redactor()
    tracemalloc.start()
    for _ in range(2000):
        emitted = guard.feed("jane@example.com 4111 1111 1111 1111 ")
        assert "jane@example.com" not in emitted
        assert "4111 1111 1111 1111" not in emitted
    guard.finish()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert guard.chunks == 2000
    assert guard.max_pending_seen <= MAX_PENDING
    assert peak < 5_000_000


def test_alternative_formats_are_explicitly_outside_scope():
    samples = ["123456789", "123 45 6789", "4111.1111.1111.1111", "josé@example.com", "jane [at] example.com"]
    for value in samples:
        guard = Redactor()
        actual = guard.feed(value) + guard.finish()
        assert actual == value


class PartitionedStream(httpx.AsyncByteStream):
    def __init__(self, raw: bytes, size: int):
        self.raw, self.size = raw, size

    async def __aiter__(self):
        for index in range(0, len(self.raw), self.size):
            yield self.raw[index:index + self.size]


async def _gateway_body(raw: bytes, size: int | None = None) -> bytes:
    stream = httpx.ByteStream(raw) if size is None else PartitionedStream(raw, size)
    gateway = create_gateway(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=stream)))
    request = {"model": "mock-llm", "messages": [{"role": "user", "content": "Sample"}], "stream": True}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
        response = await client.post("/v1/chat/completions", json=request)
    return response.content


def _sse(value: object) -> bytes:
    return ("data: " + json.dumps(value) + "\n\n").encode()


def _visible_text(body: bytes) -> str:
    return "".join(
        json.loads(frame[6:])["choices"][0]["delta"]["content"]
        for frame in body.decode().split("\n\n")
        if frame.startswith("data: ") and frame != "data: [DONE]"
    )


def test_multiple_choices_fail_closed_instead_of_sharing_redaction_state():
    raw = _sse({"choices": [{"delta": {"content": "Contact jane"}}, {"delta": {"content": "@example.com"}}]}) + b"data: [DONE]\n\n"
    body = asyncio.run(_gateway_body(raw))
    assert b"event: error" in body
    assert b"data: [DONE]" not in body
    assert b"jane" not in body and b"example.com" not in body


def test_unexpected_named_sse_event_fails_closed():
    raw = b"event: unexpected\n" + _sse({"choices": [{"delta": {"content": "safe"}}]}) + b"data: [DONE]\n\n"
    body = asyncio.run(_gateway_body(raw))
    assert b"event: error" in body
    assert b"data: [DONE]" not in body
    assert b"safe" not in body


@pytest.mark.parametrize("payload", [
    {"choices": [{"delta": {"content": "safe"}}], "unknown_output": "secret"},
    {"choices": [{"delta": {"content": "safe"}, "unknown_text": "secret"}]},
    {"choices": [{"delta": {"content": "safe", "tool_calls": [{"text": "secret"}]}}]},
])
def test_unknown_provider_schema_fails_closed(payload):
    body = asyncio.run(_gateway_body(_sse(payload) + b"data: [DONE]\n\n"))
    assert b"event: error" in body
    assert b"data: [DONE]" not in body
    assert b"secret" not in body


def test_client_visible_canary_is_never_emitted_in_any_sse_event():
    raw = b"".join(_sse({"choices": [{"delta": {"content": part}}]}) for part in ["Please contact jane", "@example", ".com now"]) + b"data: [DONE]\n\n"
    body = asyncio.run(_gateway_body(raw))
    for frame in body.split(b"\n\n"):
        assert b"jane" not in frame and b"@example" not in frame and b"example.com" not in frame
    assert b"[REDACTED]" in body


def test_gateway_handles_byte_partitioned_sse_and_utf8():
    raw = _sse({"choices": [{"delta": {"content": "café jane@example.com"}}]}) + b"data: [DONE]\n\n"
    body = asyncio.run(_gateway_body(raw, size=1))
    assert _visible_text(body) == "café [REDACTED]"
    assert b"jane@example.com" not in body
    assert body.endswith(b"data: [DONE]\n\n")


def test_cr_only_sse_boundaries_across_byte_chunks():
    async def chunks():
        for byte in b"data: first\r\rdata: [DONE]\r\r":
            yield bytes([byte])
    async def run():
        return [event async for event in data_events(chunks())]
    assert asyncio.run(run()) == ["first", "[DONE]"]


@pytest.mark.parametrize("raw", [
    b"data: " + b"x" * 16385 + b"\n\n",
    b'data: {"choices":[{"delta":{"content":"secret"}}]}\n',
    b'data: {"choices":[{"delta":{"content":42}}]}\n\ndata: [DONE]\n\n',
])
def test_gateway_rejects_bad_frame_without_forwarding_raw_content(raw):
    body = asyncio.run(_gateway_body(raw, size=1))
    assert b"event: error" in body
    assert b"secret" not in body and b"data: [DONE]" not in body


def test_incomplete_email_at_done_is_redacted_in_client_stream():
    raw = _sse({"choices": [{"delta": {"content": "jane@exam"}}]}) + b"data: [DONE]\n\n"
    body = asyncio.run(_gateway_body(raw))
    assert b"jane@exam" not in body
    assert b"[REDACTED]" in body and body.endswith(b"data: [DONE]\n\n")


def test_concurrent_streams_have_independent_redaction_state():
    async def run():
        gateway = create_gateway(transport=httpx.ASGITransport(app=create_provider()))
        scenarios = ["email_split", "ssn_split", "card_split", "plain"] * 4
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
            return await asyncio.gather(*(client.post("/v1/chat/completions", json={"model": "mock-llm", "messages": [{"role": "user", "content": "Sample"}], "stream": True, "scenario": scenario}) for scenario in scenarios))
    responses = asyncio.run(run())
    expected = ["Please contact [REDACTED] for assistance. ", "Example SSN [REDACTED] is private. ", "Test card [REDACTED] is private. ", "Hello from the fictional provider. "] * 4
    for response, marker in zip(responses, expected):
        assert _visible_text(response.content) == marker
        assert b"jane@example.com" not in response.content
        assert b"123-45-6789" not in response.content
        assert b"4111 1111 1111 1111" not in response.content
