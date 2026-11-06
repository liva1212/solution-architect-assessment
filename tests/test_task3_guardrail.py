"""Streaming redaction and HTTP gateway integration tests."""

from __future__ import annotations

import asyncio
import json
import socket
import time

import httpx
import pytest
import uvicorn

from task3.gateway import create_app as create_gateway
from task3.provider import create_app as create_provider
from task3.redactor import MAX_PENDING, Redactor
from task3.sse import MAX_FRAME_BYTES, StreamFormatError, data_events


REQUEST = {"model": "mock-llm", "messages": [{"role": "user", "content": "Give a sample"}], "stream": True}


def visible(body: bytes) -> tuple[str, list[str]]:
    frames = body.decode("utf-8").split("\n\n")
    assert frames[-1] == ""
    deltas = []
    for frame in frames[:-1]:
        if frame.startswith("event: error\n"):
            continue
        assert frame.startswith("data: ")
        data = frame[6:]
        if data == "[DONE]":
            continue
        payload = json.loads(data)
        assert set(payload) == {"choices"}
        deltas.append(payload["choices"][0]["delta"]["content"])
    return "".join(deltas), frames[:-1]


async def call(scenario: str, **options):
    provider = create_provider()
    gateway = create_gateway(transport=httpx.ASGITransport(app=provider))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
        response = await client.post("/v1/chat/completions", json={**REQUEST, "scenario": scenario, **options})
    return response, gateway.state.last_metrics


@pytest.mark.parametrize("scenario,expected", [
    ("plain", "Hello from the fictional provider. "),
    ("email", "Contact [REDACTED] for help. "),
    ("email_split", "Please contact [REDACTED] for assistance. "),
    ("ssn", "Example SSN [REDACTED] is private. "),
    ("ssn_split", "Example SSN [REDACTED] is private. "),
    ("card", "Test card [REDACTED] is private. "),
    ("card_split", "Test card [REDACTED] is private. "),
    ("multiple", "Email [REDACTED], SSN [REDACTED], card [REDACTED]. "),
])
def test_scenarios(scenario, expected):
    response, metrics = asyncio.run(call(scenario))
    assert response.status_code == 200
    text, frames = visible(response.content)
    assert text == expected
    assert frames[-1] == "data: [DONE]"
    assert "jane@example.com" not in response.text
    assert "123-45-6789" not in response.text
    assert "4111 1111 1111 1111" not in response.text
    assert metrics["max_pending"] <= MAX_PENDING


@pytest.mark.parametrize("size", [1, 2, 7, 64])
def test_chunk_sizes_have_same_result(size):
    response, _ = asyncio.run(call("multiple", chunk_size=size))
    assert visible(response.content)[0] == "Email [REDACTED], SSN [REDACTED], card [REDACTED]. "


def test_no_partial_email_is_released():
    redactor = Redactor()
    first = redactor.feed("Please contact jane")
    assert first == "Please contact "
    assert redactor.feed("@example") == ""
    assert redactor.feed(".com for assistance") == "[REDACTED] for "
    assert redactor.finish() == "assistance"


def test_long_candidate_fails_closed_and_bounds_state():
    redactor = Redactor()
    output = redactor.feed("a" * 400 + "@example.com ") + redactor.finish()
    assert output == "[REDACTED] "
    assert redactor.max_pending_seen <= MAX_PENDING


@pytest.mark.parametrize("sensitive", ["jane@example.com", "123-45-6789", "4111 1111 1111 1111"])
def test_sensitive_value_at_end_of_stream(sensitive):
    redactor = Redactor()
    output = "".join(redactor.feed(character) for character in "Value " + sensitive)
    assert sensitive not in output
    assert output + redactor.finish() == "Value [REDACTED]"


def test_large_stream_is_incremental_and_bounded():
    response, metrics = asyncio.run(call("long"))
    text, frames = visible(response.content)
    assert text == "safe " * 5000
    assert len(frames) > 100
    assert metrics["chunks"] == 5000
    assert metrics["max_pending"] <= MAX_PENDING


def test_independent_stream_state():
    async def run():
        return await asyncio.gather(call("email_split"), call("plain"))
    (first, _), (second, _) = asyncio.run(run())
    assert visible(first.content)[0] == "Please contact [REDACTED] for assistance. "
    assert visible(second.content)[0] == "Hello from the fictional provider. "


def test_sse_parser_handles_network_partitions_and_utf8():
    async def chunks():
        raw = b'data: {"choices":[{"delta":{"content":"caf\xc3\xa9"}}]}\n\ndata: [DONE]\n\n'
        for byte in raw:
            yield bytes([byte])
    async def run():
        return [item async for item in data_events(chunks())]
    assert asyncio.run(run()) == ['{"choices":[{"delta":{"content":"café"}}]}', "[DONE]"]


def test_sse_parser_handles_crlf_and_multiple_frames_per_read():
    async def chunks():
        yield b'data: one\r'
        yield b'\n\r\ndata: two\r\n\r\n'
    async def run():
        return [item async for item in data_events(chunks())]
    assert asyncio.run(run()) == ["one", "two"]


def test_oversized_sse_frame_fails_closed():
    async def chunks():
        yield b"data: " + b"x" * MAX_FRAME_BYTES + b"\n\n"
    async def run():
        return [item async for item in data_events(chunks())]
    with pytest.raises(StreamFormatError):
        asyncio.run(run())


def test_invalid_requests():
    async def run(payload):
        gateway = create_gateway(transport=httpx.ASGITransport(app=create_provider()))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
            return await client.post("/v1/chat/completions", json=payload)
    assert asyncio.run(run({**REQUEST, "stream": False})).status_code == 400
    assert asyncio.run(run({"stream": True})).status_code == 422


@pytest.mark.parametrize("exception,status", [(httpx.ConnectError("offline"), 502), (httpx.ReadTimeout("slow"), 504)])
def test_pre_stream_upstream_failures(exception, status):
    def fail(request):
        raise exception
    async def run():
        gateway = create_gateway(transport=httpx.MockTransport(fail))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
            return await client.post("/v1/chat/completions", json=REQUEST)
    response = asyncio.run(run())
    assert response.status_code == status
    assert "offline" not in response.text and "slow" not in response.text


@pytest.mark.parametrize("body", [
    b'data: not-json\n\n',
    b'data: {"choices":[]}\n\n',
    b'data: {"choices":[{"delta":{"content":3}}]}\n\n',
    b'data: {"choices":[{"delta":{"content":"secret"}}]}\n',
])
def test_malformed_or_interrupted_upstream_fails_safely(body):
    async def run():
        gateway = create_gateway(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=httpx.ByteStream(body))))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
            return await client.post("/v1/chat/completions", json=REQUEST)
    response = asyncio.run(run())
    assert "event: error" in response.text
    assert "secret" not in response.text
    assert "data: [DONE]" not in response.text


def test_provider_interruption_emits_safe_error():
    response, _ = asyncio.run(call("interrupted"))
    assert "event: error" in response.text
    assert "data: [DONE]" not in response.text


def test_non_content_metadata_is_never_forwarded():
    body = b'data: {"choices":[{"delta":{},"metadata":"jane@example.com"}]}\n\ndata: [DONE]\n\n'
    async def run():
        gateway = create_gateway(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=httpx.ByteStream(body))))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
            return await client.post("/v1/chat/completions", json=REQUEST)
    response = asyncio.run(run())
    assert response.text == "data: [DONE]\n\n"


def test_real_http_stream_delivers_before_done():
    async def run():
        sockets = []
        for _ in range(2):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sock.listen()
            sockets.append(sock)
        provider_port, gateway_port = (sock.getsockname()[1] for sock in sockets)
        provider = create_provider()
        gateway = create_gateway(f"http://127.0.0.1:{provider_port}/v1/chat/completions")
        servers = [uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off")) for app in (provider, gateway)]
        tasks = [asyncio.create_task(server.serve(sockets=[sock])) for server, sock in zip(servers, sockets)]
        try:
            for _ in range(100):
                if all(server.started for server in servers):
                    break
                await asyncio.sleep(0.01)
            assert all(server.started for server in servers)
            start = time.perf_counter()
            first = None
            body = bytearray()
            async with httpx.AsyncClient(timeout=10) as client:
                async with client.stream("POST", f"http://127.0.0.1:{gateway_port}/v1/chat/completions", json={**REQUEST, "scenario": "delayed", "delay_ms": 150}) as response:
                    assert response.status_code == 200
                    async for chunk in response.aiter_bytes():
                        if first is None and b'"content"' in chunk:
                            first = time.perf_counter() - start
                        body.extend(chunk)
            total = time.perf_counter() - start
            assert first is not None and first < total - 0.25
            assert visible(bytes(body))[0] == "First safe words. More safe words. Final safe words. "
            assert gateway.state.last_metrics["max_pending"] <= MAX_PENDING
            async with httpx.AsyncClient(timeout=10) as client:
                async with client.stream("POST", f"http://127.0.0.1:{gateway_port}/v1/chat/completions", json={**REQUEST, "scenario": "email_split", "delay_ms": 5}) as response:
                    chunks = [chunk async for chunk in response.aiter_bytes()]
            assert all(b"jane" not in chunk and b"example.com" not in chunk for chunk in chunks)
            assert visible(b"".join(chunks))[0] == "Please contact [REDACTED] for assistance. "
        finally:
            for server in servers:
                server.should_exit = True
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
            for sock in sockets:
                sock.close()
    asyncio.run(run())
