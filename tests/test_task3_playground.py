"""Custom-text playground integration through the real Task 3 gateway path."""

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


REQUEST = {"model": "mock-llm", "messages": [{"role": "user", "content": "Synthetic test"}], "stream": True}


def text_from_sse(body: bytes) -> str:
    frames = body.decode("utf-8").split("\n\n")
    assert frames[-2] == "data: [DONE]"
    return "".join(
        json.loads(frame[6:])["choices"][0]["delta"]["content"]
        for frame in frames[:-2] if frame.startswith("data: ")
    )


async def post_gateway(payload: dict):
    gateway = create_gateway(transport=httpx.ASGITransport(app=create_provider()), enable_playground=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
        return await client.post("/v1/chat/completions", json=payload)


def test_custom_text_uses_existing_streaming_redactor():
    original = "Contact jane@example.com; SSN 123-45-6789; card 4111 1111 1111 1111."
    response = asyncio.run(post_gateway({**REQUEST, "scenario": "custom", "test_text": original, "chunk_size": 1}))
    assert response.status_code == 200
    assert text_from_sse(response.content) == "Contact [REDACTED]; SSN [REDACTED]; card [REDACTED]."
    for canary in ("jane@example.com", "123-45-6789", "4111 1111 1111 1111"):
        assert canary not in response.text


def test_safe_custom_text_and_predefined_scenario_still_work():
    safe = asyncio.run(post_gateway({**REQUEST, "scenario": "custom", "test_text": "Just fictional safe text.", "chunk_size": 2}))
    plain = asyncio.run(post_gateway({**REQUEST, "scenario": "plain"}))
    assert text_from_sse(safe.content) == "Just fictional safe text."
    assert text_from_sse(plain.content) == "Hello from the fictional provider. "


@pytest.mark.parametrize("changes", [
    {"scenario": "custom"},
    {"scenario": "custom", "test_text": ""},
    {"scenario": "custom", "test_text": "x" * 4097},
    {"scenario": "custom", "test_text": 42},
    {"scenario": "custom", "test_text": "safe", "chunk_size": 0},
    {"scenario": "custom", "test_text": "safe", "delay_ms": -1},
    {"scenario": "plain", "test_text": "unexpected"},
    {"scenario": "custom", "test_text": "safe", "unexpected": "field"},
])
def test_invalid_custom_requests_are_rejected_before_upstream(changes):
    async def run():
        calls = []
        transport = httpx.MockTransport(lambda request: (calls.append(request), httpx.Response(200))[1])
        gateway = create_gateway(transport=transport, enable_playground=False)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", json={**REQUEST, **changes})
        return response, calls
    response, calls = asyncio.run(run())
    assert response.status_code == 422
    assert calls == []


def test_provider_enforces_text_limit_independently():
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_provider()), base_url="http://provider") as client:
            return await client.post("/v1/chat/completions", json={**REQUEST, "scenario": "custom", "test_text": "x" * 4097})
    assert asyncio.run(run()).status_code == 422


def test_playground_route_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("TASK3_ENABLE_PLAYGROUND", raising=False)
    async def run():
        app = create_gateway(transport=httpx.ASGITransport(app=create_provider()))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            return await client.get("/playground")
    assert asyncio.run(run()).status_code == 404


def test_playground_route_requires_explicit_enable(monkeypatch):
    monkeypatch.setenv("TASK3_ENABLE_PLAYGROUND", "1")
    async def run():
        app = create_gateway(transport=httpx.ASGITransport(app=create_provider()))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            return await client.get("/playground")
    response = asyncio.run(run())
    assert response.status_code == 200
    assert "no-store" in response.headers["cache-control"]
    assert 'fetch("/v1/chat/completions"' in response.text
    assert "getReader()" in response.text
    assert "localStorage" not in response.text and "sessionStorage" not in response.text


def test_custom_text_streams_incrementally_over_real_http():
    async def run():
        sockets = []
        for _ in range(2):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sock.listen()
            sockets.append(sock)
        provider_port, gateway_port = (sock.getsockname()[1] for sock in sockets)
        provider = create_provider()
        gateway = create_gateway(f"http://127.0.0.1:{provider_port}/v1/chat/completions", enable_playground=True)
        servers = [uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off")) for app in (provider, gateway)]
        tasks = [asyncio.create_task(server.serve(sockets=[sock])) for server, sock in zip(servers, sockets)]
        try:
            for _ in range(100):
                if all(server.started for server in servers):
                    break
                await asyncio.sleep(0.01)
            assert all(server.started for server in servers)
            async with httpx.AsyncClient(timeout=10) as client:
                page = await client.get(f"http://127.0.0.1:{gateway_port}/playground")
                assert page.status_code == 200
                started = time.perf_counter()
                first = None
                body = bytearray()
                async with client.stream("POST", f"http://127.0.0.1:{gateway_port}/v1/chat/completions", json={**REQUEST, "scenario": "custom", "test_text": "First safe words. Then jane@example.com is hidden.", "chunk_size": 6, "delay_ms": 80}) as response:
                    assert response.status_code == 200
                    async for chunk in response.aiter_bytes():
                        if first is None and b'"content"' in chunk:
                            first = time.perf_counter() - started
                        body.extend(chunk)
                total = time.perf_counter() - started
            assert first is not None and first < total - 0.20
            assert text_from_sse(bytes(body)) == "First safe words. Then [REDACTED] is hidden."
            assert b"jane@example.com" not in body
        finally:
            for server in servers:
                server.should_exit = True
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
            for sock in sockets:
                sock.close()
    asyncio.run(run())
