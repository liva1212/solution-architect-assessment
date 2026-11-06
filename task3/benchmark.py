"""Measure representative local streaming timing without logging response text."""

from __future__ import annotations

import asyncio
import socket
import time

import httpx
import uvicorn

from task3.gateway import create_app as create_gateway
from task3.provider import create_app as create_provider

REQUEST = {
    "model": "mock-llm", "messages": [{"role": "user", "content": "Sample"}],
    "stream": True, "scenario": "delayed", "delay_ms": 150,
}


async def measure(url: str) -> tuple[float, float]:
    start = time.perf_counter()
    first = None
    async with httpx.AsyncClient(timeout=10) as client:
        async with client.stream("POST", url, json=REQUEST) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                if first is None and b'"content"' in chunk:
                    first = time.perf_counter() - start
    assert first is not None
    return first, time.perf_counter() - start


async def main() -> None:
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
        direct_first, direct_total = await measure(f"http://127.0.0.1:{provider_port}/v1/chat/completions")
        guarded_first, guarded_total = await measure(f"http://127.0.0.1:{gateway_port}/v1/chat/completions")
        metrics = gateway.state.last_metrics
        print(f"provider first={direct_first:.3f}s total={direct_total:.3f}s")
        print(f"gateway first={guarded_first:.3f}s total={guarded_total:.3f}s")
        print(f"added first={guarded_first - direct_first:.3f}s total={guarded_total - direct_total:.3f}s")
        print(f"processed chunks={metrics['chunks']} max pending={metrics['max_pending']} chars")
    finally:
        for server in servers:
            server.should_exit = True
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
        for sock in sockets:
            sock.close()


if __name__ == "__main__":
    asyncio.run(main())
