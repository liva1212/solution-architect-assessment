"""Actual localhost gateway-to-provider requests and default deadline."""

import asyncio
import socket
import time

import httpx
import uvicorn

from task4.gateway import create_app
from task4.providers import create_app as provider_app
from test_task4_gateway import AUTH, REQUEST


def test_real_http_fallback_and_three_second_deadline(tmp_path):
    async def run():
        sockets = []
        for _ in range(3):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sock.listen()
            sockets.append(sock)
        gateway_port, primary_port, secondary_port = (s.getsockname()[1] for s in sockets)
        primary = provider_app("primary")
        secondary = provider_app("secondary")
        gateway = create_app(
            db_path=tmp_path / "real.sqlite3",
            primary_url=f"http://127.0.0.1:{primary_port}/v1/chat/completions",
            secondary_url=f"http://127.0.0.1:{secondary_port}/v1/chat/completions",
        )
        assert gateway.state.primary_timeout == 3.0
        apps = (gateway, primary, secondary)
        servers = [uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off")) for app in apps]
        tasks = [asyncio.create_task(server.serve(sockets=[sock])) for server, sock in zip(servers, sockets)]
        try:
            for _ in range(100):
                if all(server.started for server in servers):
                    break
                await asyncio.sleep(0.01)
            assert all(server.started for server in servers)
            async with httpx.AsyncClient(timeout=8) as client:
                url = f"http://127.0.0.1:{gateway_port}/v1/chat/completions"
                normal = await client.post(url, json=REQUEST, headers=AUTH)
                assert normal.status_code == 200 and normal.json()["provider"] == "primary"
                fallback = await client.post(url, json={**REQUEST, "primary_scenario": "rate_limit"}, headers=AUTH)
                assert fallback.status_code == 200 and fallback.json()["provider"] == "secondary"
                started = time.perf_counter()
                delayed = await client.post(url, json={**REQUEST, "primary_scenario": "timeout"}, headers=AUTH)
                elapsed = time.perf_counter() - started
                assert delayed.status_code == 200 and delayed.json()["provider"] == "secondary"
                assert 2.8 <= elapsed < 3.9
            assert primary.state.calls == 3 and secondary.state.calls == 2
        finally:
            for server in servers:
                server.should_exit = True
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
            for sock in sockets:
                sock.close()
    asyncio.run(run())
