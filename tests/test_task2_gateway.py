"""Gateway and downstream integration tests, including a real HTTP smoke test."""

from __future__ import annotations

import asyncio
import json
import socket

import httpx
import pytest
import uvicorn

from task2.downstream import create_app as create_downstream
from task2.gateway import create_app as create_gateway


class RecordingTransport(httpx.AsyncBaseTransport):
    def __init__(self, downstream):
        self.inner = httpx.ASGITransport(app=downstream)
        self.requests: list[httpx.Request] = []
        self.bodies: list[bytes] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.bodies.append(await request.aread())
        return await self.inner.handle_async_request(request)


def rpc(method="tools/list", params=None, request_id=1):
    message = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    return message


async def exchange(message, token="demo-viewer-token", *, raw=None, extra_headers=None):
    downstream = create_downstream()
    transport = RecordingTransport(downstream)
    gateway = create_gateway(transport=transport)
    headers = {"Authorization": f"Bearer {token}"} if token is not None else {}
    headers.update(extra_headers or {})
    body = raw if raw is not None else json.dumps(message).encode()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
        response = await client.post("/mcp", content=body, headers=headers)
    return response, downstream.state.received, transport


@pytest.mark.parametrize("token", ["demo-viewer-token", "demo-admin-token"])
def test_tool_listing_is_transparent(token):
    message = rpc(request_id="list-1")
    response, received, transport = asyncio.run(exchange(message, token))
    assert response.status_code == 200
    assert response.json()["id"] == "list-1"
    assert {tool["name"] for tool in response.json()["result"]["tools"]} >= {"get_customer_record", "admin_reset_key"}
    assert received == [message]
    assert transport.bodies == [json.dumps(message).encode()]
    assert "authorization" not in transport.requests[0].headers


@pytest.mark.parametrize("token,name", [
    ("demo-viewer-token", "get_customer_record"),
    ("demo-admin-token", "get_customer_record"),
    ("demo-admin-token", "admin_reset_key"),
    ("demo-viewer-token", "administration_report"),
])
def test_authorized_calls_reach_downstream_once(token, name):
    message = rpc("tools/call", {"name": name}, request_id=0)
    response, received, transport = asyncio.run(exchange(message, token))
    assert response.status_code == 200 and response.json()["id"] == 0
    assert "result" in response.json()
    assert received == [message]
    assert len(transport.requests) == 1


@pytest.mark.parametrize("name", ["admin_reset_key", "admin_delete_customer"])
def test_viewer_admin_prefix_is_blocked_without_forwarding(name):
    message = rpc("tools/call", {"name": name}, request_id="blocked")
    response, received, transport = asyncio.run(exchange(message))
    assert response.status_code == 200
    assert response.json() == {"jsonrpc": "2.0", "id": "blocked", "error": {"code": -32001, "message": "Unauthorized Tool Call"}}
    assert received == [] and transport.requests == []


@pytest.mark.parametrize("headers", [
    {}, {"Authorization": "Bearer invalid"}, {"Authorization": "Bearer"},
    {"Authorization": "Basic demo-viewer-token"}, {"Authorization": "Bearer  demo-admin-token"},
])
def test_bad_auth_never_forwards(headers):
    response, received, transport = asyncio.run(exchange(rpc(), token=None, extra_headers=headers))
    assert response.status_code == 401
    assert "token" in response.json()["detail"]
    assert received == [] and transport.requests == []


def test_role_spoofing_does_not_authorize():
    message = rpc("tools/call", {"name": "admin_reset_key", "role": "admin"})
    message["role"] = "admin"
    response, received, transport = asyncio.run(exchange(message, extra_headers={"X-Role": "admin"}))
    assert response.json()["error"]["code"] == -32001
    assert received == [] and transport.requests == []


@pytest.mark.parametrize("raw,code", [
    (b"{bad", -32700),
    (b"[]", -32600),
    (b'{"jsonrpc":"1.0","id":4,"method":"tools/list"}', -32600),
    (b'{"jsonrpc":"2.0","id":4}', -32600),
    (b'{"jsonrpc":"2.0","id":4,"method":"tools/call"}', -32602),
    (b'{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":4}}', -32602),
])
def test_protocol_errors_do_not_forward(raw, code):
    response, received, transport = asyncio.run(exchange(None, raw=raw))
    assert response.status_code == 200
    assert response.json()["error"]["code"] == code
    assert received == [] and transport.requests == []


def test_unknown_method_is_forwarded():
    message = rpc("unknown/method", request_id="custom")
    response, received, transport = asyncio.run(exchange(message))
    assert response.json()["id"] == "custom"
    assert response.json()["error"]["code"] == -32601
    assert received == [message] and len(transport.requests) == 1


@pytest.mark.parametrize("exception,status", [(httpx.ReadTimeout("slow"), 504), (httpx.ConnectError("offline"), 502)])
def test_upstream_failure_is_safe(exception, status):
    async def run():
        def fail(request):
            raise exception
        gateway = create_gateway(transport=httpx.MockTransport(fail))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway), base_url="http://gateway") as client:
            return await client.post("/mcp", json=rpc(request_id="failed"), headers={"Authorization": "Bearer demo-viewer-token"})
    response = asyncio.run(run())
    assert response.status_code == status
    assert response.json() == {"jsonrpc": "2.0", "id": "failed", "error": {"code": -32002, "message": "Downstream unavailable"}}


def test_real_http_smoke():
    async def run():
        sockets = []
        for _ in range(2):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sock.listen()
            sockets.append(sock)
        downstream_port, gateway_port = (sock.getsockname()[1] for sock in sockets)
        downstream = create_downstream()
        gateway = create_gateway(f"http://127.0.0.1:{downstream_port}/mcp")
        servers = [uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off")) for app in (downstream, gateway)]
        tasks = [asyncio.create_task(server.serve(sockets=[sock])) for server, sock in zip(servers, sockets)]
        try:
            for _ in range(100):
                if all(server.started for server in servers):
                    break
                await asyncio.sleep(0.01)
            assert all(server.started for server in servers)
            async with httpx.AsyncClient(timeout=5) as client:
                allowed = await client.post(f"http://127.0.0.1:{gateway_port}/mcp", json=rpc(), headers={"Authorization": "Bearer demo-viewer-token"})
                blocked = await client.post(f"http://127.0.0.1:{gateway_port}/mcp", json=rpc("tools/call", {"name": "admin_reset_key"}), headers={"Authorization": "Bearer demo-viewer-token"})
            assert allowed.json()["result"]["tools"]
            assert blocked.json()["error"]["code"] == -32001
            assert len(downstream.state.received) == 1
        finally:
            for server in servers:
                server.should_exit = True
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
            for sock in sockets:
                sock.close()
    asyncio.run(run())
