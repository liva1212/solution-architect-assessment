"""Independently runnable fictional MCP-like HTTP JSON-RPC service."""

from __future__ import annotations

import logging
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from task2.protocol import error, parse

LOGGER = logging.getLogger(__name__)
TOOLS = [
    {"name": "get_customer_record", "description": "Get a fictional customer record", "inputSchema": {"type": "object"}},
    {"name": "admin_reset_key", "description": "Simulate an administrator key reset", "inputSchema": {"type": "object"}},
    {"name": "administration_report", "description": "Return a fictional report", "inputSchema": {"type": "object"}},
]


def create_app() -> FastAPI:
    app = FastAPI(title="Task 2 mock downstream MCP service")
    app.state.received: list[dict[str, Any]] = []

    @app.post("/mcp")
    async def mcp(request: Request) -> JSONResponse:
        value, problem = parse(await request.body())
        if problem:
            return JSONResponse(problem)
        assert value is not None
        app.state.received.append(value)
        method, request_id = value["method"], value["id"]
        # Log the method and tool name only. Never log authentication material.
        name = value.get("params", {}).get("name") if isinstance(value.get("params"), dict) else None
        LOGGER.info("Downstream received %s %s", method, name or "")
        if method == "tools/list":
            result: dict[str, Any] = {"tools": TOOLS}
        elif method == "tools/call":
            if name == "get_customer_record":
                result = {"content": [{"type": "text", "text": "Fictional customer Avery Example (avery@example.com)"}]}
            elif name == "admin_reset_key":
                result = {"content": [{"type": "text", "text": "Simulated key reset completed; no credential was created"}]}
            elif name == "administration_report":
                result = {"content": [{"type": "text", "text": "Fictional administration report"}]}
            else:
                return JSONResponse(error(-32602, "Unknown tool", request_id))
        else:
            return JSONResponse(error(-32601, "Method not found", request_id))
        return JSONResponse({"jsonrpc": "2.0", "id": request_id, "result": result})

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("task2.downstream:app", host="127.0.0.1", port=8001)
