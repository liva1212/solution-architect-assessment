"""HTTP JSON-RPC gateway with token-derived role enforcement."""

from __future__ import annotations

import logging

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from task2.protocol import error, parse

LOGGER = logging.getLogger(__name__)
TOKENS = {"demo-admin-token": "admin", "demo-viewer-token": "viewer"}


def create_app(
    downstream_url: str = "http://127.0.0.1:8001/mcp",
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    timeout: float = 5.0,
) -> FastAPI:
    app = FastAPI(title="Task 2 MCP security gateway")

    @app.post("/mcp")
    async def mcp(request: Request) -> Response:
        authorization = request.headers.get("authorization", "")
        scheme, separator, token = authorization.partition(" ")
        # Roles come only from the server-side registry. Ignore any client role claims.
        role = TOKENS.get(token) if separator and scheme.lower() == "bearer" and token and token == token.strip() else None
        if role is None:
            return JSONResponse({"detail": "Missing or invalid bearer token"}, status_code=401, headers={"WWW-Authenticate": "Bearer"})

        raw = await request.body()
        value, problem = parse(raw)
        if problem:
            return JSONResponse(problem)
        assert value is not None
        if value["method"] == "tools/call" and value["params"]["name"].startswith("admin_") and role != "admin":
            # Authorization happens before any upstream client or request is created.
            return JSONResponse(error(-32001, "Unauthorized Tool Call", value["id"]))

        try:
            async with httpx.AsyncClient(transport=transport, timeout=timeout) as client:
                upstream = await client.post(downstream_url, content=raw, headers={"Content-Type": "application/json"})
        except httpx.TimeoutException:
            LOGGER.warning("Downstream request timed out")
            return JSONResponse(error(-32002, "Downstream unavailable", value["id"]), status_code=504)
        except httpx.RequestError:
            LOGGER.warning("Downstream connection failed")
            return JSONResponse(error(-32002, "Downstream unavailable", value["id"]), status_code=502)

        # Preserve the downstream wire body and status; never forward client credentials.
        return Response(content=upstream.content, status_code=upstream.status_code, headers={"Content-Type": upstream.headers.get("content-type", "application/json")})

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("task2.gateway:app", host="127.0.0.1", port=8000)
