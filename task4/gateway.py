"""Token-aware completion router with a single primary-to-secondary attempt."""

from __future__ import annotations

import asyncio
import hmac
import logging
import math
import os
import sqlite3
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Literal

import httpx
import tiktoken
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, ValidationError

from task4.limiter import Decision, TokenLedger

ENCODING = tiktoken.get_encoding("cl100k_base")
LOGGER = logging.getLogger(__name__)
TENANTS = {"demo-tenant-a-key": "tenant-a", "demo-tenant-b-key": "tenant-b"}


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["system", "user", "assistant"]
    content: StrictStr = Field(min_length=1)


class Completion(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model: Literal["mock-llm"]
    messages: list[Message] = Field(min_length=1, max_length=100)
    max_tokens: StrictInt = Field(gt=0, le=4096)
    stream: Literal[False]
    primary_scenario: Literal["success", "rate_limit", "timeout", "error", "invalid"] = "success"
    secondary_scenario: Literal["success", "rate_limit", "timeout", "error", "invalid"] = "success"


class Usage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    prompt_tokens: StrictInt = Field(ge=0)
    completion_tokens: StrictInt = Field(ge=0)
    total_tokens: StrictInt = Field(ge=0)


class ProviderChoice(BaseModel):
    model_config = ConfigDict(extra="forbid")
    message: Message


class ProviderResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: StrictStr
    model: StrictStr
    choices: list[ProviderChoice] = Field(min_length=1, max_length=1)
    usage: Usage


def input_tokens(body: Completion) -> int:
    # cl100k_base plus three tokens per chat message and three for reply priming.
    return sum(len(ENCODING.encode(m.content)) + 3 for m in body.messages) + 3


def error(code: str, message: str, request_id: str, status: int, retry_after: int | None = None) -> JSONResponse:
    headers = {"Retry-After": str(retry_after)} if retry_after is not None else None
    return JSONResponse({"error": {"code": code, "message": message, "request_id": request_id}}, status_code=status, headers=headers)


def create_app(
    *,
    db_path: str | Path | None = None,
    quota: int | None = None,
    primary_url: str = "http://127.0.0.1:8005/v1/chat/completions",
    secondary_url: str = "http://127.0.0.1:8006/v1/chat/completions",
    primary_timeout: float = 3.0,
    secondary_timeout: float = 3.0,
    max_wait: float | None = None,
    clock: Callable[[], float] = time.time,
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    primary_transport: httpx.AsyncBaseTransport | None = None,
    secondary_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    if primary_timeout <= 0 or secondary_timeout <= 0:
        raise ValueError("timeouts must be positive")
    if max_wait is None:
        max_wait = float(os.getenv("TASK4_MAX_WAIT_SECONDS", "0"))
    if max_wait < 0:
        raise ValueError("max_wait must be nonnegative")
    ledger = TokenLedger(db_path or os.getenv("TASK4_DB_PATH", ".data/task4.sqlite3"),
                         quota if quota is not None else int(os.getenv("TASK4_QUOTA", "50000")), clock)
    app = FastAPI(title="Task 4 token-aware model router")
    app.state.ledger = ledger
    app.state.primary_timeout = primary_timeout

    async def admit(tenant: str, tokens: int) -> Decision:
        started = clock()
        for attempt in range(5):
            decision = await ledger.reserve(tenant, tokens)
            if decision.reservation_id:
                return decision
            if decision.eligible_at is None or attempt == 4:
                return decision
            delay = max(0, decision.eligible_at - clock())
            if delay > max_wait - (clock() - started):
                return decision
            # SQLite transaction is already committed and connection closed.
            if delay > 0:
                await sleeper(delay)
        raise AssertionError("unreachable")

    @app.post("/v1/chat/completions")
    async def complete(request: Request):
        request_id = uuid.uuid4().hex
        auth = request.headers.get("authorization", "")
        tenant = next((name for key, name in TENANTS.items() if hmac.compare_digest(auth, f"Bearer {key}")), None)
        if tenant is None:
            return error("unauthorized", "Valid Bearer credential required", request_id, 401)
        try:
            body = Completion.model_validate(await request.json())
        except (ValueError, ValidationError):
            return error("invalid_request", "Invalid completion request", request_id, 422)
        prompt = input_tokens(body)
        reserved = prompt + body.max_tokens
        try:
            decision = await admit(tenant, reserved)
        except sqlite3.Error:
            return error("gateway_unavailable", "Gateway temporarily unavailable", request_id, 503)
        if decision.reservation_id is None:
            retry = max(1, math.ceil((decision.eligible_at or clock() + 60) - clock()))
            LOGGER.info("request_id=%s tenant=%s outcome=quota_rejected", request_id, tenant)
            return error("rate_limit_exceeded", "Tenant token quota exceeded", request_id, 429, retry)

        payload = {"model": body.model, "messages": [m.model_dump() for m in body.messages], "max_tokens": body.max_tokens}

        async def call(url: str, scenario: str, timeout: float, transport: httpx.AsyncBaseTransport | None):
            async with httpx.AsyncClient(transport=transport, timeout=None, trust_env=False) as client:
                async with asyncio.timeout(timeout):
                    return await client.post(url, json={**payload, "scenario": scenario})

        provider = "primary"
        try:
            try:
                response = await call(primary_url, body.primary_scenario, primary_timeout, primary_transport)
            except TimeoutError:
                provider = "secondary"
                LOGGER.info("request_id=%s tenant=%s event=primary_timeout", request_id, tenant)
            except httpx.RequestError:
                return error("primary_provider_error", "Primary provider unavailable", request_id, 502)
            else:
                if response.status_code == 429:
                    provider = "secondary"
                    LOGGER.info("request_id=%s tenant=%s event=primary_429", request_id, tenant)
                elif response.status_code != 200:
                    return error("primary_provider_error", "Primary provider failed", request_id, 502)
            if provider == "secondary":
                try:
                    response = await call(secondary_url, body.secondary_scenario, secondary_timeout, secondary_transport)
                except TimeoutError:
                    return error("providers_unavailable", "Providers timed out", request_id, 504)
                except httpx.RequestError:
                    return error("providers_unavailable", "Backup provider unavailable", request_id, 502)
                if response.status_code != 200:
                    return error("providers_unavailable", "Backup provider failed", request_id, 502)
            try:
                result = ProviderResult.model_validate(response.json())
                usage = result.usage
                if (result.model != body.model or result.choices[0].message.role != "assistant"
                        or usage.prompt_tokens != prompt or usage.total_tokens != usage.prompt_tokens + usage.completion_tokens
                        or len(ENCODING.encode(result.choices[0].message.content)) != usage.completion_tokens
                        or usage.completion_tokens > body.max_tokens or usage.total_tokens > reserved):
                    raise ValueError("untrusted usage")
            except (ValueError, ValidationError):
                code = "primary_provider_error" if provider == "primary" else "providers_unavailable"
                return error(code, "Invalid provider response", request_id, 502)
            try:
                await ledger.settle(decision.reservation_id, usage.total_tokens)
            except sqlite3.Error:
                return error("gateway_unavailable", "Gateway temporarily unavailable", request_id, 503)
            LOGGER.info("request_id=%s tenant=%s provider=%s outcome=success", request_id, tenant, provider)
            return {"id": request_id, "model": body.model, "provider": provider,
                    "choices": [{"message": result.choices[0].message.model_dump()}], "usage": usage.model_dump()}
        except asyncio.CancelledError:
            # Retain the full reservation: provider work may have occurred.
            raise

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("task4.gateway:app", host="127.0.0.1", port=8004)
