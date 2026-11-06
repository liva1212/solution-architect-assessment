"""Streaming LLM proxy that emits only inspected, sanitized SSE content."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, StrictStr, ValidationError, model_validator

from task3.redactor import Redactor
from task3.sse import DONE, ERROR, StreamFormatError, content_event, data_events

LOGGER = logging.getLogger(__name__)


class Completion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: str
    messages: list[dict[str, str]] = Field(min_length=1)
    stream: bool
    scenario: str = "plain"
    chunk_size: int | None = Field(default=None, ge=1, le=512)
    delay_ms: int = Field(default=0, ge=0, le=2000)
    test_text: StrictStr | None = Field(default=None, max_length=4096)

    @model_validator(mode="after")
    def validate_custom_text(self) -> "Completion":
        if self.scenario == "custom" and not self.test_text:
            raise ValueError("custom scenario requires nonempty test_text")
        if self.scenario != "custom" and self.test_text is not None:
            raise ValueError("test_text is only available for the custom scenario")
        return self


def create_app(
    provider_url: str = "http://127.0.0.1:8003/v1/chat/completions",
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    timeout: float = 10.0,
    enable_playground: bool | None = None,
) -> FastAPI:
    app = FastAPI(title="Task 3 streaming PII guardrail")
    app.state.last_metrics = None

    playground_enabled = enable_playground if enable_playground is not None else os.getenv("TASK3_ENABLE_PLAYGROUND") == "1"
    if playground_enabled:
        @app.get("/playground", response_class=HTMLResponse, include_in_schema=False)
        async def playground() -> HTMLResponse:
            page = Path(__file__).with_name("playground.html").read_text(encoding="utf-8")
            return HTMLResponse(page, headers={"Cache-Control": "no-store"})

    @app.post("/v1/chat/completions")
    async def completions(request: Request) -> StreamingResponse:
        try:
            payload = await request.json()
            completion = Completion.model_validate(payload)
        except (ValueError, ValidationError):
            raise HTTPException(422, "Invalid streaming completion request") from None
        if not completion.stream:
            raise HTTPException(400, "Only stream=true is supported")

        client = httpx.AsyncClient(transport=transport, timeout=timeout, trust_env=False)
        try:
            upstream_request = client.build_request("POST", provider_url, json=completion.model_dump())
            upstream = await client.send(upstream_request, stream=True)
            if upstream.status_code != 200:
                await upstream.aclose()
                await client.aclose()
                raise HTTPException(502, "Provider unavailable")
        except httpx.TimeoutException:
            await client.aclose()
            raise HTTPException(504, "Provider timed out") from None
        except httpx.RequestError:
            await client.aclose()
            raise HTTPException(502, "Provider unavailable") from None

        async def stream() -> AsyncIterator[bytes]:
            redactor = Redactor()
            completed = False
            try:
                async for data in data_events(upstream.aiter_raw()):
                    if data == "[DONE]":
                        completed = True
                        tail = redactor.finish()
                        if tail:
                            yield content_event(tail)
                        yield DONE
                        break
                    try:
                        value = json.loads(data)
                    except (json.JSONDecodeError, ValueError) as exc:
                        raise StreamFormatError("Malformed JSON event") from exc
                    if not isinstance(value, dict) or set(value) != {"choices"} or not isinstance(value.get("choices"), list):
                        raise StreamFormatError("Unsupported event")
                    if len(value["choices"]) != 1:
                        raise StreamFormatError("Expected exactly one choice")
                    # Rebuild outbound events from inspected text only; discard metadata.
                    for choice in value["choices"]:
                        if not isinstance(choice, dict) or set(choice) != {"delta"} or not isinstance(choice.get("delta"), dict):
                            raise StreamFormatError("Unsupported delta")
                        if not set(choice["delta"]) <= {"content"}:
                            raise StreamFormatError("Unsupported delta field")
                        content = choice["delta"].get("content")
                        if content is None:
                            continue
                        if not isinstance(content, str):
                            raise StreamFormatError("Unsupported content")
                        safe = redactor.feed(content)
                        if safe:
                            yield content_event(safe)
                if not completed:
                    raise StreamFormatError("Provider disconnected before completion")
            except (StreamFormatError, httpx.RequestError, httpx.TimeoutException, httpx.StreamError):
                LOGGER.warning("Provider stream failed")
                yield ERROR
            finally:
                app.state.last_metrics = {"chunks": redactor.chunks, "max_pending": redactor.max_pending_seen}
                await upstream.aclose()
                await client.aclose()

        return StreamingResponse(stream(), media_type="text/event-stream")

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("task3.gateway:app", host="127.0.0.1", port=8002)
