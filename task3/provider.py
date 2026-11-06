"""Separately runnable deterministic streaming mock LLM provider."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, StrictStr, model_validator


class Completion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: str
    messages: list[dict[str, str]]
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


TEXT = {
    "plain": "Hello from the fictional provider. ",
    "email": "Contact jane@example.com for help. ",
    "email_split": "Please contact jane@example.com for assistance. ",
    "ssn": "Example SSN 123-45-6789 is private. ",
    "ssn_split": "Example SSN 123-45-6789 is private. ",
    "card": "Test card 4111 1111 1111 1111 is private. ",
    "card_split": "Test card 4111 1111 1111 1111 is private. ",
    "multiple": "Email jane@example.com, SSN 123-45-6789, card 4111 1111 1111 1111. ",
    "delayed": "First safe words. More safe words. Final safe words. ",
    "interrupted": "An incomplete upstream stream. ",
}


def _event(piece: str) -> bytes:
    payload = {"choices": [{"delta": {"content": piece}}]}
    return ("data: " + json.dumps(payload, ensure_ascii=False) + "\n\n").encode()


def create_app() -> FastAPI:
    app = FastAPI(title="Task 3 mock LLM provider")
    app.state.active_streams = 0

    @app.post("/v1/chat/completions")
    async def completions(request: Completion) -> StreamingResponse:
        if request.scenario not in TEXT and request.scenario not in {"long", "custom"}:
            from fastapi import HTTPException
            raise HTTPException(400, "Unknown mock scenario")

        async def generate() -> AsyncIterator[bytes]:
            app.state.active_streams += 1
            try:
                if request.scenario == "long":
                    pieces = ("safe " for _ in range(5000))
                else:
                    text = request.test_text if request.scenario == "custom" else TEXT[request.scenario]
                    assert text is not None
                    size = request.chunk_size or (1 if request.scenario.endswith("_split") else 12)
                    pieces = (text[index:index + size] for index in range(0, len(text), size))
                for piece in pieces:
                    yield _event(piece)
                    if request.delay_ms:
                        await asyncio.sleep(request.delay_ms / 1000)
                if request.scenario != "interrupted":
                    yield b"data: [DONE]\n\n"
            finally:
                app.state.active_streams -= 1

        return StreamingResponse(generate(), media_type="text/event-stream")

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("task3.provider:app", host="127.0.0.1", port=8003)
