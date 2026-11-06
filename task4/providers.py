"""Separate deterministic local completion providers; no external AI calls."""

from __future__ import annotations

import asyncio

import tiktoken
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

ENCODING = tiktoken.get_encoding("cl100k_base")


class ProviderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model: str
    messages: list[dict[str, str]]
    max_tokens: int = Field(gt=0)
    scenario: str = "success"


def create_app(which: str) -> FastAPI:
    app = FastAPI(title=f"Task 4 mock {which}")
    app.state.calls = 0

    @app.post("/v1/chat/completions")
    async def complete(body: ProviderRequest):
        app.state.calls += 1
        if body.scenario in {"timeout", "slow"}:
            await asyncio.sleep(4)
        if body.scenario == "rate_limit":
            return JSONResponse({"error": "synthetic rate limit"}, status_code=429)
        if body.scenario in {"error", "http_error"}:
            return JSONResponse({"error": "synthetic internal detail"}, status_code=500)
        if body.scenario == "invalid":
            return {"unexpected": "synthetic invalid body"}
        content = f"Fictional {which} response."
        # The mock obeys the requested output cap using the same published encoding.
        output = ENCODING.decode(ENCODING.encode(content)[:body.max_tokens])
        input_tokens = sum(len(ENCODING.encode(m["content"])) + 3 for m in body.messages) + 3
        output_tokens = len(ENCODING.encode(output))
        return {
            "id": f"mock-{which}", "model": body.model,
            "choices": [{"message": {"role": "assistant", "content": output}}],
            "usage": {"prompt_tokens": input_tokens, "completion_tokens": output_tokens, "total_tokens": input_tokens + output_tokens},
        }

    return app


primary_app = create_app("primary")
secondary_app = create_app("secondary")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2 or sys.argv[1] not in {"primary", "secondary"}:
        raise SystemExit("usage: python -m task4.providers primary|secondary")
    which = sys.argv[1]
    uvicorn.run(f"task4.providers:{which}_app", host="127.0.0.1", port=8005 if which == "primary" else 8006)
