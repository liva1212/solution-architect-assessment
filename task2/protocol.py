"""Small JSON-RPC 2.0 request validation shared by the gateway and mock."""

from __future__ import annotations

import json
from typing import Any


def error(code: int, message: str, request_id: str | int | None = None) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def parse(raw: bytes) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    try:
        value = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON number")))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None, error(-32700, "Parse error")

    if not isinstance(value, dict):
        # Batch requests and other non-object inputs are outside this assessment's scope.
        return None, error(-32600, "Invalid request")
    request_id = value.get("id")
    if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
        return None, error(-32600, "Invalid request")
    if value.get("jsonrpc") != "2.0" or not isinstance(value.get("method"), str):
        return None, error(-32600, "Invalid request", request_id)
    if "params" in value and not isinstance(value["params"], (dict, list)):
        return None, error(-32602, "Invalid params", request_id)
    if value["method"] == "tools/call":
        params = value.get("params")
        if not isinstance(params, dict) or not isinstance(params.get("name"), str) or not params["name"]:
            return None, error(-32602, "Invalid params", request_id)
    return value, None
