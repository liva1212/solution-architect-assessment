# Task 2: MCP security gateway

This assessment-focused HTTP/JSON-RPC proxy uses FastAPI and HTTPX. It is separate from Task 1's stdio MCP server. It is not a complete production OAuth authorization implementation.

## Run in GitHub Codespaces

From the repository root, install with Python 3.11 or newer:

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
```

Start each server in a separate terminal:

```sh
.venv/bin/python -m task2.downstream
.venv/bin/python -m task2.gateway
```

The downstream mock listens on `127.0.0.1:8001`; the gateway listens on `127.0.0.1:8000`. Both expose `/docs` for local API inspection. Codespaces terminal access works without public port forwarding; if forwarding is used, keep ports private. The demo tokens are assessment fixtures, not secrets.

Run requests against the gateway:

```sh
curl -sS http://127.0.0.1:8000/mcp -H 'Authorization: Bearer demo-viewer-token' -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
curl -sS http://127.0.0.1:8000/mcp -H 'Authorization: Bearer demo-viewer-token' -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_customer_record"}}'
curl -sS http://127.0.0.1:8000/mcp -H 'Authorization: Bearer demo-viewer-token' -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"admin_reset_key"}}'
curl -sS http://127.0.0.1:8000/mcp -H 'Authorization: Bearer demo-admin-token' -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"admin_reset_key"}}'
```

The blocked viewer call returns `-32001` and never reaches downstream. For these four requests, the downstream terminal shows three HTTP POSTs: the two ordinary requests and the authorized admin request. The automated integration tests also assert that the downstream received-request count stays unchanged for blocked calls.

Run all Task 1 and Task 2 tests:

```sh
.venv/bin/python -m pytest -q
```

## Request flow and security policy

The gateway checks `Authorization: Bearer <token>` against its server-side mock registry (`demo-admin-token` → `admin`, `demo-viewer-token` → `viewer`). It ignores client-supplied role claims, so body fields and headers cannot elevate a viewer. Missing, malformed, or unknown credentials return HTTP 401 before forwarding. Tokens are never forwarded or logged. Replace this registry with a real verifier for production.

After authentication, the gateway parses a single JSON-RPC 2.0 request. Authenticated `tools/list` and other valid methods are forwarded unchanged. For `tools/call`, names starting with the **case-sensitive** `admin_` prefix require the `admin` role. All other tool names are forwarded. The gateway sends the original request body once to its configured downstream URL, with only `Content-Type`; it preserves downstream response bytes, status, ID, and result or error object. The URL is set by the application, not by request input.

Error mapping: malformed JSON → `-32700`; invalid request or unsupported batch → `-32600`; invalid tool parameters → `-32602`; viewer `admin_` call → required custom `-32001` (`Unauthorized Tool Call`). JSON-RPC errors use HTTP 200 so clients can read the protocol error body. Missing/invalid credentials use HTTP 401. Upstream connection failures use HTTP 502 and timeouts use HTTP 504, both with a safe `-32002` JSON-RPC error and no internal details. Unknown valid methods are forwarded; the mock downstream responds `-32601`. There is no retry logic.

The mock downstream has fictional customer and administrator results only. Its key reset does not create a credential. Batch requests, notifications without IDs, and full MCP HTTP session negotiation are outside this lightweight assessment scope. MCP Inspector interoperability is not required for this intentionally simple HTTP/JSON-RPC gateway.

## Manual verification — November 6, 2026

Verified in GitHub Codespaces with Python, FastAPI, and localhost HTTP/JSON-RPC:

| Request | Result |
| --- | --- |
| Viewer lists available tools | PASS |
| Viewer calls `get_customer_record` | PASS |
| Viewer calls `admin_reset_key` | PASS — blocked with JSON-RPC `-32001` |
| Admin calls `admin_reset_key` | PASS |
| Request without authentication | PASS — HTTP 401 |

The downstream terminal displayed exactly three HTTP POSTs, corresponding to the three authorized requests. The full local automated suite passed: **46 tests**. Its integration tests independently verify that unauthorized requests never reach the downstream mock server.
