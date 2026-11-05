# Task 1: customer records and simulated refunds

Requires Python 3.11 or newer. From the repository root:

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python -m task1.server
```

The last command starts an MCP stdio server. Connect an MCP client using that command and call:

- `get_customer_record` with `{"customer_id":"CUST-00001"}`.
- `trigger_refund` with `{"customer_id":"CUST-00001","amount":2.5,"reason":"Duplicate order"}`.

Run the integration suite with `.venv/bin/python -m pytest -q`.

The five records use fictional names and `example.com` addresses. Refunds are simulated; the in-memory log resets when the process restarts. No real payment system or database is connected.

Pydantic v2 validates types, exact ASCII ID syntax, positive finite amounts, trimmed reason length, and extra fields. The SDK's low-level server advertises these schemas but does not validate tool arguments automatically. This server validates them and raises SDK `MCPError(INVALID_PARAMS)` so invalid arguments produce JSON-RPC `-32602`. A correctly formatted unknown customer instead returns `CallToolResult(isError=true)`. Unknown tool names use `-32602`. The SDK's stdio reader silently discards malformed JSON lines; subsequent valid requests still work.

Only the SDK's stdio transport writes to stdout. Application logging uses stderr. The mock has no persistence, authentication, or real financial effects.
