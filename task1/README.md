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

Pydantic v2 validates types, exact ASCII ID syntax, positive finite amounts, trimmed reason length, and extra fields. The advertised refund schema includes `exclusiveMinimum: 0` for amount and `minLength: 10` for reason. The reason validator also trims surrounding whitespace and checks the trimmed length. The SDK's low-level server advertises these schemas but does not validate tool arguments automatically. This server validates them and raises SDK `MCPError(INVALID_PARAMS)` so invalid arguments produce JSON-RPC `-32602`. A correctly formatted unknown customer instead returns `CallToolResult(isError=true)`. Unknown tool names use `-32602`.

The SDK 2.3.0 stdio reader forwards malformed-line parsing exceptions to its dispatcher. The dispatcher drops them by default; its supported `on_stream_exception` hook is an observer and does not send a JSON-RPC error. There is no supported server setting here that emits `-32700`. Malformed JSON lines are silently discarded and subsequent valid requests still work. This server keeps the official stdio transport intact.

Only the SDK's stdio transport writes to stdout. Application logging uses stderr. The mock has no persistence, authentication, or real financial effects.

## Manual Verification

On November 6, 2026, Task 1 was tested in GitHub Codespaces with MCP Inspector over stdio. MCP Inspector selected the correct tool for each request.

| Test | Expected result | Actual result |
| --- | --- | --- |
| Valid customer lookup | Return fictional customer record | PASS |
| Invalid customer ID | JSON-RPC `-32602` | PASS |
| Unknown customer | MCP tool error | PASS |
| Valid simulated refund | Return simulated refund confirmation | PASS |
| Negative refund amount | JSON-RPC `-32602` | PASS |
| Refund reason too short | JSON-RPC `-32602` | PASS |

Reproduce successful calls in MCP Inspector using these tool arguments:

```json
{"customer_id":"CUST-00001"}
```

Use that input with `get_customer_record`; the response includes `customer_id`, fictional `name`, `email`, `status`, and `account_balance`. For `trigger_refund`, use:

```json
{"customer_id":"CUST-00001","amount":2.5,"reason":"Duplicate order"}
```

The response includes a `SIM-` refund ID and `status: "simulated_refund"`.
