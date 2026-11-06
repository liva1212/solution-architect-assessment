# Task 4: token-aware model router

This independent assessment service accepts non-streaming mock completion requests. It authenticates a fictional tenant, reserves a maximum token budget in on-disk SQLite, calls a primary provider, and tries one secondary provider only after primary HTTP 429 or an overall 3.0-second primary timeout. No paid model or credential is needed. It is not a complete production AI gateway.

```text
Client → gateway (8004) → SQLite reservation → primary (8005)
                                     └ on 429 or timeout → secondary (8006)
```

## Run in a local shell or GitHub Codespaces

From the repository root (Python 3.11 or newer):

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
```

Start each service in its own terminal:

```sh
.venv/bin/python -m task4.providers primary
.venv/bin/python -m task4.providers secondary
TASK4_DB_PATH=.data/task4.sqlite3 .venv/bin/python -m task4.gateway
```

All bind to `127.0.0.1` by default. Keep Codespaces forwarded ports private. In a fourth terminal, call the gateway:

```sh
curl -i http://127.0.0.1:8004/v1/chat/completions -H 'Authorization: Bearer demo-tenant-a-key' -H 'Content-Type: application/json' -d '{"model":"mock-llm","messages":[{"role":"user","content":"Explain aircraft lift briefly."}],"max_tokens":20,"stream":false}'
curl -i http://127.0.0.1:8004/v1/chat/completions -H 'Authorization: Bearer demo-tenant-a-key' -H 'Content-Type: application/json' -d '{"model":"mock-llm","messages":[{"role":"user","content":"Explain aircraft lift briefly."}],"max_tokens":20,"stream":false,"primary_scenario":"rate_limit"}'
curl -i http://127.0.0.1:8004/v1/chat/completions -H 'Authorization: Bearer demo-tenant-a-key' -H 'Content-Type: application/json' -d '{"model":"mock-llm","messages":[{"role":"user","content":"Explain aircraft lift briefly."}],"max_tokens":20,"stream":false,"primary_scenario":"timeout"}'
```

The normalized response's `provider` field shows `primary` or `secondary`; the timeout example takes about three seconds. For a quota demo, restart only the gateway with `TASK4_QUOTA=40 TASK4_DB_PATH=.data/demo-quota.sqlite3 .venv/bin/python -m task4.gateway`, then repeat the first command. A request reserves input tokens plus `max_tokens`, so a second request should receive HTTP 429 and `Retry-After`. Use `demo-tenant-b-key` to see an independent tenant allowance. Restart the gateway with the same database path; its quota state persists until entries leave the 60-second window. Change the demo path if prior usage affects the demonstration. Optional near-expiration waiting is enabled with `TASK4_MAX_WAIT_SECONDS=2`; the default is `0` and rejects immediately.

Run the full suite with `.venv/bin/python -m pytest -q`.

## Quota and concurrency

The server-side registry maps the two fictional Bearer keys to `tenant-a` and `tenant-b`; request fields cannot choose a tenant or provider URL. Production authentication would replace this registry. SQLite stores tenant identifiers, not raw keys. The default limit is **50,000 tokens per tenant in (T − 60 seconds, T]**. Unlike a request-count limit, this accounts for the size of prompts and the maximum requested output. The gateway uses tiktoken `cl100k_base` for message content plus three fixed tokens per message and three reply-priming tokens. It reserves **input tokens + `max_tokens`** once, before either provider. The mock providers use the same encoding and cap output at `max_tokens`; real providers need their own token and response adapters.

Each reservation opens its own SQLite connection, runs `BEGIN IMMEDIATE`, evicts entries at or before T − 60, sums the tenant's remaining usage, checks the limit, inserts a reservation, and commits. WAL mode, a busy timeout, and a `(tenant, created_at)` index support short concurrent transactions. The transaction prevents two callers from observing and spending the same available capacity. SQLite locks are closed before any wait or provider call. A successful valid response settles the row to verified actual usage. The gateway rejects inconsistent, inflated, or underreported provider usage and retains the full reservation.

For failed requests and cancellation, the full reservation remains charged for its 60-second window, even after a known rejection. This conservative assessment policy avoids releasing capacity when upstream work or billing is uncertain; it can overcount attempts that generated nothing. A primary-to-secondary switch still uses only the original gateway reservation. There is no separate backup budget. The database schema records `id`, `tenant`, wall-clock `created_at`, `tokens`, and `status` (`reserved` or `settled`). Generated `.sqlite3`, WAL, and SHM files are ignored by Git.

When a request is over quota, the gateway calculates the first expiration at which cumulative remaining usage would fit. It returns `Retry-After` rounded up to whole seconds. If that time is within the configured maximum automatic wait, it closes the transaction, sleeps asynchronously, and retries a **fresh atomic reservation**. Competing requests can win the capacity, so a wake never guarantees admission. At most five checks are made; cancellation interrupts sleep. The default maximum wait of zero preserves immediate 429 behavior. This is a bounded convenience, not a queue.

## Routing, errors, and limits

Primary success returns normalized content and usage. Primary 429 or an overall 3.0-second timeout triggers exactly one secondary attempt. Primary 500, malformed response, or connection failure returns a sanitized 502 without failover. Secondary failure returns sanitized 502; a secondary timeout returns 504. The secondary deadline defaults to 3.0 seconds. No timeout is refreshed by partial bytes. Provider URLs are fixed server-side.

Gateway-generated errors share `{"error":{"code":"...","message":"...","request_id":"..."}}`. Codes include `unauthorized`, `invalid_request`, `rate_limit_exceeded`, `primary_provider_error`, `providers_unavailable`, and `gateway_unavailable`. Upstream bodies, database paths, credentials, and raw exception text are not returned. The demo scenarios are `success`, `rate_limit`, `timeout`, `error`, and `invalid` for each provider. Only synthetic prompts should be used. A local SQLite ledger is suitable for this assessment, not a distributed quota service; provider-specific billing and richer authentication are outside scope.

## Verification

The focused Task 4 suite passed **26 tests**; the complete Tasks 1–4 suite passed **146 tests** locally and in GitHub Actions. Tests cover real localhost provider fallback, exact window boundaries, separate SQLite connections, persistence, simultaneous waiters, cancellation, and sanitized errors. A real-clock localhost test verifies the default 3.0-second primary timeout; a separate local sample returned from the secondary in **3.029 seconds**. These times are local observations, not latency guarantees.
