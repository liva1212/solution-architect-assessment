# Task 3: streaming LLM PII guardrail

This assessment implementation proxies OpenAI-style text-delta SSE from a local mock provider through an HTTP gateway. It redacts email, SSN-format, and Luhn-valid 13–19 digit card candidates before emitting client-visible content. It is not universal PII protection or a production LLM integration.

## Run locally or in GitHub Codespaces

From the repository root, install with Python 3.11 or newer:

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
```

Start the mock provider in Terminal 1 and the gateway in Terminal 2:

```sh
.venv/bin/python -m task3.provider
.venv/bin/python -m task3.gateway
```

They bind to `127.0.0.1:8003` and `127.0.0.1:8002`, respectively. Keep Codespaces forwarded ports private; local terminal use needs no public exposure. In Terminal 3, use `curl -N` so the output appears as SSE frames while generation continues:

```sh
curl -N http://127.0.0.1:8002/v1/chat/completions -H 'Content-Type: application/json' -d '{"model":"mock-llm","messages":[{"role":"user","content":"Sample"}],"stream":true,"scenario":"plain","delay_ms":150}'
curl -N http://127.0.0.1:8002/v1/chat/completions -H 'Content-Type: application/json' -d '{"model":"mock-llm","messages":[{"role":"user","content":"Sample"}],"stream":true,"scenario":"email_split","delay_ms":50}'
curl -N http://127.0.0.1:8002/v1/chat/completions -H 'Content-Type: application/json' -d '{"model":"mock-llm","messages":[{"role":"user","content":"Sample"}],"stream":true,"scenario":"ssn_split","delay_ms":50}'
curl -N http://127.0.0.1:8002/v1/chat/completions -H 'Content-Type: application/json' -d '{"model":"mock-llm","messages":[{"role":"user","content":"Sample"}],"stream":true,"scenario":"card_split","delay_ms":50}'
```

Supported mock scenarios are `plain`, `email`, `email_split`, `ssn`, `ssn_split`, `card`, `card_split`, `multiple`, `long`, `delayed`, and `interrupted`. Optional `chunk_size` (1–512) and `delay_ms` (0–2000) control provider deltas. The long scenario yields 5,000 small pieces without building a full response. The interrupted scenario intentionally omits `[DONE]`.

Run all Tasks 1–3 tests and the reproducible localhost timing sample:

```sh
.venv/bin/python -m pytest -q
.venv/bin/python -m task3.benchmark
```

## Stream and buffer design

The gateway validates `stream: true`, opens an HTTPX streaming response to its application-configured provider URL, and keeps that connection open until its FastAPI `StreamingResponse` completes or is cancelled. It incrementally decodes UTF-8, assembles complete SSE frames across transport reads, extracts only `choices[*].delta.content`, and emits newly built `data:` frames containing sanitized text. Empty or non-content deltas are ignored. Provider metadata is never forwarded. A successful stream ends with `data: [DONE]`.

The redactor keeps a potentially sensitive suffix pending across semantic deltas. Safe prefixes can leave immediately; an incomplete email or numeric run cannot. Per-chunk substitution alone would release fragments such as `jane` before `@example.com` arrives. Complete candidates are checked with regex; card candidates also need a Luhn checksum. Only fictional `example.com` addresses and published synthetic card-format numbers are in the mock.

Pending candidate state is capped at **256 characters**. An unresolved candidate that exceeds the cap is replaced once with `[REDACTED]`, and the rest of that candidate is suppressed until a safe delimiter. This fail-closed policy can redact long harmless tokens. Individual SSE frames are limited to **16 KiB**; transport reads are processed internally in 4 KiB slices without making HTTPX wait to fill a chunk. Email candidates longer than 256 characters are deliberately redacted rather than released. The bounded state costs some latency around ambiguous words and numbers; it does not hold the entire response.

The supported email regex covers common ASCII mailbox syntax, SSN detection covers `ddd-dd-dddd`, and card detection covers 13–19 digits with optional spaces or hyphens that pass Luhn. These patterns may miss other PII and can redact fictional values that merely match. Luhn is not proof of a real card. Ambiguous adjacent numeric sequences and non-ASCII mailbox formats are outside this defined scope.

Malformed UTF-8, oversized frames, invalid JSON deltas, unsupported content, or an upstream disconnect before `[DONE]` cause a safe SSE `event: error` and no completion marker. Before streaming starts, provider connection failures return HTTP 502 and timeouts return HTTP 504. The gateway does not expose raw upstream errors or content in error events. Client cancellation closes the upstream response and HTTPX client in the generator's `finally` block.

## Observed local timing

On a representative localhost run of `python -m task3.benchmark` with five provider deltas and 150 ms between deltas: direct provider first content **0.070 s**, total **0.822 s**; gateway first sanitized content **0.071 s**, total **0.828 s**. Added latency was about **0.002 s** to first content and **0.006 s** overall. The gateway processed **5 content chunks** and held at most **6 pending characters**. These are sample measurements, not guaranteed bounds. The automated real-HTTP test also asserts first sanitized content arrives before provider completion. For real providers, an adapter would need to validate their event schema, credentials, transport policy, and failure semantics before use.

In the separate long-response test, the gateway processed **5,000 content chunks** while its observed maximum pending state was **4 characters**; the configured maximum remains 256 characters.
