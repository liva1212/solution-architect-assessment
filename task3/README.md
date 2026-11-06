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

## Interactive manual testing

The optional local playground is disabled by default. In GitHub Codespaces, use the setup commands above, start the mock provider in Terminal 1, then start the gateway in Terminal 2 with:

```sh
TASK3_ENABLE_PLAYGROUND=1 .venv/bin/python -m task3.gateway
```

Open the private forwarded port 8002 URL from the Codespaces **Ports** panel and append `/playground`. Enter synthetic text or choose a preset, select a provider chunk size and delay, and click **Start test**. The page shows the original text beside the progressively sanitized output, plus status, redaction marker count, content event count, time to first sanitized content, total duration, and optional raw sanitized SSE. The input limit is 4,096 characters. Try the email, SSN, and test-card presets for supported patterns; the unformatted SSN and obfuscated email presets demonstrate formats outside the current detector's scope. Use fictional data only. The page is served by the gateway and sends its request through the normal provider and redaction stream.

## Stream and buffer design

The gateway validates `stream: true`, opens an HTTPX streaming response to its application-configured provider URL, and keeps that connection open until its FastAPI `StreamingResponse` completes or is cancelled. It incrementally decodes UTF-8, assembles complete SSE frames across transport reads, extracts only `choices[0].delta.content`, and emits newly built `data:` frames containing sanitized text. The accepted provider payload is exactly `{"choices":[{"delta":{"content":"..."}}]}`; an empty `delta` is an allowed non-content event. Multiple choices, unknown fields, and unexpected named SSE events fail closed. Provider metadata is never forwarded. A successful stream ends with `data: [DONE]`.

The redactor keeps a potentially sensitive suffix pending across semantic deltas. Safe prefixes can leave immediately; an incomplete email or numeric run cannot. Per-chunk substitution alone would release fragments such as `jane` before `@example.com` arrives. Complete candidates are checked with regex; card candidates also need a Luhn checksum. Only fictional `example.com` addresses and published synthetic card-format numbers are in the mock.

Pending candidate state is capped at **256 characters**. An unresolved candidate that exceeds the cap is replaced once with `[REDACTED]`, and the rest of that candidate is suppressed until a safe delimiter. This fail-closed policy can redact long harmless tokens. Individual SSE frames are limited to **16 KiB**; transport reads are processed internally in 4 KiB slices without making HTTPX wait to fill a chunk. Email candidates longer than 256 characters are deliberately redacted rather than released. The bounded state costs some latency around ambiguous words and numbers; it does not hold the entire response.

The supported email regex covers common ASCII mailbox syntax, SSN detection covers `ddd-dd-dddd`, and card detection covers 13–19 digits with optional spaces or hyphens that pass Luhn. Incomplete email-like candidates containing `@`, partial hyphenated SSNs, and grouped 12-digit card prefixes are redacted at a boundary or successful stream end. This can hide harmless text. Luhn is not proof of a real card. Unformatted or space-separated SSNs, period-separated cards, Unicode mailbox names, and obfuscated addresses such as `jane [at] example.com` are deliberately outside scope. Adjacent numeric runs and other PII may also be missed. A Luhn-valid non-card number may be falsely redacted.

Malformed UTF-8, oversized frames, invalid JSON deltas, unsupported content, or an upstream disconnect before `[DONE]` cause a safe SSE `event: error` and no completion marker. Before streaming starts, provider connection failures return HTTP 502 and timeouts return HTTP 504. The gateway does not expose raw upstream errors or content in error events. Client cancellation closes the upstream response and HTTPX client in the generator's `finally` block; a real-HTTP test verifies that an early client disconnect leaves no active mock-provider stream.

## Observed local timing

On a representative localhost run of `python -m task3.benchmark` with five provider deltas and 150 ms between deltas: direct provider first content **0.072 s**, total **0.827 s**; gateway first sanitized content **0.076 s**, total **0.834 s**. Added latency was about **0.004 s** to first content and **0.006 s** overall. The gateway processed **5 content chunks** and held at most **6 pending characters**. These are sample measurements, not guaranteed bounds. The automated real-HTTP test also asserts first sanitized content arrives before provider completion. For real providers, an adapter would need to validate their event schema, credentials, transport policy, and failure semantics before use.

In the separate long-response test, the gateway processed **5,000 content chunks** while its observed maximum pending state was **4 characters**; the configured maximum remains 256 characters.

## Adversarial review

The added adversarial suite contributes **28 pytest cases**. It covers every single split boundary for representative synthetic email, SSN, and card values; 900 reproducibly seeded multi-chunk partitions; one-character deltas; values at the beginning and end; punctuation; 255/256/257-character candidate boundaries; 2,000 repeated sensitive chunks; byte-partitioned UTF-8 and SSE; malformed and oversized frames; 16 concurrent streams; and client-visible canary checks. The suite also exercises deliberately unsupported formats.

Confirmed defects in the original Task 3 draft were: incomplete candidate fragments emitted at a successful stream end; two choices incorrectly sharing one redaction state; unexpected named SSE events and unknown payload fields being accepted; and valid CR-only SSE boundaries being rejected. The fixes redact recognizable incomplete candidates, require one strictly shaped choice, reject unsupported event types and fields, and parse LF, CRLF, and CR incrementally. These findings were reproduced by failing tests before the fixes. The existing early-disconnect test still checks upstream cleanup.

One local stress run measured the following. First-content and total times include localhost HTTP and test-process overhead; they are observations, not latency guarantees.

| Scenario | First content | Total | Deltas | Max pending |
| --- | ---: | ---: | ---: | ---: |
| Short safe | 0.100 s | 0.101 s | 3 | 9 chars |
| Split email, 5 ms delay | 0.094 s | 0.336 s | 48 | 16 chars |
| Long safe | 0.352 s | 0.526 s | 5,000 | 4 chars |
| Delayed, 150 ms | 0.058 s | 0.815 s | 5 | 6 chars |

Four concurrent split-email requests completed in **0.525 s** wall time, with first-content times of **0.189–0.284 s**; their output remained isolated. Process high-water RSS rose from about **47.2 MB to 56.1 MB** across the sequential localhost runs, including both servers and HTTP clients. In a separate 2,000-chunk redactor-only stress run, `tracemalloc` peak was about **4.7 KB** and max pending state was **20 characters**. Neither measurement is a production memory bound. Each concurrent stream has its own redactor and up to the documented pending/frame limits.

Remaining risks are pattern false negatives outside the defined formats, false positives from fail-closed partial detection and Luhn-valid non-cards, and provider-specific schema incompatibility. Once safe text is sent it cannot be retracted if a later upstream event fails; the gateway then sends a safe SSE error and withholds `[DONE]`. Integration with a real provider requires an explicit schema adapter and a separate security review.
