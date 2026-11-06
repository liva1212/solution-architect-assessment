"""Gateway routing, quota, bounded wait, and sanitized failures."""

import asyncio
import json
import sqlite3

import httpx
import pytest

from task4.gateway import create_app, input_tokens, Completion
from task4.providers import create_app as provider_app
from test_task4_limiter import Clock

REQUEST = {"model": "mock-llm", "messages": [{"role": "user", "content": "Explain lift briefly."}], "max_tokens": 10, "stream": False}
AUTH = {"Authorization": "Bearer demo-tenant-a-key"}


def run(coro):
    return asyncio.run(coro)


async def call(app, payload=REQUEST, headers=AUTH):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
        return await client.post("/v1/chat/completions", json=payload, headers=headers)


def gateway(tmp_path, **kw):
    primary = provider_app("primary")
    secondary = provider_app("secondary")
    app = create_app(db_path=tmp_path / "q.sqlite3", primary_transport=httpx.ASGITransport(app=primary),
                     secondary_transport=httpx.ASGITransport(app=secondary), **kw)
    return app, primary, secondary


def test_tokenizer_reservation_settlement_and_provider_selection(tmp_path):
    body = Completion.model_validate(REQUEST)
    assert input_tokens(body) > len(REQUEST["messages"][0]["content"].split())
    app, primary, secondary = gateway(tmp_path)
    response = run(call(app))
    assert response.status_code == 200
    assert response.json()["provider"] == "primary"
    assert primary.state.calls == 1 and secondary.state.calls == 0
    assert run(app.state.ledger.usage("tenant-a")) == response.json()["usage"]["total_tokens"]


@pytest.mark.parametrize("scenario", ["rate_limit", "timeout"])
def test_primary_fallback_once(tmp_path, scenario):
    app, primary, secondary = gateway(tmp_path, primary_timeout=0.02)
    response = run(call(app, {**REQUEST, "primary_scenario": scenario}))
    assert response.status_code == 200
    assert response.json()["provider"] == "secondary"
    assert primary.state.calls == secondary.state.calls == 1
    assert run(app.state.ledger.usage("tenant-a")) == response.json()["usage"]["total_tokens"]


@pytest.mark.parametrize("scenario", ["error", "invalid"])
def test_primary_failure_no_fallback_and_safe_error(tmp_path, scenario):
    app, primary, secondary = gateway(tmp_path)
    response = run(call(app, {**REQUEST, "primary_scenario": scenario}))
    assert response.status_code == 502
    assert response.json()["error"]["code"] == "primary_provider_error"
    assert "synthetic internal" not in response.text
    assert secondary.state.calls == 0


@pytest.mark.parametrize("scenario,status", [("error", 502), ("invalid", 502), ("timeout", 504)])
def test_secondary_failure_is_sanitized(tmp_path, scenario, status):
    app, _, secondary = gateway(tmp_path, secondary_timeout=0.02)
    response = run(call(app, {**REQUEST, "primary_scenario": "rate_limit", "secondary_scenario": scenario}))
    assert response.status_code == status
    assert response.json()["error"]["code"] == "providers_unavailable"
    assert secondary.state.calls == 1


def test_quota_is_before_providers_and_tenant_isolation(tmp_path):
    cost = input_tokens(Completion.model_validate(REQUEST)) + REQUEST["max_tokens"]
    app, primary, secondary = gateway(tmp_path, quota=cost)
    assert run(call(app)).status_code == 200
    denied = run(call(app))
    assert denied.status_code == 429 and int(denied.headers["Retry-After"]) >= 1
    assert denied.json()["error"]["code"] == "rate_limit_exceeded"
    assert primary.state.calls == 1 and secondary.state.calls == 0
    other = run(call(app, headers={"Authorization": "Bearer demo-tenant-b-key"}))
    assert other.status_code == 200


@pytest.mark.parametrize("payload", [{**REQUEST, "max_tokens": 0}, {**REQUEST, "max_tokens": True},
                                     {**REQUEST, "max_tokens": "10"}, {**REQUEST, "tenant_id": "tenant-b"},
                                     {**REQUEST, "provider_url": "http://evil.example"}])
def test_invalid_request_rejected_before_provider(tmp_path, payload):
    app, primary, _ = gateway(tmp_path)
    response = run(call(app, payload))
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    assert primary.state.calls == 0


def test_authentication_and_default_timeout(tmp_path):
    app, primary, _ = gateway(tmp_path)
    assert app.state.primary_timeout == 3.0
    for headers in ({}, {"Authorization": "Bearer wrong"}):
        response = run(call(app, headers=headers))
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "unauthorized"
    assert primary.state.calls == 0


def test_database_failure_and_key_storage_are_safe(tmp_path):
    app, _, _ = gateway(tmp_path)
    async def fail(*_):
        raise sqlite3.OperationalError("secret database path")
    app.state.ledger.reserve = fail
    response = run(call(app))
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "gateway_unavailable"
    assert "secret database path" not in response.text
    assert b"demo-tenant-a-key" not in (tmp_path / "q.sqlite3").read_bytes()


def test_untrustworthy_usage_keeps_full_reservation(tmp_path):
    async def handler(request):
        return httpx.Response(200, json={"id": "mock", "model": "mock-llm",
            "choices": [{"message": {"role": "assistant", "content": "Longer content"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
    app = create_app(db_path=tmp_path / "q.sqlite3", primary_transport=httpx.MockTransport(handler))
    response = run(call(app))
    assert response.status_code == 502
    expected = input_tokens(Completion.model_validate(REQUEST)) + REQUEST["max_tokens"]
    assert run(app.state.ledger.usage("tenant-a")) == expected


def test_bounded_wait_admits_after_expiration_and_persists(tmp_path):
    clock = Clock()
    cost = input_tokens(Completion.model_validate(REQUEST)) + REQUEST["max_tokens"]
    app, primary, _ = gateway(tmp_path, quota=cost, clock=clock, sleeper=clock.sleep, max_wait=2)
    assert run(app.state.ledger.reserve("tenant-a", cost)).reservation_id
    clock.now = 1059
    response = run(call(app))
    assert response.status_code == 200
    assert clock.now == 1060
    assert primary.state.calls == 1
    reopened, _, _ = gateway(tmp_path, quota=cost, clock=clock)
    assert run(reopened.state.ledger.usage("tenant-a")) == response.json()["usage"]["total_tokens"]


def test_wait_too_long_and_cumulative_retry_after(tmp_path):
    clock = Clock()
    cost = input_tokens(Completion.model_validate(REQUEST)) + REQUEST["max_tokens"]
    app, primary, _ = gateway(tmp_path, quota=cost + 1, clock=clock, sleeper=clock.sleep, max_wait=2)
    run(app.state.ledger.reserve("tenant-a", cost - 1))
    clock.now = 1001
    run(app.state.ledger.reserve("tenant-a", 2))
    clock.now = 1058
    response = run(call(app))
    assert response.status_code == 429
    assert response.headers["Retry-After"] == "3"
    assert primary.state.calls == 0


def test_wait_cancellation_retains_existing_reservation(tmp_path):
    clock = Clock()
    cost = input_tokens(Completion.model_validate(REQUEST)) + REQUEST["max_tokens"]
    async def sleeper(_):
        await asyncio.Event().wait()
    app, primary, _ = gateway(tmp_path, quota=cost, clock=clock, sleeper=sleeper, max_wait=2)
    async def check():
        await app.state.ledger.reserve("tenant-a", cost)
        clock.now = 1059
        task = asyncio.create_task(call(app))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await app.state.ledger.usage("tenant-a") == cost
    run(check())
    assert primary.state.calls == 0


def test_simultaneous_waiters_recheck_atomic_quota(tmp_path):
    clock = Clock()
    cost = input_tokens(Completion.model_validate(REQUEST)) + REQUEST["max_tokens"]
    awake = asyncio.Event()
    entered = asyncio.Event()
    waiting = 0

    async def sleeper(_):
        nonlocal waiting
        waiting += 1
        if waiting == 2:
            entered.set()
        await awake.wait()

    app, primary, _ = gateway(tmp_path, quota=cost, clock=clock, sleeper=sleeper, max_wait=2)

    async def check():
        await app.state.ledger.reserve("tenant-a", cost)
        clock.now = 1059
        requests = [asyncio.create_task(call(app)) for _ in range(2)]
        await asyncio.wait_for(entered.wait(), timeout=2)
        clock.now = 1060
        awake.set()
        responses = await asyncio.gather(*requests)
        assert sorted(r.status_code for r in responses) == [200, 429]

    run(check())
    assert primary.state.calls == 1
