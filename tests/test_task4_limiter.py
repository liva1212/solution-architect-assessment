"""On-disk quota, exact expiry, waiting and concurrency."""

import asyncio
import sqlite3

import pytest

from task4.limiter import TokenLedger


class Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    async def sleep(self, delay):
        self.now += delay


def run(coro):
    return asyncio.run(coro)


def test_exact_limit_boundary_tenants_and_persistence(tmp_path):
    clock = Clock()
    path = tmp_path / "nested" / "quota.sqlite3"
    first = TokenLedger(path, 10, clock)
    assert run(first.reserve("a", 9)).reservation_id
    assert run(first.reserve("a", 1)).reservation_id
    assert run(first.reserve("a", 1)).reservation_id is None
    assert run(first.reserve("b", 10)).reservation_id
    assert path.is_file()
    second = TokenLedger(path, 10, clock)
    assert run(second.usage("a")) == 10
    clock.now += 59.999
    assert run(second.reserve("a", 1)).reservation_id is None
    clock.now += 0.001
    assert run(second.reserve("a", 10)).reservation_id
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM usage WHERE tenant='a'").fetchone()[0] == 1


def test_settlement_and_cumulative_expiration(tmp_path):
    clock = Clock()
    ledger = TokenLedger(tmp_path / "q.sqlite3", 10, clock)
    first = run(ledger.reserve("a", 4)).reservation_id
    clock.now += 1
    second = run(ledger.reserve("a", 4)).reservation_id
    assert run(ledger.reserve("a", 5)).eligible_at == 1060
    assert run(ledger.reserve("a", 7)).eligible_at == 1061
    run(ledger.settle(first, 2))
    assert run(ledger.usage("a")) == 6
    assert run(ledger.reserve("a", 7)).eligible_at == 1061
    run(ledger.settle(second, 1))
    assert run(ledger.usage("a")) == 3


def test_concurrent_separate_connections_cannot_overbook(tmp_path):
    clock = Clock()
    path = tmp_path / "q.sqlite3"
    ledgers = [TokenLedger(path, 10, clock) for _ in range(8)]

    async def race():
        return await asyncio.gather(*(ledger.reserve("a", 3) for ledger in ledgers))

    results = run(race())
    assert sum(item.reservation_id is not None for item in results) == 3
    assert run(ledgers[0].usage("a")) == 9


def test_oversized_reservation_has_no_eligibility(tmp_path):
    ledger = TokenLedger(tmp_path / "q.sqlite3", 10)
    assert run(ledger.reserve("a", 11)).eligible_at is None
