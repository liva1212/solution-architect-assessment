"""Persistent exact rolling-window quota with atomic SQLite reservations."""

from __future__ import annotations

import asyncio
import sqlite3
import time
import uuid
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class Decision:
    reservation_id: str | None
    eligible_at: float | None = None


class TokenLedger:
    def __init__(self, path: str | Path, limit: int = 50_000, clock: Callable[[], float] = time.time):
        if limit < 1:
            raise ValueError("limit must be positive")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.limit = limit
        self.clock = clock
        with closing(self._connect()) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS usage (
                id TEXT PRIMARY KEY, tenant TEXT NOT NULL, created_at REAL NOT NULL,
                tokens INTEGER NOT NULL CHECK(tokens >= 0), status TEXT NOT NULL
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS usage_tenant_time ON usage(tenant, created_at)")

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=5000")
        return db

    def _reserve(self, tenant: str, tokens: int) -> Decision:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            now = self.clock()
            db.execute("DELETE FROM usage WHERE created_at <= ?", (now - 60,))
            rows = db.execute(
                "SELECT created_at, tokens FROM usage WHERE tenant=? AND created_at > ? ORDER BY created_at, id",
                (tenant, now - 60),
            ).fetchall()
            used = sum(row[1] for row in rows)
            if tokens <= self.limit and used + tokens <= self.limit:
                reservation_id = uuid.uuid4().hex
                db.execute(
                    "INSERT INTO usage(id,tenant,created_at,tokens,status) VALUES(?,?,?,?,?)",
                    (reservation_id, tenant, now, tokens, "reserved"),
                )
                db.commit()
                return Decision(reservation_id)
            eligible_at = None
            if tokens <= self.limit:
                for stamp, amount in rows:
                    used -= amount
                    if used + tokens <= self.limit:
                        eligible_at = stamp + 60
                        break
            db.commit()
            return Decision(None, eligible_at)
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    async def reserve(self, tenant: str, tokens: int) -> Decision:
        return await asyncio.to_thread(self._reserve, tenant, tokens)

    def _settle(self, reservation_id: str, tokens: int) -> None:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE usage SET tokens=?, status='settled' WHERE id=? AND status='reserved'", (tokens, reservation_id))
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    async def settle(self, reservation_id: str, tokens: int) -> None:
        await asyncio.to_thread(self._settle, reservation_id, tokens)

    def _usage(self, tenant: str) -> int:
        now = self.clock()
        with closing(self._connect()) as db:
            return db.execute("SELECT COALESCE(SUM(tokens),0) FROM usage WHERE tenant=? AND created_at>?", (tenant, now - 60)).fetchone()[0]

    async def usage(self, tenant: str) -> int:
        return await asyncio.to_thread(self._usage, tenant)
