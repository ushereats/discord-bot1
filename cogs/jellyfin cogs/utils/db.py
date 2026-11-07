"""
Shared database utilities for all Jellyfin cogs.

Two patterns:
  1. Persistent connection (registry cogs): open once, reuse, close on unload.
     Use Database class directly.
  2. Ad-hoc queries (stats, leaderboard, trending, etc.): open/close per query.
     Use run_query() and run_read() helpers.

Both set WAL journal mode and NORMAL synchronous on first connect.
"""

import os
import asyncio
import logging

import aiosqlite

logger = logging.getLogger("db_utils")


class Database:
    """A persistent aiosqlite connection with a write lock for atomic commits.

    Usage:
        db = Database("/path/to/file.db")
        await db.open()
        # reads
        rows = await db.fetch_all("SELECT * FROM foo")
        row = await db.fetch_one("SELECT * FROM foo WHERE id = ?", (1,))
        # writes (serialized, atomic)
        await db.write(("INSERT INTO foo VALUES (?)", (1,)))
        await db.write(
            ("INSERT INTO foo VALUES (?)", (1,)),
            ("UPDATE bar SET x = ? WHERE id = ?", (2, 3)),
        )
        # cleanup
        await db.close()
    """

    def __init__(self, path: str, *, row_factory: bool = True):
        self._path = path
        self._row_factory = row_factory
        self._conn: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def open(self) -> "Database":
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        if self._row_factory:
            self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.commit()
        return self

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database not open")
        return self._conn

    async def close(self):
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # --- reads (no lock needed) ---

    async def fetch_all(self, sql: str, params: tuple = ()) -> list[dict]:
        async with self.conn.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def fetch_one(self, sql: str, params: tuple = ()) -> dict | None:
        async with self.conn.execute(sql, params) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def fetch_val(self, sql: str, params: tuple = ()):
        """Single scalar value from the first row, first column."""
        async with self.conn.execute(sql, params) as cursor:
            row = await cursor.fetchone()
        return row[0] if row else None

    # --- writes (serialized on the lock) ---

    async def write(self, *statements: tuple[str, tuple]) -> list[int]:
        """Execute one or more (sql, params) as a single atomic commit.
        Returns each statement's rowcount."""
        async with self._write_lock:
            try:
                counts = []
                for sql, params in statements:
                    cur = await self.conn.execute(sql, params)
                    counts.append(cur.rowcount)
                await self.conn.commit()
                return counts
            except Exception:
                await self.conn.rollback()
                raise

    async def execute(self, sql: str, params: tuple = ()):
        """Single execute + commit, for DDL and simple writes."""
        await self.conn.execute(sql, params)
        await self.conn.commit()

    async def execute_many(self, sql: str, params_list: list[tuple]):
        await self.conn.executemany(sql, params_list)
        await self.conn.commit()


# --- Ad-hoc helpers for cogs that don't keep a persistent connection ---

async def run_query(db_path: str, sql: str, params: tuple = ()) -> list[dict]:
    """Open, query, close. Returns list of dicts."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(sql, params) as cursor:
            return [dict(r) for r in await cursor.fetchall()]


async def run_write(db_path: str, sql: str, params: tuple = ()) -> int:
    """Open, write, commit, close. Returns rowcount."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    async with aiosqlite.connect(db_path) as conn:
        cursor = await conn.execute(sql, params)
        await conn.commit()
        return cursor.rowcount


async def run_script(db_path: str, sql: str):
    """Open, executescript, close. For DDL / multi-statement init."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    async with aiosqlite.connect(db_path) as conn:
        await conn.executescript(sql)
