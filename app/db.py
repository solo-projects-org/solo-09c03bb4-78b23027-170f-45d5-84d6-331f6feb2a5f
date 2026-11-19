"""SQLite storage layer.

Tables:
  trades            -- current state per (symbol, trade_no), keyed by revision
  minute_bars       -- current materialized minute bar per (symbol, minute)
  journal           -- append-only change log; its seq is the cursor risk control uses
  journal_provenance-- one row per journal entry created after tracing was enabled;
                       its existence separates "no historical snapshot (legacy)"
                       from "snapshot available"
  journal_trades    -- frozen per-trade composition of the minute at commit time.
                       Copies, never references: later corrections cannot rewrite it.

The provenance rows are written in the SAME transaction as the journal row, so a
committed change always has its historical composition and vice versa.

WAL + synchronous=FULL: a committed transaction survives a process restart or
power loss, so a cursor a client has already seen can never be "uncommitted".
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    symbol            TEXT    NOT NULL,
    trade_no          TEXT    NOT NULL,
    revision          INTEGER NOT NULL,
    op                TEXT    NOT NULL CHECK (op IN ('trade', 'correction', 'cancel')),
    trade_time_utc_ms INTEGER,
    price_cents       INTEGER,
    qty               INTEGER,
    cancelled         INTEGER NOT NULL DEFAULT 0,
    trade_no_num      INTEGER,
    PRIMARY KEY (symbol, trade_no)
);

CREATE TABLE IF NOT EXISTS minute_bars (
    symbol              TEXT    NOT NULL,
    minute_start_utc_ms INTEGER NOT NULL,
    open_cents          INTEGER NOT NULL,
    high_cents          INTEGER NOT NULL,
    low_cents           INTEGER NOT NULL,
    close_cents         INTEGER NOT NULL,
    volume              INTEGER NOT NULL,
    trade_count         INTEGER NOT NULL,
    PRIMARY KEY (symbol, minute_start_utc_ms)
);

CREATE TABLE IF NOT EXISTS journal (
    seq                 INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT    NOT NULL,
    minute_start_utc_ms INTEGER NOT NULL,
    change_type         TEXT    NOT NULL CHECK (change_type IN ('upsert', 'delete')),
    bar_json            TEXT
);

CREATE INDEX IF NOT EXISTS idx_journal_symbol_seq ON journal (symbol, seq);
CREATE INDEX IF NOT EXISTS idx_trades_minute ON trades (symbol, cancelled, trade_time_utc_ms);

-- Tracing is controlled per deployment (FEED_TRACE_ENABLED, default on). A
-- journal row with a provenance row has a frozen historical composition; one
-- without predates tracing or was written while it was off -> reported as
-- "historical composition unavailable" instead of being guessed from today's
-- state.
CREATE TABLE IF NOT EXISTS journal_provenance (
    seq           INTEGER PRIMARY KEY REFERENCES journal(seq),
    ingest_id     TEXT    NOT NULL,   -- groups the (1 or 2) journal rows of one commit
    symbol        TEXT    NOT NULL,
    trade_no      TEXT    NOT NULL,   -- trade that caused this change
    revision      INTEGER NOT NULL,   -- the revision that was committed
    op            TEXT    NOT NULL CHECK (op IN ('trade', 'correction', 'cancel')),
    trade_time_utc_ms INTEGER,        -- this trade's effective time/price/qty after commit
    price_cents   INTEGER,
    qty           INTEGER,
    cancelled     INTEGER NOT NULL DEFAULT 0,
    occurred_utc_ms INTEGER NOT NULL  -- server commit time, UTC ms
);

CREATE TABLE IF NOT EXISTS journal_trades (
    seq              INTEGER NOT NULL REFERENCES journal_provenance(seq),
    phase            TEXT    NOT NULL CHECK (phase IN ('before', 'after')),
    trade_no         TEXT    NOT NULL,
    revision         INTEGER NOT NULL,
    trade_time_utc_ms INTEGER NOT NULL,
    price_cents      INTEGER NOT NULL,
    qty              INTEGER NOT NULL,
    PRIMARY KEY (seq, phase, trade_no)
);

CREATE INDEX IF NOT EXISTS idx_journal_provenance_trade
    ON journal_provenance (symbol, trade_no, seq);
CREATE INDEX IF NOT EXISTS idx_journal_provenance_ingest
    ON journal_provenance (symbol, ingest_id, seq);
"""


def connect(db_path: str) -> sqlite3.Connection:
    # isolation_level=None -> autocommit; transactions are opened explicitly
    # with BEGIN / BEGIN IMMEDIATE in logic.py so commit boundaries are exact.
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(db_path: str) -> None:
    path = Path(db_path)
    if path.parent and str(path.parent) not in ("", "."):
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.executescript(SCHEMA)
    finally:
        conn.close()
