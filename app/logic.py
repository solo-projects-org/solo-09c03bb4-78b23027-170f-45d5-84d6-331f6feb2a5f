"""Core feed logic: revision-checked ingest, minute-bar recompute, cursor pulls.

Consistency rules:
  * Every ingest runs in ONE immediate transaction: trade-state upsert +
    minute-bar recompute + journal insert commit or roll back together.
    A failed write or a crash therefore never leaves a half-applied update,
    and a committed journal seq (cursor) can never be skipped afterwards.
  * Reads (snapshot / pull) run in a single read transaction, so clients only
    ever see fully committed changes.
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any, Optional

MINUTE_MS = 60_000
INT64_MAX = 9_223_372_036_854_775_807

# Tracing (historical minute composition per journal entry) is a deployment
# switch (FEED_TRACE_ENABLED). With it off, new journal rows carry no
# provenance and trace queries politely report "historical composition
# unavailable" for them. Entries written while it was on keep their frozen
# composition even if it is later switched off.
TRACE_ENABLED = True

BAR_KEYS = (
    "minute_start_utc_ms",
    "open_cents",
    "high_cents",
    "low_cents",
    "close_cents",
    "volume",
    "trade_count",
)


class ConflictError(Exception):
    """Same revision already stored with different content -> HTTP 409."""


class CursorAheadError(Exception):
    """Client cursor is ahead of the committed journal -> HTTP 409."""

    def __init__(self, cursor: int, max_seq: int):
        self.cursor = cursor
        self.max_seq = max_seq
        super().__init__(
            f"cursor {cursor} 超过当前已提交的最大序号 {max_seq}，请重新快照"
        )


def minute_floor(ts_ms: int) -> int:
    return ts_ms - (ts_ms % MINUTE_MS)


def _trade_no_num(trade_no: str) -> Optional[int]:
    """Numeric sort key for all-digit trade numbers (must fit SQLite int64)."""
    if trade_no.isdigit():
        value = int(trade_no)
        if value <= INT64_MAX:
            return value
    return None


def _incoming_content(msg) -> tuple:
    if msg.op == "cancel":
        return ("cancel", None, None, None)
    return (msg.op, msg.trade_time_utc_ms, msg.price_cents, msg.qty)


def _stored_content(row: sqlite3.Row) -> tuple:
    return (row["op"], row["trade_time_utc_ms"], row["price_cents"], row["qty"])


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------

def apply_message(conn: sqlite3.Connection, msg) -> dict[str, Any]:
    """Apply one message. Returns {status, current_revision, affected_minutes}.

    Revision rules per (symbol, trade_no):
      revision <  stored : stale_ignored -- late message, never rolls back state
      revision == stored : identical content -> duplicate (idempotent);
                           different content  -> ConflictError (409)
      revision >  stored : applied (gaps allowed)
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM trades WHERE symbol = ? AND trade_no = ?",
            (msg.symbol, msg.trade_no),
        ).fetchone()

        if row is not None and msg.revision < row["revision"]:
            conn.execute("COMMIT")
            return {
                "status": "stale_ignored",
                "current_revision": row["revision"],
                "affected_minutes": [],
            }

        if row is not None and msg.revision == row["revision"]:
            if _stored_content(row) == _incoming_content(msg):
                conn.execute("COMMIT")
                return {
                    "status": "duplicate",
                    "current_revision": row["revision"],
                    "affected_minutes": [],
                }
            raise ConflictError(
                f"{msg.symbol}/{msg.trade_no} 修订号 {msg.revision} "
                "已存在且内容不同，拒绝写入"
            )

        old_minute: Optional[int] = None
        if row is not None and not row["cancelled"]:
            old_minute = minute_floor(row["trade_time_utc_ms"])

        cancelled = msg.op == "cancel"
        # Snapshot each touched minute's composition BEFORE this message is
        # applied. For a delete this is the only record of what disappeared.
        minutes_touched = sorted(
            {m for m in (old_minute, None if cancelled else minute_floor(msg.trade_time_utc_ms)) if m is not None}
        )
        before_legs = (
            {m: _minute_legs(conn, msg.symbol, m) for m in minutes_touched}
            if TRACE_ENABLED
            else {}
        )
        conn.execute(
            """
            INSERT INTO trades (symbol, trade_no, revision, op,
                                trade_time_utc_ms, price_cents, qty,
                                cancelled, trade_no_num)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, trade_no) DO UPDATE SET
                revision          = excluded.revision,
                op                = excluded.op,
                trade_time_utc_ms = excluded.trade_time_utc_ms,
                price_cents       = excluded.price_cents,
                qty               = excluded.qty,
                cancelled         = excluded.cancelled,
                trade_no_num      = excluded.trade_no_num
            """,
            (
                msg.symbol,
                msg.trade_no,
                msg.revision,
                msg.op,
                None if cancelled else msg.trade_time_utc_ms,
                None if cancelled else msg.price_cents,
                None if cancelled else msg.qty,
                1 if cancelled else 0,
                _trade_no_num(msg.trade_no),
            ),
        )

        new_minute = None if cancelled else minute_floor(msg.trade_time_utc_ms)

        # Trace context for every journal row this commit produces. It is
        # written in the same transaction, so journal and historical
        # composition are never separated by a failure or a restart.
        trace = None
        if TRACE_ENABLED:
            trace = {
                "ingest_id": uuid.uuid4().hex,
                "trade_no": msg.trade_no,
                "revision": msg.revision,
                "op": msg.op,
                "trade_time_utc_ms": None if cancelled else msg.trade_time_utc_ms,
                "price_cents": None if cancelled else msg.price_cents,
                "qty": None if cancelled else msg.qty,
                "cancelled": 1 if cancelled else 0,
                "occurred_utc_ms": int(time.time() * 1000),
            }

        # Cross-minute corrections touch both the old and the new minute;
        # both recomputes (and their journal rows) commit in this transaction.
        affected = []
        for minute in sorted({m for m in (old_minute, new_minute) if m is not None}):
            if recompute_minute(
                conn, msg.symbol, minute, trace=trace, before=before_legs.get(minute)
            ):
                affected.append(minute)

        conn.execute("COMMIT")
        return {
            "status": "applied",
            "current_revision": msg.revision,
            "affected_minutes": affected,
        }
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------------------
# minute bars
# ---------------------------------------------------------------------------

def _trade_order_key(row: sqlite3.Row) -> tuple:
    # Open/close are determined by (trade_time_utc_ms, trade_no); all-digit
    # trade numbers compare numerically, others lexicographically.
    if row["trade_no_num"] is not None:
        no_key = (0, row["trade_no_num"], row["trade_no"])
    else:
        no_key = (1, 0, row["trade_no"])
    return (row["trade_time_utc_ms"], no_key)


def recompute_minute(
    conn: sqlite3.Connection,
    symbol: str,
    minute_ms: int,
    trace: Optional[dict] = None,
    before: Optional[list[dict]] = None,
) -> bool:
    """Rebuild one minute bar from active trades and journal the visible change.

    Returns True if a journal row was written (bar created/updated/deleted).
    Must be called inside the ingest transaction. When `trace` is given, the
    per-trade composition of the minute right after this commit is frozen
    next to the journal row in the same transaction.
    """
    # Full active composition (not only the OHLCV projection) so provenance
    # can record revision per trade, not just price/qty.
    rows = conn.execute(
        """
        SELECT trade_time_utc_ms, trade_no, trade_no_num, revision,
               price_cents, qty
        FROM trades
        WHERE symbol = ? AND cancelled = 0
          AND trade_time_utc_ms >= ? AND trade_time_utc_ms < ?
        """,
        (symbol, minute_ms, minute_ms + MINUTE_MS),
    ).fetchall()

    existing = conn.execute(
        "SELECT * FROM minute_bars WHERE symbol = ? AND minute_start_utc_ms = ?",
        (symbol, minute_ms),
    ).fetchone()

    if not rows:
        # Last trade of the minute disappeared -> delete marker, but only if a
        # bar was ever published for this minute.
        if existing is None:
            return False
        conn.execute(
            "DELETE FROM minute_bars WHERE symbol = ? AND minute_start_utc_ms = ?",
            (symbol, minute_ms),
        )
        _journal(conn, symbol, minute_ms, "delete", None, trace, before=before)
        return True

    ordered = sorted(rows, key=_trade_order_key)
    bar = {
        "minute_start_utc_ms": minute_ms,
        "open_cents": ordered[0]["price_cents"],
        "high_cents": max(r["price_cents"] for r in ordered),
        "low_cents": min(r["price_cents"] for r in ordered),
        "close_cents": ordered[-1]["price_cents"],
        "volume": sum(r["qty"] for r in ordered),
        "trade_count": len(ordered),
    }

    if existing is not None and all(existing[k] == v for k, v in bar.items()):
        return False  # visible state unchanged -> no journal noise

    conn.execute(
        """
        INSERT INTO minute_bars (symbol, minute_start_utc_ms, open_cents, high_cents,
                                 low_cents, close_cents, volume, trade_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, minute_start_utc_ms) DO UPDATE SET
            open_cents  = excluded.open_cents,
            high_cents  = excluded.high_cents,
            low_cents   = excluded.low_cents,
            close_cents = excluded.close_cents,
            volume      = excluded.volume,
            trade_count = excluded.trade_count
        """,
        (
            symbol,
            minute_ms,
            bar["open_cents"],
            bar["high_cents"],
            bar["low_cents"],
            bar["close_cents"],
            bar["volume"],
            bar["trade_count"],
        ),
    )
    _journal(
        conn,
        symbol,
        minute_ms,
        "upsert",
        bar,
        trace,
        before=before,
        composition=[dict(r) for r in ordered],
    )
    return True


def _journal(
    conn: sqlite3.Connection,
    symbol: str,
    minute_ms: int,
    change_type: str,
    bar: Optional[dict],
    trace: Optional[dict] = None,
    before: Optional[list[dict]] = None,
    composition: Optional[list[dict]] = None,
) -> None:
    cur = conn.execute(
        """
        INSERT INTO journal (symbol, minute_start_utc_ms, change_type, bar_json)
        VALUES (?, ?, ?, ?)
        """,
        (symbol, minute_ms, change_type, None if bar is None else json.dumps(bar, sort_keys=True)),
    )
    if trace is None:
        return
    seq = cur.lastrowid
    conn.execute(
        """
        INSERT INTO journal_provenance
            (seq, ingest_id, symbol, trade_no, revision, op, trade_time_utc_ms,
             price_cents, qty, cancelled, occurred_utc_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            seq,
            trace["ingest_id"],
            symbol,
            trace["trade_no"],
            trace["revision"],
            trace["op"],
            trace["trade_time_utc_ms"],
            trace["price_cents"],
            trace["qty"],
            trace["cancelled"],
            trace["occurred_utc_ms"],
        ),
    )
    # 'before' preserves what the minute held just before the commit (the only
    # record of a minute emptied by the change); 'after' is the composition the
    # journal entry describes. Both are copies, never references.
    for phase, legs in (("before", before), ("after", composition)):
        for item in legs or ():
            conn.execute(
                """
                INSERT INTO journal_trades
                    (seq, phase, trade_no, revision, trade_time_utc_ms, price_cents, qty)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    seq,
                    phase,
                    item["trade_no"],
                    item["revision"],
                    item["trade_time_utc_ms"],
                    item["price_cents"],
                    item["qty"],
                ),
            )


# ---------------------------------------------------------------------------
# queries
# ---------------------------------------------------------------------------

def snapshot_bars(conn: sqlite3.Connection, symbol: str) -> dict[str, Any]:
    """First query: all current minute bars plus a cursor consistent with them."""
    conn.execute("BEGIN")
    try:
        cursor = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM journal").fetchone()[0]
        rows = conn.execute(
            "SELECT * FROM minute_bars WHERE symbol = ? ORDER BY minute_start_utc_ms",
            (symbol,),
        ).fetchall()
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return {
        "symbol": symbol,
        "cursor": cursor,
        "bars": [{k: row[k] for k in BAR_KEYS} for row in rows],
    }


def pull_changes(
    conn: sqlite3.Connection, symbol: str, cursor: int, limit: int
) -> dict[str, Any]:
    """Incremental pull: committed changes for one symbol with seq > cursor."""
    conn.execute("BEGIN")
    try:
        max_seq = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM journal").fetchone()[0]
        if cursor > max_seq:
            raise CursorAheadError(cursor, max_seq)
        rows = conn.execute(
            "SELECT * FROM journal WHERE symbol = ? AND seq > ? ORDER BY seq LIMIT ?",
            (symbol, cursor, limit),
        ).fetchall()
        has_more = len(rows) == limit
        # Fewer rows than the limit => we saw everything committed up to max_seq.
        next_cursor = rows[-1]["seq"] if has_more else max_seq
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise

    changes = [
        {
            "seq": r["seq"],
            "minute_start_utc_ms": r["minute_start_utc_ms"],
            "type": r["change_type"],
            "bar": None if r["bar_json"] is None else json.loads(r["bar_json"]),
        }
        for r in rows
    ]
    return {
        "symbol": symbol,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "changes": changes,
    }


def get_trade(conn: sqlite3.Connection, symbol: str, trade_no: str) -> Optional[dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM trades WHERE symbol = ? AND trade_no = ?", (symbol, trade_no)
    ).fetchone()
    if row is None:
        return None
    return {
        "symbol": row["symbol"],
        "trade_no": row["trade_no"],
        "revision": row["revision"],
        "op": row["op"],
        "trade_time_utc_ms": row["trade_time_utc_ms"],
        "price_cents": row["price_cents"],
        "qty": row["qty"],
        "cancelled": bool(row["cancelled"]),
    }


# ---------------------------------------------------------------------------
# provenance / tracing
# ---------------------------------------------------------------------------

def _leg(row: sqlite3.Row | dict) -> dict[str, Any]:
    return {
        "trade_no": row["trade_no"],
        "revision": row["revision"],
        "trade_time_utc_ms": row["trade_time_utc_ms"],
        "price_cents": row["price_cents"],
        "qty": row["qty"],
    }


def _ordered_legs(legs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order legs exactly like bar open/close: (trade_time, trade_no)."""
    return sorted(
        legs,
        key=lambda t: (
            t["trade_time_utc_ms"],
            (0, _trade_no_num(t["trade_no"]), t["trade_no"])
            if _trade_no_num(t["trade_no"]) is not None
            else (1, 0, t["trade_no"]),
        ),
    )


def _price_source(legs: list[dict[str, Any]]) -> dict[str, Optional[str]]:
    """Which trade supplied the open/close of the historical bar."""
    if not legs:
        return {"open_trade_no": None, "close_trade_no": None}
    ordered = _ordered_legs(legs)
    return {"open_trade_no": ordered[0]["trade_no"], "close_trade_no": ordered[-1]["trade_no"]}


def _minute_legs(conn: sqlite3.Connection, symbol: str, minute_ms: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT trade_no, revision, trade_time_utc_ms, price_cents, qty
        FROM trades
        WHERE symbol = ? AND cancelled = 0
          AND trade_time_utc_ms >= ? AND trade_time_utc_ms < ?
        """,
        (symbol, minute_ms, minute_ms + MINUTE_MS),
    ).fetchall()
    return _ordered_legs([_leg(r) for r in rows])


def _trade_status(conn: sqlite3.Connection, symbol: str, trade_no: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM trades WHERE symbol = ? AND trade_no = ?", (symbol, trade_no)
    ).fetchone()


def _diff_minute(
    historical: list[dict[str, Any]],
    current: list[dict[str, Any]],
    statuses: dict[str, sqlite3.Row],
) -> dict[str, Any]:
    """Explain how the minute's composition differs from the frozen snapshot.

    removed  -- in the historical snapshot, not active in the minute now.
                reason=cancelled (cancel record in current state),
                reason=moved_to_other_minute (still active elsewhere).
    appeared -- absent from the historical snapshot, active in the minute now.
    changed  -- same trade number present in both, but a field differs.
    """
    hist = {t["trade_no"]: t for t in historical}
    cur = {t["trade_no"]: t for t in current}

    removed = []
    for leg in historical:
        no = leg["trade_no"]
        if no in cur:
            continue
        status = statuses.get(no)
        if status is not None and status["cancelled"]:
            reason = "cancelled"
        elif status is not None:
            reason = "moved_to_other_minute"
        else:
            reason = "not_found"
        removed.append({"trade_no": no, "reason": reason, "last_known": leg})

    appeared = [leg for leg in current if leg["trade_no"] not in hist]

    changed = []
    for no in sorted(set(hist) & set(cur)):
        h, c = hist[no], cur[no]
        fields = [
            f for f in ("revision", "trade_time_utc_ms", "price_cents", "qty")
            if h[f] != c[f]
        ]
        if fields:
            changed.append({
                "trade_no": no,
                "fields": fields,
                "historical": {f: h[f] for f in fields},
                "current": {f: c[f] for f in fields},
            })

    return {"removed": removed, "appeared": appeared, "changed": changed}


def _trace_change_in_txn(
    conn: sqlite3.Connection,
    symbol: str,
    seq: int,
) -> Optional[dict[str, Any]]:
    """Trace one seq; the caller already holds a read transaction open."""
    journal = conn.execute(
        "SELECT * FROM journal WHERE symbol = ? AND seq = ?", (symbol, seq)
    ).fetchone()
    if journal is None:
        return None

    prov = conn.execute(
        "SELECT * FROM journal_provenance WHERE seq = ?", (seq,)
    ).fetchone()
    if prov is None:
        # Written before tracing was enabled: no frozen snapshot exists, so
        # only the current composition can be shown (never guessed backwards).
        return {
            "symbol": symbol,
            "seq": seq,
            "type": journal["change_type"],
            "minute_start_utc_ms": journal["minute_start_utc_ms"],
            "bar": None if journal["bar_json"] is None else json.loads(journal["bar_json"]),
            "caused_by": None,
            "open_trade_no": None,
            "close_trade_no": None,
            "current_open_trade_no": None,
            "current_close_trade_no": None,
            "historical_trades": None,
            "composition_before_commit": None,
            "composition_after_commit": None,
            "current_trades": _minute_legs(conn, symbol, journal["minute_start_utc_ms"]),
            "diff": None,
            "historical_available": False,
            "related_changes": [],
        }

    # The journal entry describes the state AFTER its commit. For a delete
    # that set is empty, so the frozen "before" snapshot is the meaningful
    # historical composition ("what this change removed").
    after = _ordered_legs([
        _leg(r) for r in conn.execute(
            """
            SELECT trade_no, revision, trade_time_utc_ms, price_cents, qty
            FROM journal_trades WHERE seq = ? AND phase = 'after'
            """,
            (seq,),
        ).fetchall()
    ])
    before = _ordered_legs([
        _leg(r) for r in conn.execute(
            """
            SELECT trade_no, revision, trade_time_utc_ms, price_cents, qty
            FROM journal_trades WHERE seq = ? AND phase = 'before'
            """,
            (seq,),
        ).fetchall()
    ])
    historical = before if journal["change_type"] == "delete" else after
    current = _minute_legs(conn, symbol, journal["minute_start_utc_ms"])

    # Statuses for every trade the diff may need to classify.
    statuses = {}
    for no in {t["trade_no"] for t in historical} | {t["trade_no"] for t in current}:
        row = _trade_status(conn, symbol, no)
        if row is not None:
            statuses[no] = row

    # Sibling journal rows from the same commit. A cross-minute correction
    # writes old-minute + new-minute together; each is traceable on its own
    # and points at the other.
    related = [
        {
            "seq": r["seq"],
            "type": r["change_type"],
            "minute_start_utc_ms": r["minute_start_utc_ms"],
        }
        for r in conn.execute(
            """
            SELECT j.seq, j.change_type, j.minute_start_utc_ms
            FROM journal j JOIN journal_provenance p ON p.seq = j.seq
            WHERE p.ingest_id = ? AND j.seq <> ?
            ORDER BY j.seq
            """,
            (prov["ingest_id"], seq),
        ).fetchall()
    ]

    sources = _price_source(historical)
    current_sources = _price_source(current)
    return {
        "symbol": symbol,
        "seq": seq,
        "type": journal["change_type"],
        "minute_start_utc_ms": journal["minute_start_utc_ms"],
        "bar": None if journal["bar_json"] is None else json.loads(journal["bar_json"]),
        "caused_by": {
            "trade_no": prov["trade_no"],
            "revision": prov["revision"],
            "op": prov["op"],
            "trade_time_utc_ms": prov["trade_time_utc_ms"],
            "price_cents": prov["price_cents"],
            "qty": prov["qty"],
            "cancelled": bool(prov["cancelled"]),
            "occurred_utc_ms": prov["occurred_utc_ms"],
        },
        "open_trade_no": sources["open_trade_no"],
        "close_trade_no": sources["close_trade_no"],
        "current_open_trade_no": current_sources["open_trade_no"],
        "current_close_trade_no": current_sources["close_trade_no"],
        "historical_trades": historical,
        "composition_before_commit": before,
        "composition_after_commit": after,
        "current_trades": current,
        "diff": _diff_minute(historical, current, statuses),
        "historical_available": True,
        "related_changes": related,
    }


def trace_change(
    conn: sqlite3.Connection,
    symbol: str,
    seq: int,
) -> Optional[dict[str, Any]]:
    """Trace one journal entry: what it was at commit and what that minute is now.

    Everything (historical snapshot, current composition, current trade
    statuses) is read inside ONE transaction, so a concurrent correction
    either lands wholly before or wholly after this read. Historical and
    current detail can never come from different commits.
    """
    conn.execute("BEGIN")
    try:
        result = _trace_change_in_txn(conn, symbol, seq)
        conn.execute("COMMIT")
        return result
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def trace_by_trade(
    conn: sqlite3.Connection, symbol: str, trade_no: str, limit: int
) -> dict[str, Any]:
    """All journal entries caused by one trade (newest first), each fully traced."""
    # The whole page (seq list + every entry's historical/current detail) is
    # one read transaction.
    conn.execute("BEGIN")
    try:
        seqs = [
            r["seq"] for r in conn.execute(
                """
                SELECT seq FROM journal_provenance
                WHERE symbol = ? AND trade_no = ?
                ORDER BY seq DESC LIMIT ?
                """,
                (symbol, trade_no, limit),
            ).fetchall()
        ]
        changes = [_trace_change_in_txn(conn, symbol, s) for s in seqs]
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return {"symbol": symbol, "trade_no": trade_no, "changes": changes}
