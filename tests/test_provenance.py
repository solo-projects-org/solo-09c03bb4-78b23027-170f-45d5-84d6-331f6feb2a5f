"""Provenance / tracing tests: historical minute composition per change.

Run: python -m pytest tests/ -v
"""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app import logic
from app.main import create_app

T0 = 1_699_999_980_000
T_IN_0 = T0 + 20_000
T_IN_0B = T0 + 30_000
T_IN_1 = T0 + 60_000 + 5_000
M0, M1 = T0, T0 + 60_000


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def msg(symbol="600519", trade_no="1", revision=1, op="trade",
        t=T_IN_0, price=10000, qty=100):
    m = {"symbol": symbol, "trade_no": trade_no, "revision": revision, "op": op}
    if op != "cancel":
        m.update(trade_time_utc_ms=t, price_cents=price, qty=qty)
    return m


def post(client, m):
    r = client.post("/v1/trades", json=m)
    assert r.status_code == 200, r.text
    return r.json()


def trace(client, seq, symbol="600519"):
    r = client.get(f"/v1/symbols/{symbol}/changes/{seq}")
    assert r.status_code == 200, r.text
    return r.json()


def trace_trade(client, trade_no, symbol="600519"):
    r = client.get(f"/v1/symbols/{symbol}/trades/{trade_no}/changes")
    assert r.status_code == 200, r.text
    return r.json()


def journal_seqs(client, symbol="600519"):
    page = client.get(f"/v1/symbols/{symbol}/changes", params={"cursor": 0}).json()
    return [(c["seq"], c["type"], c["minute_start_utc_ms"]) for c in page["changes"]]


# ---------------------------------------------------------------------------
# historical composition & open/close attribution
# ---------------------------------------------------------------------------

def test_trace_returns_historical_composition_and_sources(client):
    post(client, msg(trade_no="1", revision=1, t=T_IN_0, price=10000, qty=100))
    post(client, msg(trade_no="2", revision=1, t=T_IN_0B, price=10100, qty=200))
    seq = journal_seqs(client)[-1][0]

    entry = trace(client, seq)
    assert entry["historical_available"] is True
    assert entry["type"] == "upsert"
    assert [t["trade_no"] for t in entry["historical_trades"]] == ["1", "2"]
    assert entry["open_trade_no"] == "1"
    assert entry["close_trade_no"] == "2"
    assert entry["caused_by"]["trade_no"] == "2"
    assert entry["caused_by"]["revision"] == 1
    assert entry["diff"] == {"removed": [], "appeared": [], "changed": []}


def test_historical_composition_frozen_after_correction(client):
    post(client, msg(trade_no="1", revision=1, t=T_IN_0, price=10000, qty=100))
    seq = journal_seqs(client)[-1][0]
    hist_before = trace(client, seq)["historical_trades"]

    # A later correction changes the current trade; history must not move.
    post(client, msg(trade_no="1", revision=2, op="correction", price=11000, qty=150))

    entry = trace(client, seq)
    assert entry["historical_trades"] == hist_before
    assert entry["historical_trades"][0]["price_cents"] == 10000
    assert entry["current_trades"][0]["price_cents"] == 11000
    # Same trade still present -> shown as changed, not removed/appeared.
    assert entry["diff"]["removed"] == []
    assert entry["diff"]["appeared"] == []
    assert entry["diff"]["changed"] == [{
        "trade_no": "1",
        "fields": ["revision", "price_cents", "qty"],
        "historical": {"revision": 1, "price_cents": 10000, "qty": 100},
        "current": {"revision": 2, "price_cents": 11000, "qty": 150},
    }]
    # Current side re-attributes open/close.
    assert entry["current_open_trade_no"] == "1"


def test_trace_lists_disappeared_and_appeared_trades(client):
    post(client, msg(trade_no="1", revision=1, t=T_IN_0, price=10000, qty=100))
    post(client, msg(trade_no="2", revision=1, t=T_IN_0B, price=10100, qty=200))
    seq = journal_seqs(client)[-1][0]

    # Trade 2 moves to minute 1, trade 3 arrives in minute 0.
    post(client, msg(trade_no="2", revision=2, op="correction", t=T_IN_1))
    post(client, msg(trade_no="3", revision=1, t=T_IN_0 + 1_000, price=10300, qty=300))

    entry = trace(client, seq)
    removed = {r["trade_no"]: r["reason"] for r in entry["diff"]["removed"]}
    appeared = {t["trade_no"] for t in entry["diff"]["appeared"]}
    assert removed == {"2": "moved_to_other_minute"}
    assert appeared == {"3"}
    assert {t["trade_no"] for t in entry["current_trades"]} == {"1", "3"}


def test_cancel_is_confirmed_from_current_state(client):
    post(client, msg(trade_no="1", revision=1, t=T_IN_0, price=10000, qty=100))
    post(client, msg(trade_no="2", revision=1, t=T_IN_0B, price=10100, qty=200))
    seq = journal_seqs(client)[-1][0]

    post(client, msg(trade_no="2", revision=2, op="cancel"))

    entry = trace(client, seq)
    assert entry["diff"]["removed"][0]["trade_no"] == "2"
    assert entry["diff"]["removed"][0]["reason"] == "cancelled"
    # The cancel's own delete/upsert entry is traceable too.
    cancel_seq = journal_seqs(client)[-1][0]
    cancel_entry = trace(client, cancel_seq)
    assert cancel_entry["caused_by"]["op"] == "cancel"
    assert cancel_entry["current_trades"][0]["trade_no"] == "1"


def test_trace_delete_entry_shows_empty_history(client):
    post(client, msg(trade_no="1", revision=1, t=T_IN_0, price=10000, qty=100))
    post(client, msg(trade_no="1", revision=2, op="cancel"))
    seq, change_type, minute = journal_seqs(client)[-1]
    assert change_type == "delete" and minute == M0

    entry = trace(client, seq)
    assert entry["type"] == "delete"
    assert entry["bar"] is None
    # The delete records what was removed, not an empty after-set.
    assert [t["trade_no"] for t in entry["historical_trades"]] == ["1"]
    assert entry["composition_before_commit"] == entry["historical_trades"]
    assert entry["composition_after_commit"] == []
    assert entry["open_trade_no"] == entry["close_trade_no"] == "1"
    assert entry["current_trades"] == []
    assert entry["diff"]["removed"][0]["reason"] == "cancelled"


# ---------------------------------------------------------------------------
# cross-minute corrections: both journal rows traceable
# ---------------------------------------------------------------------------

def test_cross_minute_correction_both_entries_traceable(client):
    post(client, msg(trade_no="1", revision=1, t=T_IN_0, price=10000, qty=100))
    post(client, msg(trade_no="1", revision=2, op="correction", t=T_IN_1, price=10100))
    seqs = journal_seqs(client)
    old_seq = next(s for s, typ, m in seqs if m == M0 and typ == "delete")
    new_seq = next(s for s, typ, m in seqs if m == M1 and typ == "upsert")

    for seq, minute, typ in ((old_seq, M0, "delete"), (new_seq, M1, "upsert")):
        entry = trace(client, seq)
        assert entry["minute_start_utc_ms"] == minute
        assert entry["type"] == typ
        assert entry["caused_by"]["trade_no"] == "1"
        assert entry["caused_by"]["revision"] == 2
        # Each entry points at its sibling from the same commit.
        assert [r["seq"] for r in entry["related_changes"]] == [
            new_seq if seq == old_seq else old_seq
        ]

    # $old-minute delete: history had trade 1, now the minute is empty.
    old_entry = trace(client, old_seq)
    assert [t["trade_no"] for t in old_entry["historical_trades"]] == ["1"]
    assert old_entry["current_trades"] == []
    assert old_entry["diff"]["removed"][0]["reason"] == "moved_to_other_minute"

    # By trade number every entry this trade caused comes back, newest first,
    # each individually traceable (including the original trade commit).
    listed = trace_trade(client, "1")
    assert {c["seq"] for c in listed["changes"]} == {1, old_seq, new_seq}
    assert [c["seq"] for c in listed["changes"]] == [new_seq, old_seq, 1]
    assert all(c["historical_available"] for c in listed["changes"])


def test_same_minute_correction_one_entry_with_both_compositions(client):
    post(client, msg(trade_no="1", revision=1, price=10000, qty=100))
    post(client, msg(trade_no="1", revision=2, op="correction", price=11000, qty=120))
    seq = journal_seqs(client)[-1][0]
    entry = trace(client, seq)
    assert [t["trade_no"] for t in entry["historical_trades"]] == ["1"]
    assert entry["historical_trades"][0]["revision"] == 2
    assert entry["diff"] == {"removed": [], "appeared": [], "changed": []}


def test_trace_by_trade_records_revision_and_op(client):
    post(client, msg(trade_no="7", revision=1, price=10000))
    post(client, msg(trade_no="7", revision=3, op="correction", price=10200))
    listed = trace_trade(client, "7")
    assert [c["caused_by"]["revision"] for c in listed["changes"]] == [3, 1]
    assert [c["caused_by"]["op"] for c in listed["changes"]] == ["correction", "trade"]


def test_duplicate_conflict_and_stale_do_not_create_traces(client):
    post(client, msg(trade_no="1", revision=1))
    assert post(client, msg(trade_no="1", revision=1))["status"] == "duplicate"
    # Same revision, different content: rejected, still no trace.
    assert client.post("/v1/trades", json=msg(trade_no="1", revision=1, price=1)).status_code == 409
    post(client, msg(trade_no="1", revision=2, op="correction", price=10500))
    assert post(client, msg(trade_no="1", revision=1))["status"] == "stale_ignored"
    listed = trace_trade(client, "1")
    assert len(listed["changes"]) == 2  # trade rev1 + correction rev2 only


# ---------------------------------------------------------------------------
# isolation, 404, legacy rows
# ---------------------------------------------------------------------------

def test_trace_is_symbol_isolated_and_404_for_unknown(client):
    post(client, msg(symbol="600519", trade_no="1"))
    seq = journal_seqs(client, "600519")[-1][0]
    assert client.get(f"/v1/symbols/000001/changes/{seq}").status_code == 404
    assert client.get("/v1/symbols/600519/changes/999999").status_code == 404
    assert trace(client, seq, "600519")["symbol"] == "600519"
    assert trace_trade(client, "nope")["changes"] == []


def test_legacy_journal_without_provenance_is_reported_unavailable(client):
    # Simulate an entry written before tracing existed.
    with sqlite3.connect(client.app.state.db_path) as raw:
        raw.execute(
            "INSERT INTO journal (symbol, minute_start_utc_ms, change_type, bar_json)"
            " VALUES ('600519', ?, 'upsert', NULL)",
            (M0,),
        )
        legacy_seq = raw.execute("SELECT MAX(seq) FROM journal").fetchone()[0]
    entry = trace(client, legacy_seq)
    assert entry["historical_available"] is False
    assert entry["historical_trades"] is None and entry["caused_by"] is None
    # Current state is still shown; nothing is invented for history.
    assert entry["current_trades"] == []


# ---------------------------------------------------------------------------
# consistency and durability
# ---------------------------------------------------------------------------

def test_historical_and_current_from_same_snapshot(client):
    post(client, msg(trade_no="1", revision=1, price=10000, qty=100))
    seq = journal_seqs(client)[-1][0]
    post(client, msg(trade_no="1", revision=2, op="correction", price=11000, qty=110))
    entry = trace(client, seq)
    # Historical is the rev-1 commit; current is the rev-2 state. Same read.
    assert entry["historical_trades"][0]["revision"] == 1
    assert entry["current_trades"][0]["revision"] == 2
    assert entry["bar"]["close_cents"] == 10000


def test_rejected_write_leaves_no_provenance(client):
    post(client, msg(trade_no="1", revision=1, price=10000))
    before = journal_seqs(client)
    r = client.post("/v1/trades", json=msg(trade_no="1", revision=1, price=10001))
    assert r.status_code == 409
    assert journal_seqs(client) == before
    # No orphan provenance rows either.
    listed = trace_trade(client, "1")
    assert len(listed["changes"]) == 1


def test_failed_write_keeps_journal_and_provenance_together(client, monkeypatch):
    post(client, msg(trade_no="1", revision=1))
    seq = journal_seqs(client)[-1][0]

    def boom(*args, **kwargs):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(logic, "_journal", boom)
    with TestClient(client.app, raise_server_exceptions=False) as c:
        r = c.post("/v1/trades", json=msg(trade_no="1", revision=2, op="correction",
                                          price=10500))
        assert r.status_code == 500
    monkeypatch.undo()

    # The journal gained nothing and the original trace is intact.
    assert journal_seqs(client)[-1][0] == seq
    entry = trace(client, seq)
    assert entry["historical_available"] is True
    assert entry["historical_trades"][0]["revision"] == 1
    assert entry["current_trades"][0]["revision"] == 1


def test_restart_keeps_provenance_and_cursor(tmp_path):
    db_file = str(tmp_path / "feed.db")
    with TestClient(create_app(db_file)) as c1:
        post(c1, msg(trade_no="1", revision=1, price=10000))
        seq = journal_seqs(c1)[-1][0]
        before = trace(c1, seq)
        cursor = c1.get("/v1/symbols/600519/bars").json()["cursor"]

    with TestClient(create_app(db_file)) as c2:
        after = trace(c2, seq)
        assert after["historical_trades"] == before["historical_trades"]
        assert after["bar"] == before["bar"]
        assert c2.get("/v1/symbols/600519/bars").json()["cursor"] == cursor


def test_trace_endpoints_in_openapi(client):
    schema = client.get("/openapi.json").json()
    paths = schema["paths"]
    assert "/v1/symbols/{symbol}/changes/{seq}" in paths
    assert "/v1/symbols/{symbol}/trades/{trade_no}/changes" in paths


# ---------------------------------------------------------------------------
# read isolation: trace and concurrent correction
# ---------------------------------------------------------------------------

def test_trace_read_snapshot_isolated_from_concurrent_correction(tmp_path):
    from app import db as dbmod

    db_file = str(tmp_path / "iso.db")
    dbmod.init_db(db_file)
    writer = dbmod.connect(db_file)
    reader = dbmod.connect(db_file)

    logic.apply_message(writer, type("M", (), {
        "symbol": "600519", "trade_no": "1", "revision": 1, "op": "trade",
        "trade_time_utc_ms": T_IN_0, "price_cents": 10000, "qty": 100,
    })())
    seq = writer.execute("SELECT MAX(seq) FROM journal").fetchone()[0]

    # Open the trace read transaction and take it halfway through.
    reader.execute("BEGIN")
    first = logic._trace_change_in_txn(reader, "600519", seq)
    assert first["historical_trades"][0]["revision"] == 1

    # A correction commits on another connection while the read txn is open.
    logic.apply_message(writer, type("M", (), {
        "symbol": "600519", "trade_no": "1", "revision": 2, "op": "correction",
        "trade_time_utc_ms": T_IN_0, "price_cents": 11000, "qty": 150,
    })())

    # Same read transaction still sees the pre-correction "current" state, so
    # historical and current detail provably come from one snapshot.
    same_read = logic._trace_change_in_txn(reader, "600519", seq)
    reader.execute("COMMIT")
    assert same_read["current_trades"][0]["revision"] == 1
    assert same_read["diff"]["changed"] == []

    # A fresh read sees the committed correction.
    fresh = logic.trace_change(reader, "600519", seq)
    assert fresh["current_trades"][0]["revision"] == 2
    assert fresh["historical_trades"][0]["revision"] == 1
    writer.close()
    reader.close()


# ---------------------------------------------------------------------------
# feature switch
# ---------------------------------------------------------------------------

def test_trace_disabled_affects_only_new_writes(tmp_path, monkeypatch):
    db_file = str(tmp_path / "switch.db")
    with TestClient(create_app(db_file)) as c:
        post(c, msg(trade_no="1", revision=1))
        old_seq = journal_seqs(c)[-1][0]

        monkeypatch.setattr(logic, "TRACE_ENABLED", False)
        post(c, msg(trade_no="2", revision=1, t=T_IN_0B))
        new_seq = journal_seqs(c)[-1][0]
        monkeypatch.undo()

        old = trace(c, old_seq)
        new = trace(c, new_seq)
        assert old["historical_available"] is True
        assert new["historical_available"] is False
        assert new["historical_trades"] is None and new["caused_by"] is None
        assert [t["trade_no"] for t in new["current_trades"]] == ["1", "2"]
        # Tracing back on keeps working for subsequent writes.
        post(c, msg(trade_no="3", revision=1, t=T_IN_0 + 1_000))
        assert trace(c, journal_seqs(c)[-1][0])["historical_available"] is True


def test_failure_after_journal_insert_rolls_back_both(tmp_path):
    """A crash between the journal insert and the provenance insert must not
    leave a change without its historical composition."""
    from app import db as dbmod

    db_file = str(tmp_path / "atomic.db")
    dbmod.init_db(db_file)
    conn = dbmod.connect(db_file)
    logic.apply_message(conn, type("M", (), {
        "symbol": "600519", "trade_no": "1", "revision": 1, "op": "trade",
        "trade_time_utc_ms": T_IN_0, "price_cents": 10000, "qty": 100,
    })())
    before = conn.execute("SELECT COUNT(*) FROM journal").fetchone()[0]

    # Fail exactly when provenance is written: the journal row of this commit
    # is already INSERTed by then.
    conn.execute("""
        CREATE TRIGGER fail_prov BEFORE INSERT ON journal_provenance
        BEGIN SELECT RAISE(ABORT, 'simulated crash'); END
    """)
    with pytest.raises(sqlite3.DatabaseError):
        logic.apply_message(conn, type("M", (), {
            "symbol": "600519", "trade_no": "2", "revision": 1, "op": "trade",
            "trade_time_utc_ms": T_IN_0B, "price_cents": 10100, "qty": 200,
        })())

    assert conn.execute("SELECT COUNT(*) FROM journal").fetchone()[0] == before
    assert conn.execute("SELECT COUNT(*) FROM journal_provenance").fetchone()[0] == before
    # Minute bar reflects only the committed trade.
    bar = conn.execute("SELECT volume, trade_count FROM minute_bars").fetchone()
    assert (bar["volume"], bar["trade_count"]) == (100, 1)
    conn.close()
