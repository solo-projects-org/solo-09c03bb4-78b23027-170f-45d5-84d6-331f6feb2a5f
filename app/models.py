"""Pydantic request/response models (also drive the Swagger schema)."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

INT64_MAX = 9_223_372_036_854_775_807

Op = Literal["trade", "correction", "cancel"]


class TradeMessage(BaseModel):
    """One exchange message: a trade, a correction, or a cancellation.

    Keyed by (symbol, trade_no); `revision` must increase per key.
    For op=trade/correction the payload fields are required; for op=cancel
    they are ignored.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "symbol": "600519",
                    "trade_no": "000123",
                    "revision": 1,
                    "op": "trade",
                    "trade_time_utc_ms": 1700000000000,
                    "price_cents": 168500,
                    "qty": 100,
                }
            ]
        }
    )

    symbol: str = Field(min_length=1, max_length=32, description="股票代码")
    trade_no: str = Field(min_length=1, max_length=64, description="交易号（交易所逐笔编号）")
    revision: int = Field(ge=1, le=INT64_MAX, description="修订号，同一 (symbol, trade_no) 上递增")
    op: Op = Field(description="trade=成交, correction=更正, cancel=撤销")
    trade_time_utc_ms: Optional[int] = Field(default=None, ge=0, le=INT64_MAX, description="成交时刻，UTC 毫秒")
    price_cents: Optional[int] = Field(default=None, gt=0, le=INT64_MAX, description="价格，整数，单位为分")
    qty: Optional[int] = Field(default=None, gt=0, le=INT64_MAX, description="成交股数")

    @model_validator(mode="after")
    def _payload_required(self) -> "TradeMessage":
        if self.op in ("trade", "correction"):
            missing = [
                name
                for name in ("trade_time_utc_ms", "price_cents", "qty")
                if getattr(self, name) is None
            ]
            if missing:
                raise ValueError(f"op={self.op} 缺少字段: {', '.join(missing)}")
        return self


class IngestResult(BaseModel):
    status: Literal["applied", "duplicate", "stale_ignored"] = Field(
        description="applied=已受理; duplicate=同修订同内容重送，幂等忽略; stale_ignored=迟到旧修订，已忽略"
    )
    symbol: str
    trade_no: str
    revision: int = Field(description="本条消息的修订号")
    current_revision: int = Field(description="该 (symbol, trade_no) 当前存储的修订号")
    affected_minutes: list[int] = Field(description="本次写入产生变更日志的分钟（UTC 毫秒分钟起点）")


class Bar(BaseModel):
    minute_start_utc_ms: int = Field(description="分钟起点，UTC 毫秒")
    open_cents: int = Field(description="开盘价（分），按 (成交时刻, 交易号) 第一笔")
    high_cents: int = Field(description="最高价（分）")
    low_cents: int = Field(description="最低价（分）")
    close_cents: int = Field(description="收盘价（分），按 (成交时刻, 交易号) 最后一笔")
    volume: int = Field(description="总股数")
    trade_count: int = Field(description="笔数")


class BarsSnapshot(BaseModel):
    symbol: str
    cursor: int = Field(description="游标：快照一致点的 journal 最大 seq，后续用 /changes?cursor= 续拉")
    bars: list[Bar]


class MinuteChange(BaseModel):
    seq: int
    minute_start_utc_ms: int
    type: Literal["upsert", "delete"] = Field(description="upsert=分钟柱新值; delete=该分钟已无任何成交")
    bar: Optional[Bar] = Field(description="type=delete 时为 null（删除标记）")


class ChangesPage(BaseModel):
    symbol: str
    next_cursor: int = Field(description="下次续拉使用的游标")
    has_more: bool = Field(description="是否可能还有更多（本页达到 limit）")
    changes: list[MinuteChange]


class TradeState(BaseModel):
    symbol: str
    trade_no: str
    revision: int
    op: Op
    trade_time_utc_ms: Optional[int]
    price_cents: Optional[int]
    qty: Optional[int]
    cancelled: bool


# ---------------------------------------------------------------------------
# provenance / tracing
# ---------------------------------------------------------------------------

class TraceLeg(BaseModel):
    """One trade's frozen (historical) or live (current) contribution to a minute."""

    trade_no: str
    revision: int
    trade_time_utc_ms: int
    price_cents: int
    qty: int


class TraceCause(BaseModel):
    """The message whose commit produced this journal entry."""

    trade_no: str
    revision: int
    op: Op
    trade_time_utc_ms: Optional[int] = Field(description="该次提交后这笔成交的有效时刻（cancel 为 null）")
    price_cents: Optional[int]
    qty: Optional[int]
    cancelled: bool
    occurred_utc_ms: int = Field(description="服务端提交时刻，UTC 毫秒")


class RemovedTrade(BaseModel):
    trade_no: str
    reason: Literal["cancelled", "moved_to_other_minute", "not_found"] = Field(
        description="cancelled=当前状态已撤销; moved_to_other_minute=仍有效但已改到别的分钟; not_found=当前状态查无此笔"
    )
    last_known: TraceLeg


class ChangedTrade(BaseModel):
    trade_no: str
    fields: list[Literal["revision", "trade_time_utc_ms", "price_cents", "qty"]]
    historical: dict[str, int]
    current: dict[str, int]


class MinuteDiff(BaseModel):
    removed: list[RemovedTrade] = Field(description="历史构成里有、现在该分钟已不在的成交")
    appeared: list[TraceLeg] = Field(description="历史构成里没有、现在该分钟出现的成交")
    changed: list[ChangedTrade] = Field(description="同一笔仍在，但修订/时刻/价格/股数被改过")


class RelatedChange(BaseModel):
    seq: int
    type: Literal["upsert", "delete"]
    minute_start_utc_ms: int


class TraceEntry(BaseModel):
    symbol: str
    seq: int = Field(description="变更序号（journal seq）")
    type: Literal["upsert", "delete"] = Field(description="该次变更对分钟柱的影响类型")
    minute_start_utc_ms: int
    bar: Optional[Bar] = Field(description="该次变更提交时的分钟柱；delete 为 null")
    caused_by: Optional[TraceCause] = Field(
        description="产生该变更的成交消息；本功能启用前写入的老变更没有溯源信息，为 null"
    )
    open_trade_no: Optional[str] = Field(description="历史构成中开盘价来源交易号")
    close_trade_no: Optional[str] = Field(description="历史构成中收盘价来源交易号")
    current_open_trade_no: Optional[str] = Field(description="现行构成中开盘价来源交易号（该分钟已清空则 null）")
    current_close_trade_no: Optional[str] = Field(description="现行构成中收盘价来源交易号")
    historical_trades: Optional[list[TraceLeg]] = Field(
        description="提交当时该分钟的有效逐笔构成（冻结副本，不随现行逐笔变化）。"
        "upsert 为提交后构成；delete 为提交前构成（即被删除的那些成交）"
    )
    composition_before_commit: Optional[list[TraceLeg]] = Field(
        description="该次提交前该分钟的有效逐笔构成（冻结副本）"
    )
    composition_after_commit: Optional[list[TraceLeg]] = Field(
        description="该次提交后该分钟的有效逐笔构成（冻结副本；delete 为空列表）"
    )
    current_trades: list[TraceLeg] = Field(description="同一读取事务内该分钟现行的有效逐笔构成")
    diff: Optional[MinuteDiff] = Field(description="历史 → 现行的消失/出现/改值；历史不可用时为 null")
    historical_available: bool = Field(description="本变更是否留存了历史构成")
    related_changes: list[RelatedChange] = Field(
        description="同一次提交产生的其他变更（跨分钟更正会有旧分钟与新分钟两条，各自可按 seq 追查）"
    )


class TraceList(BaseModel):
    symbol: str
    trade_no: str
    changes: list[TraceEntry] = Field(description="该交易号触发的全部变更，按 seq 倒序")

