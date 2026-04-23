from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from ..binance_price_feed import BinancePriceFeed
from ..jsonl_writer import JsonlWriter
from ..market_quotes import build_market_quote_row, get_quote_snapshot
from ..market_router_5m import route_btc_5m_market
from ..polymarket_rtds_client import PolymarketRTDSClient
from ..time_utils import isoformat_utc, seconds_since, utc_now
from .market_recorder_segments import (
    SEGMENT_FILES,
    SEGMENT_MANIFEST,
    build_segment_manifest,
    list_market_recorder_segments,
    segment_dir,
    segment_start,
)


@dataclass
class RecorderState:
    active_market: Optional[dict] = None
    last_chainlink_ts: Optional[str] = None
    last_binance_ts: Optional[str] = None
    last_quote_ts: Optional[str] = None
    last_router_ts: Optional[str] = None
    last_quote_error: Optional[str] = None
    quote_source: str = "poll"


@dataclass
class SegmentState:
    start_utc: datetime
    directory: Path
    writers: dict[str, JsonlWriter]
    row_counts: dict[str, int]


class MarketRecorderRuntime:
    def __init__(
        self,
        *,
        output_dir: str | Path = "artifacts/market_recorder",
        router_fn=None,
        asset: Optional[str] = None,
        router_poll_seconds: float = 10.0,
        quote_poll_seconds: float = 2.0,
        heartbeat_seconds: float = 5.0,
        console_log_seconds: float = 5.0,
        now_fn: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.router_fn = router_fn or route_btc_5m_market
        self.asset = asset
        self.router_poll_seconds = max(1.0, float(router_poll_seconds))
        self.quote_poll_seconds = max(0.5, float(quote_poll_seconds))
        self.heartbeat_seconds = max(1.0, float(heartbeat_seconds))
        self.console_log_seconds = max(1.0, float(console_log_seconds))
        self.now_fn = now_fn or utc_now
        self.state = RecorderState()
        self.stop_event = asyncio.Event()
        self.chainlink_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.binance_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.rtds = PolymarketRTDSClient()
        self.binance = BinancePriceFeed()
        self._writers: dict[str, JsonlWriter] = {}
        self._segment_state: Optional[SegmentState] = None

    def run(self, *, duration_seconds: Optional[float] = None) -> dict[str, str]:
        return asyncio.run(self._run(duration_seconds=duration_seconds))

    async def _run(self, *, duration_seconds: Optional[float]) -> dict[str, str]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_active_segment()
        tasks = [
            asyncio.create_task(self.rtds.run(self.chainlink_queue, self.stop_event)),
            asyncio.create_task(self.binance.run(self.binance_queue, self.stop_event)),
            asyncio.create_task(self._router_loop()),
            asyncio.create_task(self._quote_loop()),
            asyncio.create_task(self._queue_drain_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._console_loop()),
        ]
        if duration_seconds is not None:
            tasks.append(asyncio.create_task(self._deadline(duration_seconds)))
        try:
            await asyncio.gather(*tasks)
        finally:
            self.stop_event.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            self._close_active_segment(closed_cleanly=True)
        return self._build_report()

    async def _deadline(self, duration_seconds: float) -> None:
        await asyncio.sleep(max(0.0, float(duration_seconds)))
        self.stop_event.set()

    async def _router_loop(self) -> None:
        last_market_identity = None
        while not self.stop_event.is_set():
            routed = await asyncio.to_thread(self.router_fn)
            self.state.last_router_ts = isoformat_utc(self.now_fn())
            market = routed.get("market")
            market_identity = _market_identity(market)
            market_changed = market_identity != last_market_identity
            self.state.active_market = market
            last_market_identity = market_identity
            self._write_record("meta", self._with_asset({
                "ts": isoformat_utc(self.now_fn()),
                "record_type": "market_route",
                "market_changed": market_changed,
                "market": market,
                "routing_reason": routed.get("routing_reason"),
                "detection_source": routed.get("detection_source"),
                "diagnostics": routed.get("diagnostics"),
            }))
            await asyncio.sleep(self.router_poll_seconds)

    async def _quote_loop(self) -> None:
        while not self.stop_event.is_set():
            market = self.state.active_market
            if not market or not market.get("token_yes") or not market.get("token_no"):
                self._write_record(
                    "quotes",
                    self._with_asset({
                        "ts": isoformat_utc(self.now_fn()),
                        "record_type": "warning",
                        "warning": "no_active_market_for_quote_poll",
                        "market": market,
                    }),
                )
                await asyncio.sleep(self.quote_poll_seconds)
                continue
            try:
                yes_quote, no_quote = await asyncio.gather(
                    asyncio.to_thread(get_quote_snapshot, market["token_yes"], force_refresh=True),
                    asyncio.to_thread(get_quote_snapshot, market["token_no"], force_refresh=True),
                )
                self.state.last_quote_ts = isoformat_utc(self.now_fn())
                row = build_market_quote_row(
                    market,
                    yes_quote,
                    no_quote,
                    source="clob_poll",
                    raw_payload_fragment={
                        "yes_raw": yes_quote.get("raw"),
                        "no_raw": no_quote.get("raw"),
                    },
                )
                row = self._with_asset(row)
                if row["quote_capture_status"] == "failed":
                    self.state.last_quote_error = (
                        f"yes={yes_quote.get('error_kind') or yes_quote.get('error') or 'unknown'}; "
                        f"no={no_quote.get('error_kind') or no_quote.get('error') or 'unknown'}"
                    )
                elif row["quote_capture_status"] == "partial_failure":
                    self.state.last_quote_error = (
                        f"partial_quote_failure yes={yes_quote.get('error_kind') or 'ok'} "
                        f"no={no_quote.get('error_kind') or 'ok'}"
                    )
                else:
                    self.state.last_quote_error = None
                self._write_record("quotes", row)
            except Exception as exc:
                self.state.last_quote_error = str(exc)
                self._write_record(
                    "quotes",
                    self._with_asset({
                        "ts": isoformat_utc(self.now_fn()),
                        "record_type": "warning",
                        "warning": "quote_poll_failed",
                        "market": market,
                        "error": str(exc),
                    }),
                )
            await asyncio.sleep(self.quote_poll_seconds)

    async def _queue_drain_loop(self) -> None:
        while not self.stop_event.is_set():
            drained = False
            try:
                chainlink_row = self.chainlink_queue.get_nowait()
                drained = True
                self.state.last_chainlink_ts = chainlink_row.get("ts")
                self._write_record("chainlink", chainlink_row)
            except asyncio.QueueEmpty:
                pass
            try:
                binance_row = self.binance_queue.get_nowait()
                drained = True
                self.state.last_binance_ts = binance_row.get("ts")
                self._write_record("binance", binance_row)
            except asyncio.QueueEmpty:
                pass
            if not drained:
                await asyncio.sleep(0.1)

    async def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            market = self.state.active_market or {}
            self._write_record("heartbeat", self._with_asset({
                "ts": isoformat_utc(self.now_fn()),
                "record_type": "heartbeat",
                "output_dir": str(self.output_dir),
                "active_market": {
                    "market_id": market.get("market_id"),
                    "slug": market.get("slug"),
                    "title": market.get("title"),
                    "status": market.get("status"),
                    "market_start_time": market.get("start_time"),
                    "market_end_time": market.get("end_time"),
                },
                "freshness_seconds": {
                    "router": seconds_since(self.state.last_router_ts),
                    "chainlink": seconds_since(self.state.last_chainlink_ts),
                    "binance": seconds_since(self.state.last_binance_ts),
                    "quotes": seconds_since(self.state.last_quote_ts),
                },
                "transport_state": {
                    "chainlink_connected": self.rtds.connected,
                    "chainlink_last_error": self.rtds.last_error,
                    "binance_connected": self.binance.connected,
                    "binance_rest_fallback": self.binance.using_rest_fallback,
                    "binance_last_error": self.binance.last_error,
                    "quote_last_error": self.state.last_quote_error,
                },
            }))
            await asyncio.sleep(self.heartbeat_seconds)

    async def _console_loop(self) -> None:
        while not self.stop_event.is_set():
            market = self.state.active_market or {}
            print(
                (
                    f"[recorder] market={market.get('market_id') or 'none'} "
                    f"slug={market.get('slug') or 'n/a'} "
                    f"tick_freshness(chainlink={_fmt_age(self.state.last_chainlink_ts)}, "
                    f"binance={_fmt_age(self.state.last_binance_ts)}) "
                    f"quote_freshness={_fmt_age(self.state.last_quote_ts)} "
                    f"fallback(chainlink_error={self.rtds.last_error or 'none'}, "
                    f"binance_rest={self.binance.using_rest_fallback}, "
                    f"quote_error={self.state.last_quote_error or 'none'}) "
                    f"output_dir={self.output_dir} "
                    f"segment_dir={self._active_segment_dir_str() or 'n/a'}"
                ),
                flush=True,
            )
            await asyncio.sleep(self.console_log_seconds)

    def _with_asset(self, row: dict[str, Any]) -> dict[str, Any]:
        if not self.asset:
            return row
        enriched = dict(row)
        enriched["asset"] = self.asset
        return enriched

    def _write_record(self, kind: str, row: dict[str, Any]) -> None:
        self._ensure_active_segment()
        writer = self._writers[kind]
        writer.write(row)
        if self._segment_state is not None:
            self._segment_state.row_counts[kind] += 1

    def _ensure_active_segment(self) -> None:
        now = self.now_fn()
        current_segment_start = segment_start(now)
        current = self._segment_state
        if current is not None and current.start_utc == current_segment_start:
            return
        if current is not None:
            self._close_active_segment(closed_cleanly=True, segment_end_utc=current_segment_start)
        self._open_segment(current_segment_start)

    def _open_segment(self, segment_start: datetime) -> None:
        current_segment_dir = segment_dir(self.output_dir, segment_start)
        writers = {
            key: JsonlWriter(current_segment_dir / filename)
            for key, filename in SEGMENT_FILES.items()
        }
        self._writers = writers
        self._segment_state = SegmentState(
            start_utc=segment_start,
            directory=current_segment_dir,
            writers=writers,
            row_counts={key: 0 for key in SEGMENT_FILES},
        )

    def _close_active_segment(
        self,
        *,
        closed_cleanly: bool,
        segment_end_utc: Optional[datetime] = None,
    ) -> None:
        segment = self._segment_state
        if segment is None:
            return
        end_utc = segment_end_utc or self.now_fn()
        for writer in segment.writers.values():
            writer.flush()
            writer.close()
        manifest = build_segment_manifest(
            output_dir=self.output_dir,
            segment_start_utc=segment.start_utc,
            segment_end_utc=end_utc,
            closed_cleanly=closed_cleanly,
            row_counts=segment.row_counts,
            asset=self.asset,
        )
        manifest_path = segment.directory / SEGMENT_MANIFEST
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self._writers = {}
        self._segment_state = None

    def _build_report(self) -> dict[str, str]:
        latest = list_market_recorder_segments(self.output_dir)
        report: dict[str, str] = {"output_dir": str(self.output_dir)}
        if latest["closed_segments"]:
            last_closed = latest["closed_segments"][-1]
            report["last_closed_segment_dir"] = last_closed["segment_dir"]
            report["last_closed_manifest_path"] = last_closed["manifest_path"]
        return report

    def _active_segment_dir_str(self) -> Optional[str]:
        if self._segment_state is None:
            return None
        return str(self._segment_state.directory)


def _fmt_age(value: Optional[str]) -> str:
    age = seconds_since(value)
    if age is None:
        return "n/a"
    return f"{age:.1f}s"


def _market_identity(market: Optional[dict]) -> Optional[tuple[Optional[str], ...]]:
    if not market:
        return None
    return (
        market.get("market_id"),
        market.get("slug"),
        market.get("condition_id"),
        market.get("token_yes"),
        market.get("token_no"),
        market.get("start_time"),
        market.get("end_time"),
    )
