from pathlib import Path

from scripts import run_btc5m_canary_live as live_runner
from src.runtime.btc5m_brownian_conservative import BrownianConservativeConfig
from src.runtime.btc5m_live_input_builder import BTC5MCanaryLiveInputBuilder, LiveInputBuilderConfig
from src.runtime.btc5m_strategy_router import run_btc5m_strategy_cycle


NOW = "2026-05-24T10:01:30Z"


def cfg(tmp_path: Path, **kwargs):
    base = {
        "enabled": True,
        "paper_only": True,
        "live_enabled": False,
        "decision_log_path": tmp_path / "decision_state.jsonl",
        "validation_log_path": tmp_path / "order_validation.jsonl",
    }
    base.update(kwargs)
    return BrownianConservativeConfig(**base)


def live_input(**overrides):
    payload = {
        "market": {
            "market_id": "m1",
            "condition_id": "c1",
            "slug": "btc-updown",
            "market_start_ts": "2026-05-24T10:00:00Z",
            "market_end_ts": "2026-05-24T10:05:00Z",
            "market_age_seconds": 90,
            "market_age_sec": 90,
            "yes_token_id": "yes-token",
            "no_token_id": "no-token",
            "tradable": True,
            "is_open": True,
        },
        "quote": {
            "valid_topbook": True,
            "quote_ts": NOW,
            "quote_age_ms": 100.0,
            "yes_ask": 0.40,
            "no_ask": 0.90,
            "yes_token_id": "yes-token",
            "no_token_id": "no-token",
            "yes_top10_depth_cap": 1000.0,
            "no_top10_depth_cap": 1000.0,
        },
        "price_state": {"reference_price": 100.0, "current_price": 100.01, "sigma": 0.01},
        "risk_state": {"bankroll": 2000.0, "session_start_bankroll": 2000.0, "day_start_bankroll": 2000.0, "daily_pnl": 0.0},
        "decision_ts": NOW,
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(payload.get(key), dict):
            payload[key].update(value)
        else:
            payload[key] = value
    return payload


def test_strategy_router_calls_old_hmm_path_for_canary_strategy():
    called = []
    result = run_btc5m_strategy_cycle(
        strategy_id="state3_ask_brownian_age60_v0",
        live_input={"x": 1},
        canary_cycle=lambda payload: called.append(payload) or {"event_type": "old_hmm"},
    )
    assert result.route == "hmm_canary"
    assert result.result == {"event_type": "old_hmm"}
    assert called == [{"x": 1}]


def test_strategy_router_calls_brownian_runner_in_paper_mode(tmp_path: Path):
    result = run_btc5m_strategy_cycle(
        strategy_id="brownian_no_hmm_conservative_v1",
        live_input=live_input(),
        brownian_config=cfg(tmp_path),
    )
    assert result.route == "brownian_conservative"
    assert result.result["status"] == "paper_validated"


def test_brownian_paper_mode_never_calls_execution_callback(tmp_path: Path):
    calls = []
    result = run_btc5m_strategy_cycle(
        strategy_id="brownian_no_hmm_conservative_v1",
        live_input=live_input(),
        brownian_config=cfg(tmp_path),
        brownian_execution_callback=lambda request: calls.append(request) or {"event_type": "live_order_submitted"},
    )
    assert result.result["status"] == "paper_validated"
    assert calls == []


def test_brownian_live_disabled_does_not_execute(tmp_path: Path):
    calls = []
    result = run_btc5m_strategy_cycle(
        strategy_id="brownian_no_hmm_conservative_v1",
        live_input=live_input(),
        brownian_config=cfg(tmp_path, paper_only=False, live_enabled=False),
        brownian_execution_callback=lambda request: calls.append(request) or {"event_type": "live_order_submitted"},
    )
    assert result.result["status"] == "validation_rejected"
    assert result.result["reason"] == "live_not_enabled"
    assert calls == []


def test_brownian_live_enabled_calls_existing_execution_callback_once(tmp_path: Path):
    calls = []
    result = run_btc5m_strategy_cycle(
        strategy_id="brownian_no_hmm_conservative_v1",
        live_input=live_input(),
        brownian_config=cfg(tmp_path, paper_only=False, live_enabled=True),
        brownian_execution_callback=lambda request: calls.append(request) or {"event_type": "live_order_submitted", "order_id": "o1"},
    )
    assert result.result["status"] == "submitted_live"
    assert len(calls) == 1
    request = calls[0]
    assert request["strategy_id"] == "brownian_no_hmm_conservative_v1"
    assert request["validation_id"]
    assert "order_intent" not in request


def test_missing_sigma_causes_no_trade_and_no_execution(tmp_path: Path):
    calls = []
    result = run_btc5m_strategy_cycle(
        strategy_id="brownian_no_hmm_conservative_v1",
        live_input=live_input(price_state={"sigma": None}),
        brownian_config=cfg(tmp_path, paper_only=False, live_enabled=True),
        brownian_execution_callback=lambda request: calls.append(request) or {"event_type": "live_order_submitted"},
    )
    assert result.result["status"] == "no_trade"
    assert result.result["reason"] == "missing_or_invalid_sigma"
    assert calls == []


def test_missing_top10_depth_causes_no_execution(tmp_path: Path):
    calls = []
    payload = live_input()
    payload["quote"].pop("yes_top10_depth_cap")
    payload["quote"].pop("no_top10_depth_cap")
    result = run_btc5m_strategy_cycle(
        strategy_id="brownian_no_hmm_conservative_v1",
        live_input=payload,
        brownian_config=cfg(tmp_path, paper_only=False, live_enabled=True),
        brownian_execution_callback=lambda request: calls.append(request) or {"event_type": "live_order_submitted"},
    )
    assert result.result["status"] in {"no_trade", "validation_rejected"}
    assert calls == []


def test_duplicate_and_closed_market_do_not_execute(tmp_path: Path):
    calls = []
    dup = run_btc5m_strategy_cycle(
        strategy_id="brownian_no_hmm_conservative_v1",
        live_input=live_input(risk_state={"already_traded_market": True}),
        brownian_config=cfg(tmp_path, paper_only=False, live_enabled=True),
        brownian_execution_callback=lambda request: calls.append(request) or {"event_type": "live_order_submitted"},
    )
    closed = run_btc5m_strategy_cycle(
        strategy_id="brownian_no_hmm_conservative_v1",
        live_input=live_input(market={"tradable": False}),
        brownian_config=cfg(tmp_path, paper_only=False, live_enabled=True),
        brownian_execution_callback=lambda request: calls.append(request) or {"event_type": "live_order_submitted"},
    )
    assert dup.result["status"] == "no_trade"
    assert closed.result["status"] == "validation_rejected"
    assert calls == []


def test_paper_logs_are_written_in_paper_mode(tmp_path: Path):
    result = run_btc5m_strategy_cycle(
        strategy_id="brownian_no_hmm_conservative_v1",
        live_input=live_input(),
        brownian_config=cfg(tmp_path),
    )
    assert result.result["status"] == "paper_validated"
    assert (tmp_path / "decision_state.jsonl").exists()
    assert (tmp_path / "order_validation.jsonl").exists()


class FakeBuilder:
    payload = live_input()

    def __init__(self, *args, **kwargs):
        pass

    def build(self):
        return {"ok": True, "input": self.payload, "missing_components": [], "missing_input_reason": None}


def args(tmp_path: Path):
    return type(
        "Args",
        (),
        {
            "build_live_input": True,
            "decision_json": None,
            "decision_input_json": None,
            "decision_output_json": tmp_path / "decision.jsonl",
            "live_log_root": tmp_path / "live",
            "max_runtime_sec": 1,
            "poll_interval_sec": 0,
            "stop_after_first_eligible_decision": True,
        },
    )()


def test_server_process_brownian_strategy_is_reachable_in_paper_mode(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("BTC5M_STRATEGY_ID", "brownian_no_hmm_conservative_v1")
    monkeypatch.setenv("BTC5M_BROWNIAN_ENABLED", "true")
    monkeypatch.setenv("BTC5M_BROWNIAN_PAPER_ONLY", "true")
    monkeypatch.setenv("BTC5M_BROWNIAN_DECISION_LOG", str(tmp_path / "decision_state.jsonl"))
    monkeypatch.setenv("BTC5M_BROWNIAN_VALIDATION_LOG", str(tmp_path / "order_validation.jsonl"))
    monkeypatch.setattr(live_runner, "BTC5MCanaryLiveInputBuilder", FakeBuilder)
    result = live_runner.run(args(tmp_path))
    assert result["status"] == "paper_validated"
    assert list((tmp_path / "live").glob("*/*/live_input_state.jsonl"))
    assert list((tmp_path / "live").glob("*/*/decision_state.jsonl"))


def test_brownian_live_input_builder_does_not_require_hmm_when_disabled(tmp_path: Path):
    brownian_path = tmp_path / "brownian.json"
    brownian_path.write_text(
        """{
          "model_id": "brownian_zero_drift__rv30",
          "model_p_yes": 0.55,
          "reference_price": 100.0,
          "current_price": 100.01,
          "rv30": 0.01,
          "probability_convention": "replay-matched brownian_zero_drift__rv30",
          "asof_ts": "2026-05-24T10:01:30Z",
          "generated_ts": "2026-05-24T10:01:30Z"
        }""",
        encoding="utf-8",
    )
    builder = BTC5MCanaryLiveInputBuilder(
        LiveInputBuilderConfig(
            brownian_state_path=brownian_path,
            hmm_state_path=None,
            max_state_age_sec=60,
            require_hmm_state=False,
        ),
        market_fn=lambda: {
            "market": {
                "market_id": "m1",
                "condition_id": "c1",
                "slug": "btc-updown",
                "start_time": "2026-05-24T10:00:00Z",
                "end_time": "2026-05-24T10:05:00Z",
                "token_yes": "yes-token",
                "token_no": "no-token",
                "active": True,
            }
        },
        quote_fn=lambda token: {
            "fetch_ok": True,
            "best_ask": 0.40 if token == "yes-token" else 0.90,
            "ask_size": 1000,
            "fetched_at": "2026-05-24T10:01:30Z",
            "age_seconds": 0,
        },
        now_fn=lambda: __import__("datetime").datetime(2026, 5, 24, 10, 1, 30, tzinfo=__import__("datetime").timezone.utc),
    )
    built = builder.build()
    assert built["ok"] is True
    assert built["missing_components"] == []
    assert built["input"]["hmm_state"] is None
