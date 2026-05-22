# BTC-5M Canary Policy: `state3_ask_brownian_age60_v0`

This is a narrow live canary admission policy for BTC-5M execution/fill-quality measurement. It is not the BTC-1H tau policy; the timing control is a market-age gate measured in seconds since the active 5-minute market start.

The implementation lives in `src/runtime/btc5m_canary_policy.py` and produces structured decision rows. It does not place orders by itself. A runner or execution adapter should call `evaluate_canary_policy(...)`, log the returned row, and submit only rows with `final_decision` equal to `BUY_YES` or `BUY_NO`.

Startup validation is intentionally strict. With `CanaryConfig.from_env(strict=True)`, changing the HMM model id, disabling the HMM gate, changing allowed HMM states, changing the probability model allowlist, disabling the ask filter, changing the market-age gate, or changing the ask interval raises before live trading can start.

## Required Policy Env

- `BTC5M_POLICY_ID=state3_ask_brownian_age60_v0`
- `BTC5M_MIN_EDGE` must be set. The policy fails startup validation if missing.
- `BTC5M_CANARY_STAKE_USD` should be set to a tiny fixed canary stake. If missing, the decision function abstains safely with `missing_stake`.

## Market-Age Gate

- `BTC5M_MIN_ENTRY_AGE_SEC=60`
- `BTC5M_MAX_ENTRY_AGE_SEC=240`
- `BTC5M_SHADOW_MAX_ENTRY_AGE_SEC=300`

Live entries are blocked before 60 seconds and after 240 seconds. Candidates from 240-300 seconds are logged as `SHADOW_ONLY` when every non-age live gate passes.

## HMM Gate

- `BTC5M_HMM_GATE_ENABLED=true`
- `BTC5M_HMM_MODEL_ID=laplace_1m__gaussian_hmm__k4`
- `BTC5M_HMM_ALLOWED_STATES=3`

The caller must provide a causal, previous-only HMM state row. Missing state abstains with `hmm_state_missing`.

Use `select_previous_hmm_state(...)` when selecting from in-memory HMM rows; it ignores future rows and only returns the latest state at or before the decision timestamp for the configured model id.

If a different HMM model is supplied, the policy abstains with `hmm_model_missing`; it does not substitute another HMM.

## Model Gate

- `BTC5M_MODEL_ALLOWLIST=brownian_zero_drift__rv30`
- `BTC5M_MODEL_BLOCKLIST=baseline_50,calibrated_logistic__gbm_rv30,gbm_zero_drift__rv30_no_ito,gbm_winsorized_sigma__w30__z2.5,gbm_blended_sigma__50_30_20`

Only `brownian_zero_drift__rv30` can produce live entries for this canary.

If the required probability model is absent, including a baseline-only input, the policy abstains with `probability_model_missing`. If the configured model identity or probability formula/convention does not match the replay convention, it abstains with `probability_model_mismatch`.

## Ask And Edge Gates

- `BTC5M_ASK_FILTER_ENABLED=true`
- `BTC5M_MIN_ASK=0.30`
- `BTC5M_MAX_ASK=0.47`

The selected executable ask must satisfy `0.30 < ask < 0.47`.

For YES: `edge_yes = model_p_yes - yes_ask`

For NO: `edge_no = model_p_no - no_ask`

The policy selects the side with the larger executable edge and requires `selected_edge >= BTC5M_MIN_EDGE`.

## Quote And Risk Controls

- `BTC5M_QUOTE_MAX_AGE_MS=5000` or `BTC5M_MAX_QUOTE_AGE_MS=5000`
- `BTC5M_ONE_ENTRY_PER_MARKET=true`
- `BTC5M_MAX_OPEN_POSITIONS=1`
- `BTC5M_DAILY_MAX_LOSS_USD`

Valid executable topbook is required. Missing executable asks abstain with `quote_missing`; stale quotes abstain with `quote_stale`. Submitted, pending, accepted, open, filled, partially filled, booked, and recently accepted market entries block same-market re-entry by default.

## Decision Row

The returned row includes policy id, market identifiers, token ids, market start, decision time, market age, required and observed HMM ids, HMM state/pmax, HMM model version/artifact path when provided, required and observed probability model ids, model probabilities, probability model version/artifact path when provided, probability formula/convention fields, a stable config hash, YES/NO asks, selected side/ask/edge, quote timestamp/age, depth fields, all gate booleans, final decision, abstain reason, shadow flag for 240-300 second candidates, order ids, and fill status placeholders.

Use `write_decision_log_row(path, row)` to append JSONL decision rows.

## Model-Parity Check

Before live shadow mode, first build replay-backed live-input rows from compact/research artifacts:

```bash
.venv/bin/python scripts/build_btc5m_canary_rebuilt_inputs.py \
  --replay-path artifacts/market_age_policy_replay/compact_20260423_20260511_state3_ask_age_v1/trade_level_policy_results.parquet \
  --compact-root artifacts/compact_market_recorder/2026-04-23_to_2026-05-11 \
  --predictions-root artifacts/probability_models_5m/compact_overlap_20260423_20260511_predictions \
  --hmm-state-path artifacts/hmm_regime_veto_attribution/compact_20260423_20260511_phase1_v2/trade_level_with_hmm.parquet \
  --output-dir artifacts/btc5m_canary_rebuilt_inputs/state3_ask_brownian_age60_v0 \
  --overwrite
```

The builder uses replay rows only to sample row ids and decision timestamps. It reattaches market start, market age, executable YES/NO asks, `brownian_zero_drift__rv30` probabilities, and `laplace_1m__gaussian_hmm__k4` state from source artifacts with previous-only joins.

Then run the parity harness against the rebuilt inputs:

```bash
.venv/bin/python scripts/check_btc5m_canary_parity.py \
  --replay-path artifacts/market_age_policy_replay/compact_20260423_20260511_state3_ask_age_v1/trade_level_policy_results.parquet \
  --rebuilt-input-path artifacts/btc5m_canary_rebuilt_inputs/state3_ask_brownian_age60_v0/rebuilt_inputs.parquet \
  --output-dir artifacts/btc5m_canary_parity/state3_ask_brownian_age60_v0
```

For a smoke check that only re-evaluates replay rows through the policy evaluator, omit `--rebuilt-input-path`:

```bash
.venv/bin/python scripts/check_btc5m_canary_parity.py \
  --replay-path artifacts/market_age_policy_replay/compact_20260423_20260511_state3_ask_age_v1/trade_level_policy_results.parquet \
  --output-dir artifacts/btc5m_canary_parity/state3_ask_brownian_age60_v0
```

The replay and rebuilt input rows must include the canary decision inputs: market identifiers, market start and decision timestamps, market age, `brownian_zero_drift__rv30` probabilities, `laplace_1m__gaussian_hmm__k4` state/pmax, YES/NO executable asks, selected side/ask/edge, and expected decision fields where available.

Default tolerances:

- `--prob-tol 1e-9`
- `--age-tol-sec 1.0`
- `--ask-tol 1e-9`
- `--edge-tol 1e-9`

The script writes `parity_summary.json`, `parity_diagnostics.csv`, `parity_diagnostics.jsonl`, and `README.txt`. It exits nonzero for HMM state drift, model probability drift beyond tolerance, selected-side drift, final-decision drift, missing artifacts, or any silent substitution of model/convention. Live shadow mode should not be trusted unless `passed=true` and fatal mismatches are zero.
