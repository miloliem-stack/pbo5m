# BTC-5M Canary Server Runbook

This runs `state3_ask_brownian_age60_v0` as an autonomous live loop. Default configuration is observe-only and cannot submit orders.

Warning: live mode can place real Polymarket CLOB buy orders. Use an isolated wallet; $50 is the recommended first canary wallet size. Settlement and redemption are not implemented.

`git pull` is not sufficient by itself. Runtime state under `artifacts/` is gitignored and must be created on the server. The required HMM deploy bundle is external unless it is explicitly copied into a versioned `models/` path later.

## Required Environment

The canary server scripts auto-load `.env` from the repo root. Existing shell environment variables take precedence. To use a different file, set `BTC5M_ENV_FILE=/path/to/file.env` before launching the script.

- `BTC5M_POLICY_ID=state3_ask_brownian_age60_v0`
- `BTC5M_EXECUTION_MODE=observe|live`
- `BTC5M_LIVE_TRADING_ENABLED=false` by default
- `BTC5M_LIVE_ONE_SHOT=true`
- `BTC5M_MAX_ORDER_ATTEMPTS_PER_PROCESS=1`
- `BTC5M_MIN_EDGE`
- `BTC5M_CANARY_STAKE_USD`
- `BTC5M_MAX_NOTIONAL_PER_MARKET_USD`
- `BTC5M_MAX_DAILY_NOTIONAL_USD`
- `BTC5M_MAX_OPEN_POSITIONS=1`
- `BTC5M_ONE_ENTRY_PER_MARKET=true`
- `BTC5M_EXPECTED_WALLET_ADDRESS` recommended in observe, required operationally for live
- `BTC5M_LIVE_HMM_STATE_PATH` pointing at the causal `laplace_1m__gaussian_hmm__k4` live state JSON
- `BTC5M_LIVE_BROWNIAN_STATE_PATH` pointing at the live `brownian_zero_drift__rv30` prediction JSON, or `BTC5M_LIVE_REFERENCE_PRICE` plus `BTC5M_LIVE_RV30`
- `BTC5M_HMM_ARTIFACT_DIR` pointing at the external deploy HMM bundle
- `BTC5M_LIVE_RV30` for Brownian live probability generation
- `BTC5M_LIVE_STATE_MAX_AGE_SEC=15`
- `BTC5M_MAX_QUOTE_AGE_MS=5000`
- `BTC5M_DECISION_EXPIRY_MS=2000`

Live credentials:

- `POLY_WALLET_PRIVATE_KEY`
- optional existing API creds: `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_API_PASSPHRASE`
- optional `POLY_FUNDER`, `POLY_SIGNATURE_TYPE`, `POLYGON_CHAIN_ID`

## Logs

Live loop state:

`artifacts/btc5m_canary_live/YYYY-MM-DD/HH/live_input_state.jsonl`

Policy decisions:

`artifacts/btc5m_canary_live/YYYY-MM-DD/HH/decision_state.jsonl`

Execution events and duplicate-protection journal:

`artifacts/btc5m_canary_execution/YYYY-MM-DD/execution_events.jsonl`

Live state producer outputs:

`artifacts/live_state/btc5m_brownian_prediction.json`

`artifacts/live_state/btc5m_hmm_state.json`

`artifacts/live_state/btc5m_brownian_reference_cache.json`

Inspect latest events:

```bash
tail -n 20 artifacts/btc5m_canary_execution/$(date -u +%F)/execution_events.jsonl
```

## Server Bootstrap

1. Pull the repo and install dependencies.
2. Copy the external HMM deploy bundle to the server, for example:

```bash
mkdir -p /opt/btc5m_models/laplace_1m_gaussian_hmm_k4
```

The bundle must contain:

- `manifest.json`
- the immutable HMM/scaler/model assets referenced by the manifest
- `live_hmm_state_source.json`, produced by the causal HMM scorer on the server

The current repo does not commit that HMM bundle. This is intentional until the asset size/schema is finalized.

3. Create runtime state directories:

```bash
mkdir -p artifacts/live_state artifacts/btc5m_canary_live artifacts/btc5m_canary_execution
```

4. Export the deployment env:

```bash
export BTC5M_HMM_ARTIFACT_DIR=/opt/btc5m_models/laplace_1m_gaussian_hmm_k4
export BTC5M_LIVE_HMM_STATE_PATH=artifacts/live_state/btc5m_hmm_state.json
export BTC5M_LIVE_BROWNIAN_STATE_PATH=artifacts/live_state/btc5m_brownian_prediction.json
export BTC5M_LIVE_RV30=0.01
```

## Live State Producer

Start the producer before the runner:

```bash
BTC5M_HMM_ARTIFACT_DIR=/opt/btc5m_models/laplace_1m_gaussian_hmm_k4 \
BTC5M_LIVE_RV30=0.01 \
BTC5M_LIVE_HMM_STATE_PATH=artifacts/live_state/btc5m_hmm_state.json \
BTC5M_LIVE_BROWNIAN_STATE_PATH=artifacts/live_state/btc5m_brownian_prediction.json \
.venv/bin/python scripts/run_btc5m_live_state_producer.py \
  --live
```

The Brownian producer computes `brownian_zero_drift__rv30` from live Binance price, active 5-minute market metadata, a cached reference price per market, and `BTC5M_LIVE_RV30`. Start it before the target market window begins when possible so the first observed reference price is close to market start. If an exact replay-matched reference source is available, set `BTC5M_LIVE_REFERENCE_PRICE`.

The HMM producer validates and atomically copies the current causal state from `$BTC5M_HMM_ARTIFACT_DIR/live_hmm_state_source.json`. It does not substitute another model and fails in live mode if the HMM bundle/source is missing.

For a single smoke update:

```bash
.venv/bin/python scripts/run_btc5m_live_state_producer.py --live --once
```

## Preflight

Run before observe/live:

```bash
.venv/bin/python scripts/preflight_btc5m_canary_server.py --live
```

Preflight checks Python deps, Polymarket credentials presence, live-state/journal writability, external HMM bundle presence, Binance price access, active market discovery, CLOB quote fetch, and state file freshness/model identity.

## Observe Command

```bash
BTC5M_POLICY_ID=state3_ask_brownian_age60_v0 \
BTC5M_EXECUTION_MODE=observe \
BTC5M_LIVE_TRADING_ENABLED=false \
BTC5M_MIN_EDGE=0.02 \
BTC5M_CANARY_STAKE_USD=5 \
BTC5M_HMM_ARTIFACT_DIR=/opt/btc5m_models/laplace_1m_gaussian_hmm_k4 \
BTC5M_LIVE_HMM_STATE_PATH=artifacts/live_state/btc5m_hmm_state.json \
BTC5M_LIVE_BROWNIAN_STATE_PATH=artifacts/live_state/btc5m_brownian_prediction.json \
.venv/bin/python scripts/run_btc5m_canary_live.py \
  --build-live-input \
  --max-runtime-sec 300
```

Observe mode discovers the active BTC 5-minute market, fetches live YES/NO quotes, attaches the configured Brownian prediction and HMM state, evaluates the policy, and logs decisions. It never submits CLOB orders.

## Live One-Shot Command

```bash
BTC5M_POLICY_ID=state3_ask_brownian_age60_v0 \
BTC5M_EXECUTION_MODE=live \
BTC5M_LIVE_TRADING_ENABLED=true \
BTC5M_LIVE_ONE_SHOT=true \
BTC5M_MAX_ORDER_ATTEMPTS_PER_PROCESS=1 \
BTC5M_MIN_EDGE=0.02 \
BTC5M_CANARY_STAKE_USD=5 \
BTC5M_MAX_NOTIONAL_PER_MARKET_USD=5 \
BTC5M_MAX_DAILY_NOTIONAL_USD=5 \
BTC5M_EXPECTED_WALLET_ADDRESS=0x... \
POLY_WALLET_PRIVATE_KEY=... \
BTC5M_HMM_ARTIFACT_DIR=/opt/btc5m_models/laplace_1m_gaussian_hmm_k4 \
BTC5M_LIVE_HMM_STATE_PATH=artifacts/live_state/btc5m_hmm_state.json \
BTC5M_LIVE_BROWNIAN_STATE_PATH=artifacts/live_state/btc5m_brownian_prediction.json \
.venv/bin/python scripts/run_btc5m_canary_live.py \
  --build-live-input \
  --max-runtime-sec 300
```

Live one-shot mode submits at most one order attempt per process. It exits after a live order attempt is submitted/polled, and the executor journal blocks same-market re-entry after restart.

## Guarded Loop

The same command can be used with a longer `--max-runtime-sec`. Keep `BTC5M_LIVE_ONE_SHOT=true` for the first canary phase. A multi-order loop is intentionally not the default.

## Stopping

If running under `tmux`, stop with `Ctrl-C` in the session. Under `systemd`, use:

```bash
systemctl --user stop btc5m-canary.service
```

Minimal `tmux` launch:

```bash
tmux new -s btc5m-canary
```

Paste the observe or live command inside the session.

## Wallet Check

Set `BTC5M_EXPECTED_WALLET_ADDRESS`. In live mode the runner refuses startup when the authenticated wallet does not match it.

## Intentionally Not Implemented

- Settlement and redemption
- Multi-wallet orchestration
- Kelly sizing
- Edge-scaled sizing
- Final-minute live trading
- Fallback probability models
- Fallback HMM models
