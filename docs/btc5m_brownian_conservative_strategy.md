BTC-5m Brownian Conservative Strategy
====================================

`brownian_no_hmm_conservative_v1` is the first coded no-HMM BTC-5m Brownian strategy module.

Implementation:

- `src/runtime/btc5m_brownian_conservative.py`
- Decision log: `artifacts/live_strategy_decisions/brownian_no_hmm_conservative_v1/decision_state.jsonl`

Defaults are paper/shadow only. The module produces a decision row and, when accepted, an order-intent dictionary. It does not bypass the existing execution ledger, reservation, or order tracking.

Environment
-----------

- `BTC5M_STRATEGY_ID=brownian_no_hmm_conservative_v1`
- `BTC5M_BROWNIAN_ENABLED=false`
- `BTC5M_BROWNIAN_PAPER_ONLY=true`
- `BTC5M_BROWNIAN_LIVE_ENABLED=false`
- `BTC5M_BROWNIAN_MIN_MARKET_AGE_SECONDS=60`
- `BTC5M_BROWNIAN_MAX_MARKET_AGE_SECONDS=240`
- `BTC5M_BROWNIAN_EDGE_THRESHOLD=0.02`
- `BTC5M_BROWNIAN_MIN_ASK=0.30`
- `BTC5M_BROWNIAN_PROBABILITY_HAIRCUT_ABS=0.02`
- `BTC5M_BROWNIAN_ASK_SLIPPAGE_ABS=0.01`
- `BTC5M_BROWNIAN_KELLY_MULTIPLIER=0.025`
- `BTC5M_BROWNIAN_MAX_STAKE_FRACTION=0.0025`
- `BTC5M_BROWNIAN_SMALL_WALLET_THRESHOLD=2000`
- `BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL=5`
- `BTC5M_BROWNIAN_TOP_N_LEVELS=10`
- `BTC5M_BROWNIAN_MAX_DECISION_STALENESS_SECONDS=3.0`
- `BTC5M_BROWNIAN_MAX_DEPTH_UTILIZATION=1.0`
- `BTC5M_BROWNIAN_BANKROLL_USD`
- `BTC5M_BROWNIAN_SESSION_START_BANKROLL_USD`
- `BTC5M_BROWNIAN_DAY_START_BANKROLL_USD`
- `BTC5M_BROWNIAN_DAILY_PNL_USD`

Paper mode requires `BTC5M_BROWNIAN_ENABLED=true` and keeps `BTC5M_BROWNIAN_PAPER_ONLY=true`.

Live validation requires all of:

- `BTC5M_BROWNIAN_ENABLED=true`
- `BTC5M_BROWNIAN_PAPER_ONLY=false`
- `BTC5M_BROWNIAN_LIVE_ENABLED=true`

Live execution should pass validator-accepted order intents through the existing execution layer. Do not route this module directly to venue APIs.

Execution Validator
-------------------

The strategy module produces decisions and paper order intents. The policy-specific bridge to execution is:

- `src/runtime/btc5m_brownian_order_validator.py`
- Validation log: `artifacts/live_strategy_decisions/brownian_no_hmm_conservative_v1/order_validation.jsonl`

The validator is the only approved bridge from `brownian_no_hmm_conservative_v1` order intents toward execution. It is deliberately separate from the older HMM canary executor, which enforces `state3_ask_brownian_age60_v0` identity.

In paper mode, the validator can accept an intent as `paper_validated` by returning `executable_live=false`; it does not submit an order. In live mode, `BTC5M_BROWNIAN_LIVE_ENABLED=true` and `BTC5M_BROWNIAN_PAPER_ONLY=false` are both required before the normalized intent can be marked `executable_live=true`.

The validator rechecks risk-critical fields against the current runtime snapshot:

- strategy/model identity
- market identity and YES/NO token mapping
- duplicate-market status
- decision staleness and current market age
- current executable ask versus intended ask plus slippage
- current edge after quote refresh
- expected log growth after probability haircut and ask slippage
- bankroll, stake fraction, $2000 minimum-order compatibility threshold
- top-10 executable depth cap and depth utilization

The $2000 threshold is a minimum-order compatibility threshold. It does not increase risk in v1; the maximum stake fraction remains 0.25% below and above $2000.

Runner / Execution Adapter
--------------------------

The Brownian runner is:

- `src/runtime/btc5m_brownian_runner.py`
- Paper intent log: `artifacts/live_strategy_decisions/brownian_no_hmm_conservative_v1/paper_order_intents.jsonl`

The runner flow is:

1. Build the strategy inputs from the current market, quote, BTC price, volatility, and risk snapshot.
2. Call `decide_brownian_conservative(...)`.
3. Write the decision row.
4. If the strategy rejects, return `no_trade`.
5. If the strategy accepts, call `validate_brownian_order_intent(...)` against the current market snapshot.
6. Write the validation row.
7. In paper mode, write a paper validated order intent and return `paper_validated`.
8. In live mode, pass only the normalized validated intent to the configured execution callback.

The runner is the only component that should bridge this strategy to execution. It does not call venue APIs directly and does not create a separate ledger, reservation, inventory, or order-tracking system. The execution callback must route the normalized request into the existing execution/ledger path.

The older HMM canary executor remains intentionally separate because it enforces `state3_ask_brownian_age60_v0` provenance and HMM identity.

Server Integration
------------------

The server entrypoint `scripts/run_btc5m_canary_live.py` now dispatches by strategy identity:

- `BTC5M_STRATEGY_ID=brownian_no_hmm_conservative_v1` routes to the Brownian conservative runner.
- `BTC5M_POLICY_ID=state3_ask_brownian_age60_v0` or no Brownian strategy id keeps the old HMM canary path.

For Brownian mode the server uses `--build-live-input` to discover the active BTC 5-minute market, fetch live YES/NO quotes, attach Brownian state, and pass the resulting payload into `run_brownian_conservative_cycle(...)`.

Until wallet ledger balance is wired into the live input builder, set `BTC5M_BROWNIAN_BANKROLL_USD` to the effective bankroll the strategy may risk. If it is missing, the strategy fails closed through bankroll/min-order sizing checks.

Paper mode:

- Requires `BTC5M_BROWNIAN_ENABLED=true`
- Keeps `BTC5M_BROWNIAN_PAPER_ONLY=true`
- Writes decision, validation, server live-input, and paper intent logs
- Never submits a venue order

Live mode:

- Requires `BTC5M_BROWNIAN_ENABLED=true`
- Requires `BTC5M_BROWNIAN_PAPER_ONLY=false`
- Requires `BTC5M_BROWNIAN_LIVE_ENABLED=true`
- Requires `BTC5M_EXECUTION_MODE=live` before the existing execution route can submit

In live mode the runner passes only the normalized validator output into the existing execution journal/order lifecycle route. The route preserves duplicate-journal protection, one-shot order limiting, CLOB adapter submission, order polling, and execution event logging. The Brownian path does not send raw venue orders directly.

Live execution requires the Polymarket CLOB V2 Python SDK, `py-clob-client-v2`. The shared execution adapter refuses legacy V1 `py-clob-client` wiring in live mode because production CLOB V2 rejects V1 signed orders with `order_version_mismatch`.

Live execution also requires CLOB L2 credentials in env. The normal canary runner does not create API keys implicitly. If `POLY_API_KEY`, `POLY_API_SECRET`, and `POLY_API_PASSPHRASE` are missing, live startup fails with `missing_clob_l2_credentials`.

Signature type guidance:

- `POLY_SIGNATURE_TYPE=0`: EOA flow, funder defaults to wallet address.
- `POLY_SIGNATURE_TYPE=3`: POLY_1271 deposit-wallet flow, requires `POLY_FUNDER=<deposit wallet address>`.

`Could not create api key` means the process failed during L2 credential bootstrap before any order submission. Use pre-created L2 credentials for live canary runs.

Before another supervised one-shot after a CLOB SDK change, run:

```bash
.venv/bin/python scripts/diagnose_btc5m_clob_sdk.py --env-file .env
```

If `order_version_mismatch` appears in the execution journal, stop live attempts, upgrade/migrate the SDK, run a paper smoke test, then run exactly one supervised live one-shot again.

One-time auth diagnostic/bootstrap, when intentionally onboarding credentials:

```bash
.venv/bin/python scripts/diagnose_polymarket_clob_auth.py \
  --env-file .env \
  --bootstrap-api-key \
  --output-file .env.clob_l2.local
```

Expected warmup/no-trade reasons include:

- `missing_or_invalid_sigma` when `brownian_zero_drift__rv30` volatility input is unavailable
- `invalid_topbook` when either side quote is unavailable
- `insufficient_depth` or `missing_depth` when top-10 executable depth is unavailable
- `market_too_young` before 60 seconds
- `market_too_old` at or after 240 seconds

Server logs:

- Live inputs: `artifacts/btc5m_canary_live/YYYY-MM-DD/HH/live_input_state.jsonl`
- Server decisions/results: `artifacts/btc5m_canary_live/YYYY-MM-DD/HH/decision_state.jsonl`
- Strategy decisions: `artifacts/live_strategy_decisions/brownian_no_hmm_conservative_v1/decision_state.jsonl`
- Validation: `artifacts/live_strategy_decisions/brownian_no_hmm_conservative_v1/order_validation.jsonl`
- Paper intents: `artifacts/live_strategy_decisions/brownian_no_hmm_conservative_v1/paper_order_intents.jsonl`

Model Convention
----------------

The Brownian probability formula mirrors `scripts/sweep_probability_models_5m.py:brownian_probability`:

`p_yes = normal_cdf(log(current_price / reference_price) / (rv30 * sqrt(tau_minutes)))`

TODO: move this formula into a shared probability-model library so live and research cannot drift.

Environment Profiles
--------------------

Use the committed examples as templates and keep local files out of git:

```bash
cp .env.btc5m.brownian.paper.example .env.btc5m.brownian.paper.local
cp .env.btc5m.brownian.live.example .env.btc5m.brownian.live.local
```

Paper profile:

- `.env.btc5m.brownian.paper.example`
- `.env.btc5m.brownian.paper.local`

Live one-shot profile:

- `.env.btc5m.brownian.live.example`
- `.env.btc5m.brownian.live.local`

The `.local` files are ignored by git. Never commit `.env.btc5m.brownian.live.local`; it contains wallet secrets.

Paper run:

```bash
MAX_RUNTIME_SEC=300 scripts/run_btc5m_brownian_paper.sh
```

Live one-shot run:

```bash
MAX_RUNTIME_SEC=300 scripts/run_btc5m_brownian_live_oneshot.sh
```

The live one-shot script refuses to run if:

- `.env.btc5m.brownian.live.local` is missing
- `POLY_WALLET_PRIVATE_KEY` is missing or still `REPLACE_ME_DO_NOT_COMMIT`
- `BTC5M_EXPECTED_WALLET_ADDRESS` is missing or still `REPLACE_ME_DO_NOT_COMMIT`
- `BTC5M_BROWNIAN_PAPER_ONLY` is not `false`
- `BTC5M_BROWNIAN_LIVE_ENABLED` is not `true`
- `BTC5M_EXECUTION_MODE` is not `live`
- `BTC5M_LIVE_ONE_SHOT` is not `true`

The Python runner also supports:

```bash
.venv/bin/python scripts/run_btc5m_canary_live.py \
  --env-file .env.btc5m.brownian.paper.local \
  --build-live-input \
  --max-runtime-sec 300
```

Env loading does not override already-set shell variables by default. It prints loaded key names and redacts keys containing `PRIVATE_KEY`, `SECRET`, `TOKEN`, or `PASSWORD`.

Continuous live is blocked by default. To disable one-shot behavior, `BTC5M_ALLOW_CONTINUOUS_LIVE=true` must be explicitly set. This should not be used for first canary runs.

`BTC5M_BROWNIAN_BANKROLL_USD` must reflect the effective canary bankroll, not an aspirational account target. The `$2000` threshold is minimum-order compatibility for a `$5` order at `0.25%` bankroll risk; it does not increase risk above the threshold.
