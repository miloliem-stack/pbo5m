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

Polymarket collateral setup:

- Trading collateral is pUSD on Polygon.
- USDC.e must be wrapped into pUSD through CollateralOnramp before it can fund CLOB buys.
- pUSD must remain in the actual CLOB funder address.
- For `POLY_SIGNATURE_TYPE=3`, that funder is the deposit wallet in `POLY_FUNDER`, not the owner EOA.
- Wrapping, approvals, relayer batches, and `update_balance_allowance` are setup/preflight operations only; they are not executed in the live order hot path.

Manual setup diagnostic:

```bash
.venv/bin/python scripts/setup_polymarket_funder.py \
  --env-file .env \
  --diagnose-only
```

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

On this server, `.env` is the canonical complete runtime env file. It should contain the live endpoints, wallet settings, Brownian strategy settings, CLOB L2 credentials, Polygon RPC, pUSD/CTF addresses, and safety flags in one place.

The committed `.env.btc5m.brownian.*.example` files are optional override templates only. Do not split required live variables across `.env` and another profile unless the specific script invocation intentionally uses that one alternate file with `--env-file` or `BTC5M_ENV_FILE`.

For an alternate test profile, copy an example to a gitignored local file:

```bash
cp .env.btc5m.brownian.paper.example .env.btc5m.brownian.paper.local
cp .env.btc5m.brownian.live.example .env.btc5m.brownian.live.local
```

The `.local` files are ignored by git, but `.env` is the expected server runtime file. Never commit `.env`, `.env.btc5m.brownian.live.local`, or any file containing wallet secrets.

Paper run:

```bash
MAX_RUNTIME_SEC=300 scripts/run_btc5m_brownian_paper.sh
```

Live one-shot run:

```bash
MAX_RUNTIME_SEC=300 scripts/run_btc5m_brownian_live_oneshot.sh
```

The live one-shot script refuses to run if:

- the selected env file is missing
- `POLY_WALLET_PRIVATE_KEY` is missing or still `REPLACE_ME_DO_NOT_COMMIT`
- `BTC5M_EXPECTED_WALLET_ADDRESS` is missing or still `REPLACE_ME_DO_NOT_COMMIT`
- `POLYGON_RPC` is missing
- CLOB L2 credentials are missing
- `BTC5M_BROWNIAN_PAPER_ONLY` is not `false`
- `BTC5M_BROWNIAN_LIVE_ENABLED` is not `true`
- `BTC5M_EXECUTION_MODE` is not `live`
- `BTC5M_LIVE_ONE_SHOT` is not `true`
- `BTC5M_ALLOW_CONTINUOUS_LIVE=true`

The Python runner also supports:

```bash
.venv/bin/python scripts/run_btc5m_canary_live.py \
  --env-file .env \
  --build-live-input \
  --max-runtime-sec 300
```

To intentionally use an alternate profile:

```bash
BTC5M_ENV_FILE=.env.btc5m.brownian.paper.local \
  MAX_RUNTIME_SEC=300 \
  scripts/run_btc5m_brownian_paper.sh
```

Env loading does not override already-set shell variables by default. It prints loaded key names and redacts keys containing `PRIVATE_KEY`, `SECRET`, `TOKEN`, or `PASSWORD`.

Continuous live is blocked by default. To disable one-shot behavior, `BTC5M_ALLOW_CONTINUOUS_LIVE=true` must be explicitly set. This should not be used for first canary runs.

`BTC5M_BROWNIAN_BANKROLL_USD` must reflect the effective canary bankroll, not an aspirational account target. The `$2000` threshold is minimum-order compatibility for a `$5` order at `0.25%` bankroll risk; it does not increase risk above the threshold.

Venue Minimums, Ledger, And Redemption
--------------------------------------

The Brownian strategy still uses FAK market-order semantics for live canary buys:

- `create_market_order` / `createMarketOrder`
- side `BUY`
- amount is dollars to spend
- price is the worst acceptable price
- `postOrder(..., FAK)`

Do not switch this strategy to passive GTC/GTD limit orders without adding heartbeat, cancel, and stale-order handling.

Venue minimum sizing is configured separately from strategy risk:

```bash
BTC5M_BROWNIAN_MIN_MARKET_BUY_NOTIONAL_USD=5
BTC5M_BROWNIAN_MIN_LIMIT_BUY_SIZE_SHARES=5
BTC5M_BROWNIAN_VENUE_MIN_DISCOVERY_MODE=static
```

`BTC5M_BROWNIAN_MIN_MARKET_BUY_NOTIONAL_USD` replaces the old placeholder `BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL` for FAK market buys. If the venue minimum is lowered, the effective small-wallet threshold is recomputed as:

`min_market_buy_notional_usd / max_stake_fraction`

For example, a `$1` venue minimum at `0.25%` max risk gives a `$400` threshold. The strategy still does not round an order above the max stake fraction unless a separate explicit override is added later.

Live order inventory is recorded in SQLite:

```bash
BTC5M_LIVE_LEDGER_DB=state/btc5m_live_ledger.db
```

Tables include live orders, fills, outcome lots, market resolution state, redemption attempts, and redeemed lots. The JSONL execution journal remains the audit trail, while SQLite is the canonical inventory state for fills and redemption.

Filled BUY orders create YES/NO ERC1155 conditional tokens. Winning tokens redeem to pUSD after market resolution; losing tokens have no payout. Redemption is intentionally outside the trading hot path.

Redeemer dry-run:

```bash
.venv/bin/python scripts/run_btc5m_redeemer.py \
  --env-file .env \
  --once \
  --dry-run
```

Check CTF redeem adapter approval:

```bash
.venv/bin/python scripts/setup_polymarket_funder.py \
  --env-file .env \
  --eoa-mode \
  --check-ctf-redeem-adapter-approval
```

Approve the CTF redeem adapter in EOA mode:

```bash
.venv/bin/python scripts/setup_polymarket_funder.py \
  --env-file .env \
  --eoa-mode \
  --approve-ctf-redeem-adapter \
  --yes-i-understand-this-sends-transactions
```

Deposit-wallet/proxy mode requires a relayer wallet-batch approval path. The setup script refuses direct EOA-style CTF approval with `deposit_wallet_ctf_approval_requires_relayer`.

Redeemer loop:

```bash
.venv/bin/python scripts/run_btc5m_redeemer.py \
  --env-file .env \
  --interval-sec 60 \
  --max-runtime-sec 3600
```

Real one-shot redemption:

```bash
.venv/bin/python scripts/run_btc5m_redeemer.py \
  --env-file .env \
  --once \
  --yes-i-understand-this-sends-transactions
```

The redeemer uses the normal pUSD `CtfCollateralAdapter` for BTC up/down binary CTF markets:

- pUSD: `0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`
- CTF: `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`
- CtfCollateralAdapter: `0xAdA100Db00Ca00073811820692005400218FcE1f`
- parent collection id: zero bytes32
- index sets: `[1, 2]`

The adapter calls:

`redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets)`

The ABI selector is pinned in code as `0x01b7037c`; if that ABI check fails, redemption fails closed with `adapter_abi_unverified`.

Redemption requires ledger-confirmed resolution first. The script skips unresolved markets, zero token balances, already submitted/confirmed redemptions, and conditions under retry backoff. It records `redemption_attempts` before sending a transaction, then updates attempts and `redeemed_lots` after the receipt.

After successful redemption, verify the ledger:

```bash
sqlite3 state/btc5m_live_ledger.db \
  "select id, condition_id, status, tx_hash, confirmed_ts from redemption_attempts order by id desc limit 5;"

sqlite3 state/btc5m_live_ledger.db \
  "select condition_id, side, remaining_qty, status from outcome_lots order by id desc limit 10;"

sqlite3 state/btc5m_live_ledger.db \
  "select condition_id, tx_hash, redeemed_pusd_amount, created_ts from redeemed_lots order by id desc limit 5;"
```

Do not delete failed attempts; they are audit history.

Deposit-wallet redemption via relayer is not implemented in this adapter. `POLY_SIGNATURE_TYPE=3` fails closed with `deposit_wallet_redeem_requires_relayer_not_implemented`. Do not enable continuous trading until the ledger/redeemer path has passed several supervised live cycles.
