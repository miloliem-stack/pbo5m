#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=".env.btc5m.brownian.live.local"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "refusing live run: $ENV_FILE is required; do not use the example file for live trading" >&2
  exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

fail() {
  echo "refusing live run: $1" >&2
  exit 2
}

[[ "${POLY_WALLET_PRIVATE_KEY:-}" != "" ]] || fail "POLY_WALLET_PRIVATE_KEY missing"
[[ "${POLY_WALLET_PRIVATE_KEY:-}" != "REPLACE_ME_DO_NOT_COMMIT" ]] || fail "POLY_WALLET_PRIVATE_KEY is still placeholder"
[[ "${BTC5M_EXPECTED_WALLET_ADDRESS:-}" != "" ]] || fail "BTC5M_EXPECTED_WALLET_ADDRESS missing"
[[ "${BTC5M_EXPECTED_WALLET_ADDRESS:-}" != "REPLACE_ME_DO_NOT_COMMIT" ]] || fail "BTC5M_EXPECTED_WALLET_ADDRESS is still placeholder"
[[ "${BTC5M_BROWNIAN_PAPER_ONLY:-}" == "false" ]] || fail "BTC5M_BROWNIAN_PAPER_ONLY must be false"
[[ "${BTC5M_BROWNIAN_LIVE_ENABLED:-}" == "true" ]] || fail "BTC5M_BROWNIAN_LIVE_ENABLED must be true"
[[ "${BTC5M_EXECUTION_MODE:-}" == "live" ]] || fail "BTC5M_EXECUTION_MODE must be live"
[[ "${BTC5M_LIVE_ONE_SHOT:-}" == "true" ]] || fail "BTC5M_LIVE_ONE_SHOT must be true"

exec .venv/bin/python scripts/run_btc5m_canary_live.py \
  --env-file "$ENV_FILE" \
  --build-live-input \
  --max-runtime-sec "${MAX_RUNTIME_SEC:-300}"
