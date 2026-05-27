#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${BTC5M_ENV_FILE:-.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "refusing live run: env file does not exist: $ENV_FILE" >&2
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
[[ "${POLYGON_RPC:-}" != "" ]] || fail "POLYGON_RPC missing"
[[ "${POLY_API_KEY:-}" != "" ]] || fail "POLY_API_KEY missing"
[[ "${POLY_API_KEY:-}" != "REPLACE_ME_DO_NOT_COMMIT" ]] || fail "POLY_API_KEY is still placeholder"
[[ "${POLY_API_SECRET:-}" != "" ]] || fail "POLY_API_SECRET missing"
[[ "${POLY_API_SECRET:-}" != "REPLACE_ME_DO_NOT_COMMIT" ]] || fail "POLY_API_SECRET is still placeholder"
[[ "${POLY_API_PASSPHRASE:-}" != "" ]] || fail "POLY_API_PASSPHRASE missing"
[[ "${POLY_API_PASSPHRASE:-}" != "REPLACE_ME_DO_NOT_COMMIT" ]] || fail "POLY_API_PASSPHRASE is still placeholder"
[[ "${BTC5M_STRATEGY_ID:-}" == "brownian_no_hmm_conservative_v1" ]] || fail "BTC5M_STRATEGY_ID must be brownian_no_hmm_conservative_v1"
[[ "${BTC5M_BROWNIAN_PAPER_ONLY:-}" == "false" ]] || fail "BTC5M_BROWNIAN_PAPER_ONLY must be false"
[[ "${BTC5M_BROWNIAN_LIVE_ENABLED:-}" == "true" ]] || fail "BTC5M_BROWNIAN_LIVE_ENABLED must be true"
[[ "${BTC5M_EXECUTION_MODE:-}" == "live" ]] || fail "BTC5M_EXECUTION_MODE must be live"
[[ "${BTC5M_LIVE_ONE_SHOT:-}" == "true" ]] || fail "BTC5M_LIVE_ONE_SHOT must be true"
[[ "${BTC5M_ALLOW_CONTINUOUS_LIVE:-false}" != "true" ]] || fail "BTC5M_ALLOW_CONTINUOUS_LIVE must not be true"

exec .venv/bin/python scripts/run_btc5m_canary_live.py \
  --env-file "$ENV_FILE" \
  --build-live-input \
  --max-runtime-sec "${MAX_RUNTIME_SEC:-300}"
