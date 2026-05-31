#!/usr/bin/env bash
set -euo pipefail

stage() {
  local event_type="$1"
  shift || true
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  local json="{\"timestamp_utc\":\"$ts\",\"event_type\":\"$event_type\",\"pid\":$$"
  while [[ $# -gt 0 ]]; do
    local key="$1"
    local value="${2-}"
    key="${key//\"/}"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    json+=" ,\"$key\":\"$value\""
    shift 2 || true
  done
  json+="}"
  echo "$json" >&2
}

stage "wrapper_start"

ENV_FILE="${BTC5M_ENV_FILE:-.env}"
stage "env_file_selected" env_file "$ENV_FILE"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "refusing live run: env file does not exist: $ENV_FILE" >&2
  exit 2
fi

OPERATOR_MAX_RUNTIME_SEC="${MAX_RUNTIME_SEC:-}"
OPERATOR_CANARY_TICK_SEC="${BTC5M_CANARY_TICK_SEC:-}"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

if [[ -n "$OPERATOR_MAX_RUNTIME_SEC" ]]; then
  MAX_RUNTIME_SEC="$OPERATOR_MAX_RUNTIME_SEC"
fi
if [[ -n "$OPERATOR_CANARY_TICK_SEC" ]]; then
  BTC5M_CANARY_TICK_SEC="$OPERATOR_CANARY_TICK_SEC"
fi

export BTC5M_OPERATOR_TRACE="${BTC5M_OPERATOR_TRACE:-true}"
export PYTHONUNBUFFERED=1

stage "env_file_sourced" \
  env_file "$ENV_FILE" \
  strategy_id "${BTC5M_STRATEGY_ID:-}" \
  paper_only "${BTC5M_BROWNIAN_PAPER_ONLY:-}" \
  live_enabled "${BTC5M_BROWNIAN_LIVE_ENABLED:-}" \
  execution_mode "${BTC5M_EXECUTION_MODE:-}" \
  live_one_shot "${BTC5M_LIVE_ONE_SHOT:-}" \
  allow_continuous_live "${BTC5M_ALLOW_CONTINUOUS_LIVE:-}" \
  ledger_db "${BTC5M_LIVE_LEDGER_DB:-state/btc5m_live_ledger.db}" \
  journal_root "${BTC5M_EXECUTION_JOURNAL_ROOT:-artifacts/btc5m_canary_execution}"

fail() {
  echo "refusing live run: $1" >&2
  exit 2
}

stage "live_safety_validation_start"
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
stage "live_safety_validation_ok"

MAX_RUNTIME_EFFECTIVE="${MAX_RUNTIME_SEC:-${BTC5M_MAX_RUNTIME_SEC:-300}}"
[[ "$MAX_RUNTIME_EFFECTIVE" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "MAX_RUNTIME_SEC/BTC5M_MAX_RUNTIME_SEC must be numeric"
awk "BEGIN{exit !($MAX_RUNTIME_EFFECTIVE > 0)}" || fail "MAX_RUNTIME_SEC/BTC5M_MAX_RUNTIME_SEC must be > 0"

stage "python_exec_start" \
  env_file "$ENV_FILE" \
  max_runtime_sec "$MAX_RUNTIME_EFFECTIVE" \
  poll_interval_sec "${BTC5M_CANARY_TICK_SEC:-1}" \
  strategy_id "${BTC5M_STRATEGY_ID:-}" \
  paper_only "${BTC5M_BROWNIAN_PAPER_ONLY:-}" \
  live_enabled "${BTC5M_BROWNIAN_LIVE_ENABLED:-}" \
  execution_mode "${BTC5M_EXECUTION_MODE:-}" \
  live_one_shot "${BTC5M_LIVE_ONE_SHOT:-}" \
  allow_continuous_live "${BTC5M_ALLOW_CONTINUOUS_LIVE:-}" \
  ledger_db "${BTC5M_LIVE_LEDGER_DB:-state/btc5m_live_ledger.db}" \
  journal_root "${BTC5M_EXECUTION_JOURNAL_ROOT:-artifacts/btc5m_canary_execution}"

exec .venv/bin/python scripts/run_btc5m_canary_live.py \
  --env-file "$ENV_FILE" \
  --build-live-input \
  --max-runtime-sec "$MAX_RUNTIME_EFFECTIVE"
