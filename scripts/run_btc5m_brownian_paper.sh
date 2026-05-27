#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${BTC5M_ENV_FILE:-.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "refusing paper run: env file does not exist: $ENV_FILE" >&2
  echo "set BTC5M_ENV_FILE to an alternate profile if you are not using .env" >&2
  exit 2
fi

exec .venv/bin/python scripts/run_btc5m_canary_live.py \
  --env-file "$ENV_FILE" \
  --build-live-input \
  --max-runtime-sec "${MAX_RUNTIME_SEC:-300}"
