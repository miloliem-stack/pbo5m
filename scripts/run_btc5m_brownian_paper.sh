#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=".env.btc5m.brownian.paper.local"
if [[ ! -f "$ENV_FILE" ]]; then
  ENV_FILE=".env.btc5m.brownian.paper.example"
fi

exec .venv/bin/python scripts/run_btc5m_canary_live.py \
  --env-file "$ENV_FILE" \
  --build-live-input \
  --max-runtime-sec "${MAX_RUNTIME_SEC:-300}"
