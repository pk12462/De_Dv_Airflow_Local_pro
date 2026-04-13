#!/usr/bin/env sh
set -eu

echo "[cloudflare-build] Starting build..."

# Worker deployment has no compile step; run a lightweight repository check.
python -m pytest -o addopts="" tests/test_smoke_configs.py -q

echo "[cloudflare-build] Build checks completed successfully."

