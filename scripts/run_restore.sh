#!/usr/bin/env bash
# Restore an entire commit snapshot (before/after) from MinIO.
# Usage:
#   ./scripts/run_restore.sh <repo> <sha> <before|after> <out_dir>

set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "Usage: $0 <repo> <sha> <before|after> <out_dir>"
  exit 1
fi

REPO="$1"
SHA="$2"
DIRECTION="$3"
OUT_DIR="$4"

poetry run python -m github_commit_pipeline.restore_folder \
  --repo "$REPO" \
  --sha "$SHA" \
  --direction "$DIRECTION" \
  --out "$OUT_DIR"