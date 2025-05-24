#!/usr/bin/env bash
# Restore a commit snapshot from S3 to the local filesystem.
# Usage: ./scripts/run_restore.sh <owner/repo> <commit_sha> [before|after] [dest_dir]

set -euo pipefail

REPO="${1:?owner/repo is required}"
SHA="${2:?commit sha is required}"
STATE="${3:-after}"
OUTDIR="${4:-./restored}"

poetry run python -m github_commit_pipeline.restore \
  --repo "$REPO" \
  --sha  "$SHA"  \
  --state "$STATE" \
  --out "$OUTDIR"