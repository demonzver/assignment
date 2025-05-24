#!/usr/bin/env bash
set -e
export PYTHONPATH="/opt/airflow/src:$PYTHONPATH"
python -m github_commit_pipeline.collector
