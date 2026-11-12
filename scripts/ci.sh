#!/bin/sh
set -eu
STREAM_DATA_DIR=$(mktemp -d)
export STREAM_DATA_DIR
trap 'rm -rf "$STREAM_DATA_DIR"' EXIT

uv run pytest -q
uv run python -m stream.producer --replay "$STREAM_PROJECT_DIR/sample/recentchange_sample.jsonl" --limit 1000
uv run python -m stream.run --once | tee "$STREAM_DATA_DIR/run.log"
grep -qx "silver_events 1000" "$STREAM_DATA_DIR/run.log"
