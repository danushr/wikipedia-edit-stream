#!/bin/sh
set -eu
STREAM_DATA_DIR=$(mktemp -d)
export STREAM_DATA_DIR
trap 'rm -rf "$STREAM_DATA_DIR"' EXIT

uv run pytest -q
uv run python -m stream.producer --replay "$STREAM_PROJECT_DIR/sample/recentchange_sample.jsonl" --limit 1000
( sleep 100; for p in $(jps -q); do jcmd "$p" Thread.print 2>&1 | grep -A22 -E 'stream execution thread|Executor task launch' | head -120; done ) &
timeout 150 uv run python -m stream.run --once
uv run python - <<'PY'
from deltalake import DeltaTable
from stream import paths
n = DeltaTable(str(paths.LAKE_DIR / "silver_events")).to_pyarrow_table().num_rows
assert n == 1000, n
PY
