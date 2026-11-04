#!/usr/bin/env bash
# Reproduce the frontend timing experiment using its frozen Python sources.
# Activate the matched DuckDB/Sirius environment and allocate a GPU first.
set -euo pipefail

EXPERIMENT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$(cd -- "$EXPERIMENT_DIR/../.." && pwd)"
PYTHON="${PYTHON:-python}"
DB_PATH="${DB_PATH:-data/sf10.duckdb}"
SIRIUS_EXTENSION="${SIRIUS_EXTENSION:-$HOME/.local/lib/sirius/sirius.duckdb_extension}"

# Keep database, result, and telemetry paths relative to the benchmark directory.
cd "$BENCHMARK_DIR"
mkdir -p results
TIMING_DIR="$(mktemp -d "results/timing-$(date +%Y%m%d-%H%M%S)-XXXXXX")"
"$PYTHON" -c '
import sys
import duckdb
print("Python executable:", sys.executable)
print(sys.version)
print("DuckDB:", duckdb.__version__)
with duckdb.connect() as connection:
    print("Engine:", connection.execute("PRAGMA version").fetchall())
' > "$TIMING_DIR/environment.txt"

for repeat in 1 2 3; do
  # Match the original experiment: reverse the pair order in repetition two.
  if [ "$repeat" = 2 ]; then
    runners=(profile_timing.py benchmark.py)
  else
    runners=(benchmark.py profile_timing.py)
  fi
  for device in cpu gpu; do
    for connections in 1 2; do
      for runner in "${runners[@]}"; do
        kind=plain
        if [ "$runner" = profile_timing.py ]; then kind=profile; fi
        "$PYTHON" "$EXPERIMENT_DIR/$runner" benchmark --db "$DB_PATH" \
          --device "$device" --connections "$connections" \
          --sirius-extension "$SIRIUS_EXTENSION" \
          --output "$TIMING_DIR/$device-n$connections-r$repeat-$kind.json"
      done
    done
  done
done

"$PYTHON" "$EXPERIMENT_DIR/summarize_timing.py" "$TIMING_DIR"
echo "Results: $TIMING_DIR"
