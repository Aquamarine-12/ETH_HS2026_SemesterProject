# TPC-H stream benchmark

One Python file, two commands. N connections share one DuckDB instance; each
executes the 22 TPC-H queries, one at a time. Choose CPU execution (default) or
GPU execution through a locally installed Sirius extension.

## User-local installation

Use one environment containing **DuckDB 2.0.0 and its matching Sirius binary**
for both CPU and GPU measurements. All installation paths below belong to your
user; no `sudo` or machine-wide installation is needed.

From this directory, with Python 3.11+, install DuckDB:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install 'duckdb==2.0.0'
```

The [Sirius release used here](https://github.com/sirius-db/sirius/releases/tag/latest)
(`13dbefe`) specifies DuckDB 2.0.0. Download its Linux x86-64 CUDA 12 binary:

```bash
mkdir -p "$HOME/.local/lib/sirius/duckdb-2.0.0"
curl -fL \
  https://github.com/sirius-db/sirius/releases/download/latest/sirius-linux_amd64-cuda12.duckdb_extension.gz \
  -o "$HOME/.local/lib/sirius/duckdb-2.0.0/sirius.duckdb_extension.gz"
gzip -dc "$HOME/.local/lib/sirius/duckdb-2.0.0/sirius.duckdb_extension.gz" \
  > "$HOME/.local/lib/sirius/duckdb-2.0.0/sirius.duckdb_extension"
```

For ARM64, select the `linux_arm64` asset. CUDA 13 assets use `cuda13` in the
filename. Upstream requires an NVIDIA Turing-or-newer GPU and driver 525.60.13+
for CUDA 12, or 580.65.06+ for CUDA 13. Use your cluster's GPU allocation.

Verify the installed pair inside that allocation before running comparisons:

```bash
python - <<'PY'
from pathlib import Path
import duckdb

extension = (
    Path.home() / ".local/lib/sirius/duckdb-2.0.0/sirius.duckdb_extension"
)
with duckdb.connect(config={"allow_unsigned_extensions": "true"}) as con:
    con.execute(f"LOAD '{extension}'")
    con.execute("SET gpu_execution = false")
    print(con.execute("PRAGMA version").fetchall())
    print("DuckDB 2.0.0 and Sirius loaded successfully")
PY
```

Successful `LOAD` checks the extension's compatibility with the actual Python
DuckDB engine. Keep this installed binary and use this same `.venv` for both
backends. Reactivate it in later sessions with `source .venv/bin/activate`.

## Generate data and run

```bash
python benchmark.py generate-table --db data/tpch.duckdb --scale-factor 10
python benchmark.py benchmark --db data/tpch.duckdb --connections 4
```

Generate the tables only once. DuckDB reports errors from table generation.

The first generation needs network access to install DuckDB's TPC-H extension.
DuckDB chooses its engine resource defaults. `--connections` controls client
streams, not DuckDB's internal worker threads.

Omit `--seed` to give every stream the identical Q1–Q22 order. Pass `--seed 0`
(or any integer) to generate a reproducible permutation for each stream. Use
the same N and seed for CPU/GPU comparisons.

## GPU execution

Use the matched installation above on an allocated NVIDIA GPU node. Run each
backend in a fresh process, using the same environment and database:

```bash
python benchmark.py benchmark --connections 4 --device cpu --seed 0 \
  --output results/cpu-n4.json
python benchmark.py benchmark --connections 4 --device gpu --seed 0 \
  --sirius-extension "$HOME/.local/lib/sirius/duckdb-2.0.0/sirius.duckdb_extension" \
  --output results/gpu-n4.json
```

The GPU path writes `results/gpu-n4.sirius.yaml` with
`sirius.max_concurrent_queries: 4` (your N), sets `SIRIUS_CONFIG_FILE`, then
loads the unsigned local extension. This minimal startup YAML replaces any
inherited config selection; other Sirius settings use engine defaults.
GPU visibility assigned by the scheduler is preserved. Use a Sirius revision
supporting [concurrent queries](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/concurrent-queries.md).

The admission limit is a startup-only YAML setting in Sirius's documented
DuckDB interface. It must be available before `LOAD`; there is no corresponding
runtime `SET max_concurrent_queries`. Settings exposed through SQL, such as
`gpu_execution` and `enable_duckdb_fallback`, are applied directly through
`connection.execute(...)`.

Every timed GPU connection enables `gpu_execution` and CPU fallback through
`enable_duckdb_fallback`. GPU runs can therefore include CPU execution when
Sirius falls back. See the upstream [execution settings](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/configuration.md).

The default extension path is `./sirius.duckdb_extension`. Argument types and
device choices are handled by argparse; other input errors are left to Python
or DuckDB.

Sirius also writes local [Quent telemetry traces](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/quent-telemetry.md),
normally under `telemetry_data/`. These describe query plans, operators,
threads, tasks, queues and execution timing, and can be viewed in Quent's
timeline UI. Telemetry is enabled by Sirius's defaults; our generated YAML
currently leaves that default intact. It is separate from our QPS/slowdown
JSON. Disabling it requires `sirius.telemetry.enable_quent: false` in the
generated startup YAML.

## What is measured

The benchmark first executes Q1–Q22 sequentially to measure isolated latencies
`L_q` on the selected backend. It then releases all N streams together at one
barrier. Each stream
fully fetches one query's result before starting its next query. There is no
synchronization between subsequent queries.

- `C_s,q`: execute-and-fetch latency of query q in concurrent stream s.
- `elapsed_s`: common barrier release to the last stream's completion;
  connection setup and cleanup are outside this interval.
- `qps = 22 * N / elapsed_s`; baseline queries are excluded.
- `mean_slowdown = sum(C_s,q / L_q) / (22 * N) - 1`.
  This averages each query's latency ratio equally across all streams:
  **0** means no change, **+0.5** means 50% higher latency on average, and
  **-0.2** means 20% lower latency on average. Each backend uses its own
  isolated baseline; this metric measures the effect of concurrent execution.
  This measures per-query latency changes rather than total stream duration.

Older JSON files used `aggregate_slowdown`, whose no-change value was 22.
Convert those values with `aggregate_slowdown / 22 - 1`; new runs write
`mean_slowdown` instead. For files containing `mean_slowdown_percent`, divide
that value by 100. Negative values are retained to show lower
measured latencies. Since isolated queries always run first, initialization
and cache warming can contribute to negative values, especially at SF0.01.

JSON contains these metrics, device, N, seed, each stream's query order, and all
isolated/concurrent query latencies in seconds. Query IDs become string keys
in JSON. The output file is written after measurement; rerunning with the same
output path replaces it. Choose different filenames to retain multiple runs.

There is one baseline pass and one concurrent pass, with no result validation
or explicit warmup phase. Baselines already warm data, and different query
orders affect caches. These are initial exploratory measurements, not certified
TPC-H results. Any query error fails the run without writing new JSON metrics;
connection cleanup still occurs. An old output file remains if the run fails,
so choose a fresh filename for each experiment. GPU execution needs validation
on the actual cluster; CPU tests and mocked GPU calls do not establish that.

Read `benchmark.py` from top to bottom: generation → query loading → stream
execution → concurrency → metrics → benchmark coordination → CLI.
