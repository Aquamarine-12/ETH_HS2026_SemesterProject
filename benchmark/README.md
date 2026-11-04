# TPC-H stream benchmark

One Python file, two commands. N connections share one DuckDB instance; each
executes the 22 TPC-H queries, one at a time. Choose CPU execution (default) or
GPU execution through a locally installed Sirius extension.

The [timing findings](experiments/frontend_overhead_timing/TIMING_FINDINGS.md) summarize the measured planning
overhead and why complete execute-and-fetch timing is retained. Reproduce the
[timing experiment](experiments/frontend_overhead_timing/TIMING_EXPERIMENT.md) with `bash experiments/frontend_overhead_timing/run_timing_experiment.sh`.
It uses a preserved source snapshot, independent of future benchmark changes.

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
python benchmark.py benchmark --db data/tpch.duckdb --connections 1 \
  --output results/cpu-baseline.json
python benchmark.py benchmark --db data/tpch.duckdb --connections 4 \
  --baseline results/cpu-baseline.json --output results/cpu-n4.json
```

Generate the tables only once. DuckDB reports errors from table generation.

The first generation needs network access to install DuckDB's TPC-H extension.
DuckDB chooses its engine resource defaults. `--connections` controls client
streams, not DuckDB's internal worker threads.

Omit `--seed` to give every stream the identical Q1–Q22 order. This is the
recommended starting point for cold-start comparisons: the isolated baseline
and each measured stream then have the same query order. `--seed` still enables
reproducible shuffling, but different stream permutations have different cache
histories and would need their own isolated references for matched comparisons.

## Cold-start measurements

Each `benchmark` invocation performs **one measured workload**. It requests
Linux cache eviction for the database file, opens a new database instance,
then runs N streams. There is no preliminary query pass or warmup.

First run N=1 without `--baseline` to save an isolated reference. Then start a
separate Python process with `--baseline` to compare against that reference.
Use the same database, backend and query order. Without `--baseline`, JSON
contains QPS and query timings, with no `mean_slowdown` value.

For the N=1 sanity check, compare a second fresh run against the first:

```bash
python benchmark.py benchmark --db data/tpch.duckdb --connections 1 \
  --baseline results/cpu-baseline.json --output results/cpu-n1-check.json
```

Both N=1 and N>1 use worker connections and the same stream timing path. The
baseline file is read outside timing; its queries are not executed again.

Before opening DuckDB, the program calls `fsync` on the database file, then
`POSIX_FADV_DONTNEED` over the whole file. Flushing first makes dirty pages
eligible for eviction. This needs no root privileges and requests eviction
only for this file, although other processes reading the same file share its
page cache. [Linux documents this as best effort](https://man7.org/linux/man-pages/man2/posix_fadvise.2.html),
so JSON records the reset method as **unverified**. It does not clear storage
device/server caches or establish a guaranteed physical-disk cold start.
Use the cluster's cache-control mechanism if strict storage-cold measurements
are required. The implementation targets Linux.

Cache-reset work and engine setup are outside timing. Cache contents naturally
grow during Q1–Q22; the reset is at stream start, not between queries. Negative
N=1 comparisons are still possible through variability or incomplete cache
eviction; values are reported unchanged rather than clamped to zero. Prepared
statements, timing boundaries and hot-cache warmups are deferred.

## GPU execution

Use the matched installation above on an allocated NVIDIA GPU node. Run each
backend in a fresh process, using the same environment and database:

```bash
python benchmark.py benchmark --connections 1 --device gpu \
  --sirius-extension "$HOME/.local/lib/sirius/duckdb-2.0.0/sirius.duckdb_extension" \
  --output results/gpu-baseline.json
python benchmark.py benchmark --connections 4 --device gpu \
  --baseline results/gpu-baseline.json \
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

Every timed GPU connection enables `gpu_execution` and explicitly disables
CPU fallback with `enable_duckdb_fallback = false`. Unsupported execution fails
the run. This matches the current source and the preserved timing experiment.
See the upstream [execution settings](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/configuration.md).

The default extension path is
`$HOME/.local/lib/sirius/duckdb-2.0.0/sirius.duckdb_extension`, matching the
installation above. Use `--sirius-extension` for a different location. To find
an existing installation (including the unversioned path from earlier setup
instructions):

```bash
find "$HOME/.local/lib" -type f -name '*sirius*.duckdb_extension*'
```

Pass the uncompressed file's actual path to `--sirius-extension`. Argument
types and device choices are handled by argparse; other input errors are left
to Python or DuckDB.

Sirius also writes local [Quent telemetry traces](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/quent-telemetry.md),
normally under `telemetry_data/`. These describe query plans, operators,
threads, tasks, queues and execution timing, and can be viewed in Quent's
timeline UI. Telemetry is enabled by Sirius's defaults; our generated YAML
currently leaves that default intact. It is separate from our QPS/slowdown
JSON. Disabling it requires `sirius.telemetry.enable_quent: false` in the
generated startup YAML.

## What is measured

Isolated latencies `L_q` come from a separate N=1 invocation on the selected
backend. The measured invocation releases its N streams together at one
barrier. Each stream fully fetches one query's result before starting its next
query. There is no synchronization between subsequent queries.

- `C_s,q`: execute-and-fetch latency of query q in concurrent stream s.
- `elapsed_s`: common barrier release to the last stream's completion;
  connection setup and cleanup are outside this interval.
- `qps = 22 * N / elapsed_s`.
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
measured latencies. Baselines from the old combined-run workflow should be
remeasured using the separate-process workflow.

JSON contains these metrics, device, N, seed, process ID, cache reset method,
baseline path, each stream's query order, and measured latencies in seconds.
Comparison runs also include the loaded isolated latencies. Query IDs become
string keys in JSON. The output file is written after measurement; rerunning
with the same output path replaces it. Choose different filenames to retain
multiple runs.

There is one measured pass per invocation, with no result validation or warmup
phase. These are initial exploratory measurements, not certified
TPC-H results. Any query error fails the run without writing new JSON metrics;
connection cleanup still occurs. An old output file remains if the run fails,
so choose a fresh filename for each experiment. GPU execution needs validation
on the actual cluster; CPU tests and mocked GPU calls do not establish that.

Read `benchmark.py` from top to bottom: generation → query loading → stream
execution → concurrency → metrics → benchmark coordination → CLI.
