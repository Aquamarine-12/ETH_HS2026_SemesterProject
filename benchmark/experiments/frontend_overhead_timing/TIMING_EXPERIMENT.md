# Frontend timing experiment

Keep the existing cold-start procedure and compare ordinary runs with diagnostic
profiles. Test CPU and Sirius GPU at N=1 and N=2, with three repetitions each.
The [preserved experiment](README.md) contains
unchanged copies of the benchmark, profiling wrapper, and report script.
The runner uses those copies, so future changes or module splits in the active
benchmark do not change this experiment. No extra dependencies are needed.
Read [TIMING_FINDINGS.md](TIMING_FINDINGS.md) for the measured results and decision.

## Run on the cluster

Copy the complete `experiments/frontend_overhead_timing/`
directory to the cluster, preserving their locations inside `benchmark/`.
Activate your existing `tpch-bench` environment and obtain the usual GPU
allocation. Run:

```bash
cd ~/projects/ETH_HS2026_SemesterProject/benchmark
bash experiments/frontend_overhead_timing/run_timing_experiment.sh
```

The script works from any starting directory: relative database paths and
outputs are anchored to the repository's `benchmark/` directory, two levels
above the script. It creates a unique
`results/timing-DATE-TIME-SUFFIX/` directory and stops on the first failed command.
`environment.txt` records the Python executable/version and DuckDB engine
version/revision. Keep the installed Sirius binary and its provenance as well;
the engine revision alone does not identify that extension.

Defaults match the original cluster command. Override paths if needed:

```bash
DB_PATH=data/other.duckdb \
  SIRIUS_EXTENSION="$HOME/.local/lib/sirius/sirius.duckdb_extension" \
  PYTHON=python bash experiments/frontend_overhead_timing/run_timing_experiment.sh
```

There are 24 independent invocations. All use the current cache-reset request,
resource defaults, backend settings, and Q1–Q22 order. The extension path is
the unversioned location already found on your cluster. No baseline is needed:
this experiment examines timing components and variability rather than computing
slowdown. The ordinary results retain every query's latency for later analysis.

To inspect one profiled run first, use a separate output path:

```bash
python experiments/frontend_overhead_timing/profile_timing.py benchmark \
  --db data/sf10.duckdb \
  --device gpu --connections 1 \
  --sirius-extension "$HOME/.local/lib/sirius/sirius.duckdb_extension" \
  --output results/gpu-timing-check.json
```

## What is measured

The profiling wrapper temporarily replaces the benchmark's connection setup
and stream execution functions, then restores them when the invocation exits.
It calls the original backend setup, preserving GPU execution and CPU fallback
policy. It does not introduce a preliminary query pass or warmup.

Each query records three intervals:

- `execute_s`: entering `execute(sql)` through its return.
- `fetch_s`: the subsequent full `fetchall()` call.
- `wall_s`: the complete execute-and-fetch interval, matching the ordinary
  benchmark's completion boundary. It equals `execute_s + fetch_s`.

Some clients can return from `execute()` once the first rows are available.
Remaining execution may then happen while fetching. Consequently, `fetch_s`
must not be labeled Python-conversion time or subtracted to obtain pure engine
execution time. A query is fully consumed before the next query starts.
[DuckDB Python API](https://duckdb.org/docs/2.0/clients/python/reference/)

After full fetching, the wrapper retrieves the completed query's profile from
the same connection, before another query can overwrite it. Raw profile strings
remain in per-stream memory; JSON decoding and file writing happen after the
workload. Retrieval still adds gaps between queries and can change overlap
between streams. The profiling configuration itself runs before the barrier.

The engine profile records available parent-phase timings: parser, logical
planner, optimizer, and physical planner. The wrapper supports the older
DuckDB detailed profile and DuckDB 2.0's named metrics. The report preserves
missing metrics; it does not treat them as zero or sum nested subphases with
their parents. [Profiling controls](https://duckdb.org/docs/2.0/dev/profiling)

## Read the results

For each diagnostic output `NAME.json`, the wrapper also writes
`NAME.profiles.json`, containing raw engine profiles and the three client
intervals with explicit stream/query IDs. The ordinary result is labeled
`profiling_enabled: true` and includes the DuckDB version and interpretation
note. A new directory for each experiment prevents stale files being mixed in.

`python experiments/frontend_overhead_timing/summarize_timing.py DIRECTORY` prints
per-query medians grouped by device and
connection count. Its `Frontend` column sums available parent phases for each
sample, then takes the median. `Share%` is the median of each sample's reported
frontend time divided by its complete execute-and-fetch time. `Fields` counts
available fields out of the four requested DuckDB phase categories; recorded
zero is an available value. With N=2 and three repetitions, each query has six
samples, including both streams. This pooling describes observations, not six
independent process trials.

The report writes `timing-summary.csv` with individual phase medians, sample
counts, and observed minimum/maximum complete query latencies. Times in the CSV
are seconds; the terminal table displays milliseconds. Unprofiled QPS medians
and ranges come only from files ending in `-plain.json`.

`Plain` is the ordinary-run median latency for the same backend, N, and query.
`P/U` divides the profiled median by that ordinary median. It describes the
observed difference between the experiments, including changes in stream pacing;
it is not a direct measurement of profiler code's CPU cost.

Use the ordinary runs for throughput and performance comparisons. Diagnostic
QPS is printed by the reused benchmark but includes profile-collection gaps;
it is not an execution-only throughput measurement. A difference between
ordinary and diagnostic runs includes instrumentation and changed stream pacing,
not just the cost of reading a timer.

For GPU results, four available DuckDB fields do not prove all planning has
been captured. Sirius can perform additional work inside extension callbacks
and its execution wrapper. Low reported GPU frontend time therefore needs a
coverage check before concluding that total planning overhead is negligible.
No Quent parser, prepared statements, or native execution timers are added.

## Local verification

CPU SF0.01 N=1/N=2 ordinary and profiled smoke runs were checked with the local
DuckDB 1.5.6 environment. DuckDB 2.0 format normalization and completion-boundary
behavior were checked separately. The user subsequently supplied successful
cluster CPU/GPU output and raw profile excerpts from DuckDB 2.0.0; see the
[findings](TIMING_FINDINGS.md). This does not resolve missing parser timing or
prove complete coverage of Sirius planning. No GPU run was performed locally.
