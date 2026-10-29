# TPC-H stream benchmark

One Python file, two commands. N connections share one DuckDB instance; each
executes the 22 TPC-H queries, one at a time. Choose CPU execution (default) or
GPU execution through a locally installed Sirius extension.

From this directory, with Python 3.11+:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install 'duckdb==1.5.6'
python benchmark.py generate-table --db data/tpch.duckdb --scale-factor 0.01
python benchmark.py benchmark --db data/tpch.duckdb --connections 4
```

Generate the tables only once; generation refuses to overwrite a database.
SF0.01 is a small functionality check. Use a larger scale factor for experiments.
The first generation needs network access to install DuckDB's TPC-H extension.
DuckDB chooses its engine resource defaults. `--connections` controls client
streams, not DuckDB's internal worker threads.

Omit `--seed` to give every stream the identical Q1–Q22 order. Pass `--seed 0`
(or any integer) to generate a reproducible permutation for each stream. Use
the same N and seed for CPU/GPU comparisons.

## GPU execution

Use a Python DuckDB build matching your Sirius extension's exact engine
revision, on an allocated NVIDIA GPU node. The CPU installation example above
does not necessarily match your Sirius build. Run each backend in a separate
process:

```bash
python benchmark.py benchmark --connections 4 --device cpu --seed 0 \
  --output results/cpu-n4.json
python benchmark.py benchmark --connections 4 --device gpu --seed 0 \
  --sirius-extension /absolute/path/to/sirius.duckdb_extension \
  --output results/gpu-n4.json
```

The GPU path writes `results/gpu-n4.sirius.yaml` with
`sirius.max_concurrent_queries: 4` (your N), sets `SIRIUS_CONFIG_FILE`, then
loads the unsigned local extension. This minimal startup YAML replaces any
inherited config selection; other Sirius settings use engine defaults.
GPU visibility assigned by the scheduler is preserved. Use a Sirius revision
supporting [concurrent queries](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/concurrent-queries.md).

Every timed GPU connection explicitly enables `gpu_execution` and disables
`enable_duckdb_fallback`, so Sirius errors fail the benchmark rather than
producing CPU timings. See the upstream [execution settings](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/configuration.md).

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
- `aggregate_slowdown = sum(C_s,q / L_q for all streams and queries) / N`.
  This averages the sum of query slowdowns across streams. Its no-slowdown
  value is **22**, not 1.

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
