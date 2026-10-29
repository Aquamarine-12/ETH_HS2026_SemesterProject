# TPC-H stream benchmark

One Python file, two commands. N connections share one DuckDB instance; each
executes its own shuffled permutation of the 22 TPC-H queries, one at a time.
This first iteration uses DuckDB on CPU.

From this directory, with Python 3.11+:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install 'duckdb==1.5.6'
python benchmark.py generate-table --db data/tpch.duckdb --scale-factor 0.01
python benchmark.py benchmark --db data/tpch.duckdb --connections 4 --seed 0 --output results/n4.json
```

Generate the tables only once; generation refuses to overwrite a database.
SF0.01 is a small functionality check. Use a larger scale factor for experiments.
The first generation needs network access to install DuckDB's TPC-H extension.
DuckDB chooses its engine resource defaults. `--connections` controls client
streams, not DuckDB's internal worker threads.

## What is measured

The benchmark first executes Q1–Q22 sequentially to measure isolated latencies
`L_q`. It then releases all N streams together at one barrier. Each stream
fully fetches one query's result before starting its next query. There is no
synchronization between subsequent queries.

- `C_s,q`: execute-and-fetch latency of query q in concurrent stream s.
- `elapsed_s`: common barrier release to the last stream's completion;
  connection setup and cleanup are outside this interval.
- `qps = 22 * N / elapsed_s`; baseline queries are excluded.
- `aggregate_slowdown = sum(C_s,q / L_q for all streams and queries) / N`.
  This averages the sum of query slowdowns across streams. Its no-slowdown
  value is **22**, not 1.

JSON contains these metrics, N, seed, each stream's query order, and all
isolated/concurrent query latencies in seconds. Query IDs become string keys
in JSON. The output file is written after measurement; rerunning with the same
output path replaces it. Choose different filenames to retain multiple runs.

There is one baseline pass and one concurrent pass, with no result validation
or explicit warmup phase. Baselines already warm data, and different query
orders affect caches. These are initial exploratory measurements, not certified
TPC-H results. Ordinary query errors stop the run without writing new metrics.

Read `benchmark.py` from top to bottom: generation → query loading → stream
execution → concurrency → metrics → benchmark coordination → CLI.
