# Two concurrent TPC-H queries in DuckDB

One Python process, two application threads, two independent connections to
one shared DuckDB instance. The script measures both isolated and concurrent
latencies. Requires Python 3.10+; pins DuckDB 1.5.6 for reproducible setup.

## Install

Extract the archive, open a terminal in `duckdb-two-queries`, then run:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Only DuckDB is an external Python dependency. The remainder uses Python's
standard library. No DuckDB CLI installation or database server is required.
The Python package version is independent of any DuckDB CLI you installed.

## Generate the data once

```bash
python concurrent_tpch.py init --db tpch.duckdb --sf 1
```

The `tpch` extension is loaded, or installed from DuckDB's extension repository
if necessary. This initial installation needs internet access. The generator
creates the eight TPC-H tables in a persistent database file, runs ANALYZE,
and checkpoints. At SF1, `lineitem` contains 6,001,215 rows.

Initialization requires an empty database and refuses to replace existing
tables. Use a new filename for a new scale factor:

```bash
python concurrent_tpch.py init --db tpch-sf3.duckdb --sf 3
```

If a failed generation leaves an incomplete database, choose a new filename.
Do not repeatedly generate into a partially populated database.

To use an existing complete TPC-H database, skip `init` and pass its filename
to `run`. Close other programs such as the DuckDB CLI if they have that file
open. This script opens the file read/write in one process. An in-memory
database in another CLI session is not accessible to this script.

## Run Q1 and Q2 concurrently

```bash
python concurrent_tpch.py run --db tpch.duckdb --queries 1 2 --threads 4 --repetitions 5 --output q1-q2.json
```

This performs an untimed warm-up of each query alone and of the pair, followed
by five repetitions. Each repetition contains Q1 alone, Q2 alone, and Q1+Q2
together. Their order is shuffled using a recorded seed to avoid always
measuring one condition first. It prints median latencies and slowdown for
each slot, and saves every measured sample to JSON. Existing output files
are never overwritten.

Try a different pair or two copies of the same query:

```bash
python concurrent_tpch.py run --db tpch.duckdb --queries 1 6 --output q1-q6.json
python concurrent_tpch.py run --db tpch.duckdb --queries 1 1 --output q1-q1.json
```

Exactly two query numbers are required, each in 1..22. Duplicate query numbers
remain separate slots and have separate timing records.

To inspect the settings available:

```bash
python concurrent_tpch.py run --help
```

| Option | Default | Meaning |
|---|---|---|
| `--db` | `tpch.duckdb` | Persistent database file |
| `--queries` | `1 2` | The two TPC-H query numbers (`run` only) |
| `--threads` | `4` | DuckDB execution-thread configuration |
| `--memory` | `4GB` | Shared buffer-manager memory limit |
| `--repetitions` | `5` | Number of measured repetitions per condition |
| `--warmups` | `1` | Untimed warm-up rounds |
| `--seed` | `0` | Seed for condition order |
| `--partial` | off | Enable partial task rescheduling |
| `--output` | `results.json` | New JSON result file (`run` only) |
| `--sf` | `1` | Scale factor (`init` only) |

## Understand the output

The terminal summary has these columns:

| Column | Interpretation |
|---|---|
| `slot` | 0 or 1, distinguishing the two query submissions |
| `query` | TPC-H query number |
| `isolated median (s)` | Median elapsed time with this query running alone |
| `concurrent median (s)` | Median elapsed time with the other query also submitted |
| `slowdown` | Concurrent median divided by isolated median |

For example, a slowdown of 1.5x means a query took 50% longer under concurrent
submission. It does not mean overall throughput fell by the same amount.

The JSON includes:

- DuckDB version and source revision, Python/platform, process ID, and settings.
- Database path, generated scale factor when known, and both SQL strings.
- Experiment ordering and every raw latency sample.
- Result row counts and application thread IDs.
- Per-batch query start/end offsets, makespan, and batch queries per second.

Offsets are relative to the earliest query start within each condition.
They show submission/completion overlap, not a trace of CPU worker activity.
All application thread IDs belong to the recorded single Python process.

## How the code implements the setup

`benchmark()` opens a root connection and keeps it alive for the experiment.
It configures the shared database and retrieves the two SQL strings before
timing starts. A `ThreadPoolExecutor(max_workers=2)` supplies the two
application threads.

`run_pair()` creates a fresh two-party barrier and submits two `run_one()`
calls. Each application thread creates and owns its own connection to the
same absolute database filename. In the pinned DuckDB Python package, these
connections reuse the process's cached database instance; the root connection
keeps that instance alive between runs. Each worker waits at the barrier
before executing SQL:

```python
with duckdb.connect(str(db)) as con:
    barrier.wait()
    start = perf_counter()
    rows = con.execute(sql).fetchall()
    end = perf_counter()
```

This is the core sequence; the full script adds error handling, measurements,
warm-ups, isolated controls, and output. It opens a fresh SQL connection per
execution, outside the timer, while retaining the same native database
instance and background pool. Isolated controls use an application thread
and the same connection-opening path, with the other query absent.

Python orchestrates calls and timings. DuckDB performs SQL execution in
native code and can use its execution workers. A calling application thread
can also execute DuckDB tasks.

### What is timed?

`perf_counter()` surrounds `execute(sql).fetchall()`. Therefore latency
includes parsing, optimization, SQL execution, and converting all results
into Python objects. Connection setup, barrier waiting, extension loading,
data generation, and printing are outside that timer. No query plan profiling
is enabled during measurements.

### What does `--threads 4` mean?

It configures DuckDB, not the number of concurrent queries. The script always
submits exactly two queries in its concurrent condition.

For DuckDB 1.5.6, this script explicitly keeps `external_threads=1`. With
`threads=4`, the background pool has three workers; the two calling threads
can also execute tasks. Consequently, this is not a strict four-CPU-thread
limit on the process. `threads=1` with two callers can still allow concurrent
execution. Keep both thread settings in your experiment records. The memory
limit is shared by connections and is not a hard cap on every process
allocation.

## Limits of this first benchmark

- This is a simultaneous two-query batch, not a continuing stream of requests.
  It does not study arrival rates or steady-state fairness.
- The barrier releases both callers together, but OS scheduling can delay one.
  Actual start offsets are recorded. Very short queries may not overlap much.
- Warming does not guarantee all data remains cached. If SF1 is too fast,
  increase SF, while monitoring memory pressure and keeping settings fixed.
- Result row counts are checked for consistency. Full answer values are not
  validated automatically.
- The built-in extension uses fixed substitution parameters. These are
  exploratory TPC-H workload measurements, not certified TPC-H results.
- On a laptop, use AC power and a consistent power mode, and avoid substantial
  background workloads. Five samples are a starting point, not a p99 estimate.
- For the later DuckDB/Sirius comparison, align the engine versions with the
  DuckDB revision embedded in Sirius. The pin here is a tested starting point.

## Validation of this package

Tested in the assistant's environment with Python 3.12 and DuckDB 1.5.6:
SF1 generation, Q1/Q2 repeated isolated and concurrent execution, duplicate
queries, one execution-thread setting, and partial scheduling. Connection
checks confirmed shared instance settings and independent session state.
Concurrent samples used two distinct application thread IDs with overlapping
query calls. CLI error handling was checked for invalid IDs, missing
databases, existing outputs, and repeated initialization.

## References

- https://duckdb.org/docs/1.5/guides/python/multiple_threads
- https://duckdb.org/docs/1.5/clients/python/dbapi
- https://duckdb.org/docs/1.5/core_extensions/tpch
