# Two-query concurrency benchmark

Python runner for comparing DuckDB CPU execution with Sirius GPU execution.
It runs two selected fixed TPC-H queries through independent connections to
one database instance, measures isolated controls and concurrent execution,
and saves raw observations plus reproducibility metadata. Python 3.11+.

## Quick CPU smoke run

From this directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-cpu-smoke.txt
python -m parallel_bench prepare --config configs/cpu-smoke.toml
python -m parallel_bench run --config configs/cpu-smoke.toml
```

The pinned PyPI wheel is for CPU development only. This small SF0.01 dataset
is useful for checking the runner, not for drawing performance conclusions.
`prepare` installs/loads the TPC-H extension if necessary; that first download
needs internet access. It refuses to overwrite an existing database or manifest.
After preparation, runs with the saved queries need no extension download.

You can use the already installed prototype environment from this directory:

```bash
../experimente-throwaway/duckdb-two-queries/.venv/bin/python -m parallel_bench prepare --config configs/cpu-smoke.toml
../experimente-throwaway/duckdb-two-queries/.venv/bin/python -m parallel_bench run --config configs/cpu-smoke.toml
```

`python -m parallel_bench` works directly here. Optionally install the CLI
with `python -m pip install -e .`, then use `parallel-bench` from any directory.
The package deliberately does not install/upgrade DuckDB as a dependency.

## Matched DuckDB/Sirius experiment

1. Choose a Sirius build with concurrent-query support and obtain a Python
   DuckDB wheel matching its exact DuckDB source revision. Install that wheel
   for **both** CPU and GPU runs. Follow the chosen Sirius release/build
   instructions; the public DuckDB wheel used for smoke testing is not a
   substitute. Neither the runner nor setup instructions modify Git state.
2. Set `expected_source_id` identically in `configs/duckdb.toml` and
   `configs/sirius.toml`. The examples pin the DuckDB revision observed in
   Sirius main on 2026-10-27, `d54cbbe3df17e8e5ce68312f483b4155836112aa`.
   The short pin matches `PRAGMA version`'s source ID. If using another
   release, change both pins. A mismatch fails before measurements.
3. Set `sirius.extension_path` to the actual compatible extension binary and
   edit `sirius-runtime.yaml` for your hardware if needed. Install the YAML
   dependency with `python -m pip install -e '.[sirius]'`.
4. Prepare the dataset once and reuse it and its manifest unchanged:

   ```bash
   python -m parallel_bench prepare --config configs/duckdb.toml --sf 1
   python -m parallel_bench run --config configs/duckdb.toml --queries 3 6
   python -m parallel_bench run --config configs/sirius.toml --queries 3 6
   ```

For a custom DuckDB build without a downloadable TPC-H extension, supply its
matching local binary via `--tpch-extension /absolute/path/tpch.duckdb_extension`.
Local unsigned extension loading during preparation may require a signed or
statically linked TPC-H build; the unsigned option is enabled only for Sirius
runs. Close other programs with this database open before using the runner.

The Sirius adapter selects one GPU with `CUDA_VISIBLE_DEVICES`, snapshots
the YAML with startup admission explicitly set to two, and sets
`SIRIUS_CONFIG_FILE` before importing DuckDB/loading Sirius. Both worker
connections explicitly enable GPU execution and apply
`enable_duckdb_fallback`. Missing settings fail visibly. With fallback enabled,
artifacts say `fallback_permitted`; they never claim GPU-only execution.
With fallback disabled, artifacts say `gpu_requested_fallback_disabled`:
actual kernel overlap still needs a GPU trace.

## Configuration and query selection

TOML accepts `[benchmark]` and `[sirius]`; unknown keys are errors. Paths in
TOML are relative to the TOML directory. CLI paths are relative to the current
working directory. Paths inside the Sirius YAML follow Sirius's own resolution
rules (use absolute paths for storage/spill locations).

```bash
python -m parallel_bench run --config configs/cpu-smoke.toml --queries 6 6 --repetitions 10
python -m parallel_bench run --config configs/duckdb.toml --sql-files queries/a.sql queries/b.sql
python -m parallel_bench run --help
```

| `[benchmark]` option | Default | Meaning |
| --- | --- | --- |
| `backend` | `"duckdb"` | `duckdb` or `sirius` |
| `db`, `scale_factor` | `"data/tpch.duckdb"`, `1.0` | Preparation destination and size; run reads the actual manifest SF |
| `queries` | `[3, 6]` | Two IDs, 1–22; duplicates retain distinct slot identities |
| `sql_files` | `[]` | Two concrete SQL files; takes precedence over configured IDs |
| `warmups`, `repetitions` | `2`, `10` | Rounds, each containing both isolated controls and the concurrent pair |
| `seed` | `0` | Randomizes condition order within each measured round |
| `threads`, `memory_limit` | `4`, `"32GB"` | Same instance-wide limits for isolated and concurrent runs |
| `temp_directory` | `""` | Optional DuckDB spill location; Sirius storage tiers are configured in YAML |
| `output_dir`, `label` | `"results"`, `"run"` | New timestamped artifact directory for every invocation |
| `barrier_timeout_s` | `30` | Maximum wait at the Python barrier |
| `trial_timeout_s` | `300` | External deadline for each reference, diagnostic, or trial, including setup/validation |
| `startup_timeout_s` | `120` | External deadline for imports, extension initialization and metadata collection |
| `validate` | `true` | Compare full answers with untimed CPU reference answers |
| `relative_tolerance`, `absolute_tolerance` | `1e-6`, `1e-8` | Numeric comparison tolerances; integers remain exact |
| `explain` | `false` | Save untimed EXPLAIN plans before warmups; not an execution trace |
| `expected_source_id` | `""` | Expected DuckDB commit prefix, 10–40 hex characters; required for Sirius |
| `tpch_extension` | `""` | Optional local TPC-H extension for preparation |

`[sirius]` accepts `extension_path`, `config_path`, `gpu` (one index or UUID),
`enable_duckdb_fallback` (default false), and `allow_unsigned_extensions`
(default true for local builds).

`prepare` saves all 22 concrete SQL instances inside
`<database>.benchmark.json`, along with dataset ID, SF, generator revision,
and table row counts. Runs reuse these exact strings, not regenerated query
parameters. The actual SQL and hashes are also saved in every run directory.
Pass the same files to both backends when using externally generated SQL.
Existing databases without a manifest can be used with `--sql-files`.

Only a single SELECT/WITH statement per slot is accepted. DuckDB's built-in
Q15 uses a CTE and works even as Q15+Q15. External CREATE VIEW / SELECT /
DROP VIEW scripts must first be expressed as a CTE; rejecting such scripts
prevents shared view-name collisions and accidentally timing the DROP result.
All queries are exploratory TPC-H workloads, not certified TPC-H results.

## Measurement protocol

One child process retains a root connection to one native database instance.
Two caller threads create independent connections using `root.cursor()`.
The isolated controls follow the same connection/barrier/timing path with
one participant. Connection creation and backend settings are outside timing.

Each concurrent caller waits at a two-party `threading.Barrier`. Its action
records a common release timestamp `t0`, then both callers call
`execute(sql).fetchall()`. This is a caller-side barrier: parsing/planning
inside execute, engine scheduling, fetching, and Python result conversion
are included. Actual engine/kernel starts are not synchronized. The runner
records caller arrival, release, submission and completion timestamps using
a monotonic nanosecond clock and reports actual submission skew.

| Metric | Definition |
| --- | --- |
| Completion time `C_i` | `finish_i - t0`; includes caller scheduling delay after release |
| Execute/fetch duration | `finish_i - start_i`; also retained separately |
| Isolated latency `I_i` | Completion time using the one-party version of the same protocol |
| Per-query slowdown | `C_i / I_i`, using isolated controls in the same repetition |
| Aggregate / mean slowdown | `S_0 + S_1` / `(S_0 + S_1) / 2` |
| Pair makespan | `max(C_0, C_1)` |
| Submission skew | `abs(start_0 - start_1)` |

Summaries report distributions and paired slowdowns, plus the separately
labelled ratio of median concurrent/isolated latencies. They only include
complete successful repetition groups; failures never become fast samples.
Warmup observations are retained and marked, but excluded from summaries.

Validation computes untimed CPU reference answers before warmups, then compares
each full result after **both** timed callers finish. It compares sorted row
multisets (including duplicates), allowing numeric tolerance for noninteger
values. This checks CPU/GPU answers and isolated/concurrent consistency, not
agreement with an independent official TPC-H oracle. Very close numeric sort
keys with different associated columns can require manual investigation.

Resources are not halved for concurrent execution. `external_threads=1` is
recorded alongside `threads`; the DuckDB setting is not a strict process CPU
thread cap because caller threads can execute work. CPU affinity is recorded;
use your host's affinity tools to enforce the intended single-socket setup.
Memory limits do not cap every host/Python allocation. Warm runs do not promise
all data fits in memory. There is no cache flushing or randomized SQL generation.

## Artifacts and failures

Each run creates:

- `config.json`, `metadata.json`: resolved configuration, effective engine
  settings, SQL, dataset identity, software revisions, runner source hashes,
  hardware/affinity, relevant environment, and Sirius binary/config hashes.
- `sql/slot-0.sql`, `sql/slot-1.sql`: exact SQL used for the two slots.
- `samples.csv`: individual observations, including warmups, timings, row
  counts, thread IDs, and errors.
- `events.jsonl`: incremental metadata, phase changes, batches and final status.
- `summary.json`, `status.json`: statistics, success/failure, and last phase.
- `engine.log`: captured Python/native engine output; optional `plan-slot-*.txt`
  and `sirius-effective.yaml`.

A supervisor process enforces deadlines without relying on a native query
returning or releasing the GIL. A timed-out or interrupted run terminates its
engine process, preserves prior complete batches and logs, and exits nonzero.
The experiment stops at the first error; later repetitions are not attempted
on a possibly damaged GPU runtime. Timeout rows have no invented timing values.
If one query finishes while its peer hangs, that unfinished batch's result
payloads are lost on termination; earlier complete batches remain available.

The manifest identifies a prepared dataset but is not a content checksum of
the whole database. Do not edit the data between backend runs. Compare SQL
hashes, dataset IDs, resource settings and engine source IDs before interpreting
two runs together. EXPLAIN and overlapping client calls do not establish GPU
kernel overlap; use a separate GPU profiling run for that.

## Tests

```bash
python -m unittest discover -s tests -v
```

The tests use temporary databases and the installed DuckDB package. They check
simultaneous submissions, full-result validation, duplicate Q15, error and
timeout artifacts, revision mismatch handling, and summary calculations.
TPC-H integration tests require a locally loadable TPC-H extension; otherwise
they are explicitly skipped. No GPU test is silently treated as a CPU success.

Sirius execution requires a compatible GPU, driver, Python wheel and extension.
It has not been exercised on this machine; use the matched-build procedure
above for hardware qualification before trusting GPU performance numbers.

## API references used

- [DuckDB connections from multiple Python threads](https://duckdb.org/docs/2.0/guides/python/multiple_threads)
- [DuckDB TPC-H extension and fixed query instances](https://duckdb.org/docs/2.0/core_extensions/tpch)
- [Sirius build/Python compatibility](https://github.com/sirius-db/sirius)
- [Sirius concurrent-query startup configuration](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/concurrent-queries.md)
- [Sirius configuration and fallback settings](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/configuration.md)
