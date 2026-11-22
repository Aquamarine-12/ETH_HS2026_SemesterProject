# TPC-H stream benchmark

Generate a database once, then measure N concurrent 22-query streams on DuckDB
CPU or Sirius GPU, evicting before each cold run or shared warm batch. Repeat runs
with `sweep.py`, then generate Matplotlib figures with `plot.py`.

## Quick start

Run these commands from this directory, inside your Python environment.
`sf10-benchmarks.toml` selects the cluster's SF10 database and both backends,
with five samples and connection counts 2 through 10. Its single-run default
is CPU execution, and it includes the installed Sirius extension path. Edit it for your installation; all TOML paths are relative to that file.

```bash
# Only if you have not generated this database already.
python benchmark.py generate-table --config sf10-benchmarks.toml --scale-factor 10

# One isolated reference, followed by one two-connection comparison.
python benchmark.py benchmark --config sf10-benchmarks.toml --connections 1 \
  --output results/cpu-baseline.json
python benchmark.py benchmark --config sf10-benchmarks.toml \
  --baseline-json results/cpu-baseline.json

# Five samples per backend, each with its own baseline and N=2 through N=10.
python sweep.py --config sf10-benchmarks.toml

# Or a short trial: three samples, N=1 and N=2 only.
python sweep.py --config sf10-benchmarks.toml --samples 3 --connections 1 2

# GPU I/O workaround for the tested cluster Sirius binary; CPU is unchanged.
python sweep.py --config sf10-benchmarks.toml --io-backend kvikio \
  --samples 3 --connections 2 4

# Use the index path printed by sweep.py.
python -m pip install -r requirements-plot.txt
python plot.py results/sweep-YYYYMMDD-HHMMSS-ffffff/index.json
```

Without an explicit output filename, each run or sweep creates a new directory
under `results/`. An explicit `--output` still writes exactly that JSON path.
A single N>1 run needs `--baseline-json` to report slowdown; otherwise it
reports QPS and query timings only. `--baseline` remains a CLI alias.

For CPU, pass `--device cpu` to a single run, or set `device = "cpu"` in TOML
before a sweep.

## Configuration

Built-in defaults are overridden by `[run]` TOML values, then explicit CLI
arguments. CLI paths are relative to your working directory; TOML paths are
relative to the TOML file. Both expand `~`.

```toml
[run]
db = "data/sf10.duckdb"
device = "gpu"
connections = 2
sirius_extension = "~/.local/lib/sirius/sirius.duckdb_extension"
results_dir = "results"
# io_backend = "kvikio"  # GPU only; default: "io_uring"
# seed = 42
warmup_streams = 0
# baseline_json = "results/cpu-baseline.json"

[sweep]
connections = [2, 3, 4, 5, 6, 7, 8, 9, 10]
samples = 5
devices = ["cpu", "gpu"]
warmup_streams = [0]  # Use [0, 5] to sweep cold and warm cache.
seeds = ["ASC", "DESC", "ASC-DESC", 42]
```

The default `seed = "ASC"` gives identical Q1–Q22 order in every stream.
An integer enables
reproducible shuffling. Use `seed = "DESC"` for Q22–Q1 in all streams, or
`seed = "ASC-DESC"` to alternate ascending and descending streams, starting
with ascending. The same values work with `--seed`. Different query orders
have different cache histories. DuckDB chooses its internal resource defaults; connections count
client streams, not engine worker threads.

Supply connection counts through `[sweep].connections` or the sweep CLI
`--connections` option; there is no built-in sweep connection list. Table
generation requires an explicit `--scale-factor`.

The sweep runs sequentially. Cold measurements each use a fresh Python process.
Warm measurements use one fresh process per sample/backend/warmup-count/order
combination: warm up once, measure N=1, then measure the requested larger Ns in
ascending order without resetting caches or reopening the engine.
Every sample/backend/cache/order combination gets one N=1 baseline, followed by
its requested N>1 runs. Requested N=1 points reuse that baseline; they do not
trigger a second run or receive an artificial slowdown value. The main entry
point warns about an unreferenced single N=1 run. Sweep references are generated
automatically, regardless of a single-run baseline configured in TOML.

`[sweep].warmup_streams` and `[sweep].seeds` are lists, like `devices` and
`connections`. The sweep runs every combination. If omitted, each list falls
back to the corresponding `[run]` setting. Explicit CLI lists override TOML:

```bash
python sweep.py --config sf10-benchmarks.toml --warmup-streams 0 5 \
  --seed ASC DESC ASC-DESC 42
```

Zero warmup streams means cold cache; a positive count runs that many warmup
streams. The default is zero. `--seed` (also `--seeds`) accepts named orders or
integer shuffle seeds. Each combination has its own baseline and output directory.
Progress counts measured benchmarks, including baselines, rather than child
processes. The sweep reports before each child starts; a warm batch advances
the count after all its measurements finish and saves each result separately.

### GPU I/O backend

`--io-backend io_uring` (default) selects Sirius's native input path. The
generated YAML calls this backend `sirius`, the name used by older builds
and retained as an alias for `native` in newer builds. It requires usable
kernel `io_uring`; a node with `kernel.io_uring_disabled=2` blocks this path.

`--io-backend kvikio` selects the tested workaround: one GPU, POSIX file
reads, and Linux page-cache buffering. The runner sets `KVIKIO_COMPAT_MODE=ON`
and `KVIKIO_AUTO_DIRECT_IO_READ=0` before loading Sirius; no manual exports
are needed. Admission accommodates the larger of measurement N and the enabled
warmup stream count, so multiple connections share the
single GPU. CPU query fallback remains disabled. Selecting this option on a
CPU run has no effect on execution.

Both single runs and sweeps accept the option. Set `io_backend = "kvikio"`
in `[run]` to avoid repeating it; an explicit CLI option overrides TOML.
Sweeps pass the selection to each fresh child process, including baselines.

KvikIO requires an installed Sirius build that still supports that backend;
recent upstream code rejects it. The cluster binary tested in this chat
completed all 22 queries with the workaround. See the
[Sirius configuration reference](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/configuration.md#scan-manager--io-configuration)
and [KvikIO runtime settings](https://docs.nvidia.com/kvikio/latest/runtime_settings/).

The I/O path can change timing and contention, so each GPU JSON records
`io_backend`, and GPU plot labels include it. Use a baseline from the same
I/O backend; regenerate older GPU baselines whose I/O selection was not
recorded. Both I/O backends explicitly use one GPU, shared by all query streams.

## Cache and timing

Each cold measurement or warm batch requests eviction before opening DuckDB:
`fsync`, followed by `POSIX_FADV_DONTNEED`. **This is best effort**, not proof
of a physical-disk cold start. It does not clear storage-device/server caches;
other readers can refill the shared Linux page cache. No root access is needed.
This caveat lives here rather than in a repeated JSON description field.

By default, each worker executes its stream once, starting at a shared barrier.
Cache contents naturally change during execution; the eviction request happens
before the workload, not between queries.

For a standalone warm-cache measurement, use `--warmup --warmup-streams 5`.
Each warmup stream executes and fully fetches all 22 queries in shuffled order,
independently of measurement N. The default count is zero, meaning cold cache.
All warmup finishes before measurement starts, with the same database instance
and connection pool kept open and no intervening cache reset. This warms shared
engine caches but does not guarantee steady state or full RAM/VRAM residency.

Within a warm sweep batch, that warmup runs only once before the N=1 baseline.
Later connection counts inherit the cache state left by the baseline and earlier
measurements. Each new sample/backend/warmup-count/order combination starts a
fresh process. A standalone multi-count warm batch uses the same workflow:

```bash
python benchmark.py benchmark --db data/sf10.duckdb \
  --warmup --warmup-streams 5 --connections 1 2 3 4 5 \
  --output results/warm-batch/baseline.json
```

Without `--baseline-json`, N=1 is measured first, even if omitted from the list.
The command writes the baseline at `--output` and `n2.json`, `n3.json`, etc.
beside it. Without an explicit output path, it creates a fresh run directory.
Both single-count calls and warm batches accept an external N=1 reference
with `--baseline-json`. Its warmup stream count must match exactly. With an
external reference, a batch measures only the requested Ns in ascending order;
`--output` names the first result, with later `nN.json` files beside it.

Use a warm baseline for warm measurements and a cold baseline for cold ones.
For example, to enable warmup throughout a sweep:

```bash
python sweep.py --config sf10-benchmarks.toml --warmup-streams 5
```

The benchmark CLI keeps `--warmup` / `--no-warmup` as enable/disable controls
for the configured count. Without `--warmup`, the effective count is zero.
Those flags are resolved during CLI settings loading; internal settings and
JSON store only `warmup_streams`. Sweeps use count lists directly. Shuffling
uses an integer `--seed`, otherwise the sample number, and actual warmup orders
are recorded in JSON.

Each query timer includes `execute(sql).fetchall()`. Overall elapsed time is
measurement-barrier release to the last stream's completion. Setup, cache
reset, warmup, output writing, and connection cleanup are excluded. QPS counts
only the measured 22N queries. Slowdown is a latency factor: 1 means unchanged,
1.2 means 20% higher latency, and 0.8 means 20% lower latency.

The [timing findings](experiments/frontend_overhead_timing/TIMING_FINDINGS.md)
explain the retained end-to-end timing method. The preserved profiling
experiment is independent of the active modules and remains unchanged.

## Results and figures

Each result contains effective run metadata, QPS, elapsed seconds, optional
mean slowdown, and every stream's query order and latencies. Baseline links
are relative to the result file. Sweeps also write effective `settings.json`
and an `index.json` linking all samples and references. Copy the whole sweep
directory to plot elsewhere; plotting needs NumPy and Matplotlib, not DuckDB
or a GPU. Shared warm batches may have the same process ID across several Ns;
plot aggregation uses the recorded sample IDs and references.

The four plotting metrics are:

| Metric | Figure |
| --- | --- |
| `total_qps` | Total queries per second versus connection count |
| `per_query_latency` | Latency for each selected query |
| `total_slowdown` | Mean slowdown factor versus connection count |
| `per_query_slowdown` | Slowdown factor for each selected query |

All four are produced by default, each as PNG and PDF directly in one `plots/`
directory beside the source JSON. Filenames include the query orders, cache
regimes, and connection counts actually plotted, for example
`total_qps__order-ASC__cache-cold+hot__connections-1+2+4+7.png`.
Multiple values use `+`; PNG and PDF share the same basename. There are no
automatic cache/order subdirectories. `--output DIR` selects another folder.

Each run first contributes one stream-averaged value per query. Figures show
medians across samples and 95% bootstrap confidence bars, preserving whole
sample rows and their baseline associations. Total metrics use connected
curves; per-query figures use unconnected, horizontally offset series for
condition/connection combinations. Slowdown is a factor, with a reference
line at 1. The renamed plotting metrics read the existing `qps`,
`mean_slowdown`, and per-query JSON fields.

### Select and compare measurements

`[plot]` config values select the measurements and define separate series.
For CPU/GPU with cold/hot cache, giving four lines at one query order:

```toml
[plot]
metrics = ["total_qps"]
devices = ["cpu", "gpu"]
cache_modes = ["cold", "hot"]
seeds = ["ASC"]
group_by = ["device", "cache"]
# connections = [1, 2, 3, 4, 5]
```

The supplied `plot-comparison.toml` contains this example:

```bash
python plot.py path/to/index.json --config plot-comparison.toml
```

For two GPU cold/hot lines, set `devices = ["gpu"]` and
`group_by = ["cache"]`. For query-order comparisons, select multiple `seeds`
and include `query_order` in `group_by`. Seeds accept `ASC`, `DESC`,
`ASC-DESC`, or integer shuffle seeds. Cache mode is derived from the recorded
warmup stream count: zero is cold, a positive count is hot.

| Config field | CLI option | Effect |
| --- | --- | --- |
| `metrics` | `--metrics` | Metrics to generate; default: all four |
| `group_by` | `--group-by` | Separate series by `device`, `cache`, `query_order`; default: all three |
| `devices` | `--devices` | Select `cpu`, `gpu`, or both |
| `cache_modes` | `--cache-modes` | Select `cold`, `hot`, or both |
| `seeds` | `--seeds` | Select named query orders or integer shuffle seeds |
| `connections` | `--connections` | Filter connection counts for every metric |
| `queries` | `--queries` | Filter query IDs for per-query metrics |

CLI selections override the corresponding TOML values. Omitted filters keep
all available values. If a selected dimension has multiple values, include
it in `group_by` so those conditions are separate series. Otherwise, filter
it to one value. Each selected connection count adds a series to per-query
figures; total figures use connection count on the x-axis.

For example, select GPU cache comparisons with three queries and two Ns:

```bash
python plot.py path/to/index.json --devices gpu --cache-modes cold hot \
  --seeds ASC --group-by cache --connections 2 5 --queries 1 6 22
```

`--query-connections` remains an alias for `--connections`; both now filter
all four metrics. Query filters affect only the two per-query metrics.
By default, total QPS includes N=1 baselines even when the sweep requested
only N>1. An explicit connection filter controls whether N=1 is displayed.
Per-query latency includes an otherwise unplotted baseline when N=1 is
explicitly selected. Both slowdown metrics show only N>=2; N=1 is ignored
in their connection selection and remains available as the reference.

Missing requested data, ambiguous grouping, incomplete sample blocks, and
incompatible references cause errors before any figures are written. Baseline
checks include exact warmup count, backend, database, query order, engine,
fallback/I/O metadata, and saved latencies. An explicitly supplied reference
may come from an earlier sample, as supported by the benchmark CLI.

A single result JSON is also accepted. For a baseline-only source, select
metrics that do not need a slowdown comparison:

```bash
python plot.py path/to/baseline.json --metrics total_qps per_query_latency
```

Figure titles show cache mode and query orders. Their hardware subtitle uses
CPU/GPU models saved on the measurement node; it never detects the plotting
machine's hardware. Older results without these fields show "not recorded".
See [RESULTS.md](RESULTS.md) for the JSON fields and aggregation formula.

## Code map

- `benchmark.py`: entry point, reporting metrics, and writing successful runs.
- `core.py`: typed settings/results, database setup, workers, and timing.
- `cli.py`: argument parsing and TOML settings, plus sweep settings.
- `json_io.py`: JSON readers/writers and relative result references.
- `hardware.py`: CPU/GPU model detection on the measurement node.
- `sweep.py`: cold-run/warm-batch subprocesses, progress, and the completed index.
- `plot.py`: result aggregation, paired bootstrap medians, and Matplotlib.

GPU workers keep `enable_duckdb_fallback = false`; unsupported GPU execution
fails. The generated per-run Sirius YAML sets startup-only admission to the
larger of the maximum measured N and the warmup stream count for that process.
This setting has no equivalent runtime `SET`/connection option, so the YAML
must exist before loading Sirius. The YAML also selects the I/O backend and
sets `topology.num_gpus: 1` for both backends. Other Sirius defaults, including
Quent telemetry, remain intact. Scheduler GPU visibility is preserved.

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
