# TPC-H stream benchmark

Generate a database once, then measure N concurrent 22-query streams on DuckDB
CPU or Sirius GPU, starting each workload with a cache-eviction request. Repeat runs
with `sweep.py`, then generate Matplotlib figures with `plot.py`.

## Quick start

Run these commands from this directory, inside your Python environment.
`experiment-sf10.toml` selects the cluster's SF10 database and both backends,
with five samples and connection counts 2 through 10. Its single-run default
is CPU execution, and it includes the installed Sirius extension path. Edit it for your installation; all TOML paths are relative to that file.

```bash
# Only if you have not generated this database already.
python benchmark.py generate-table --config experiment-sf10.toml --scale-factor 10

# One isolated reference, followed by one two-connection comparison.
python benchmark.py benchmark --config experiment-sf10.toml --connections 1 \
  --output results/cpu-baseline.json
python benchmark.py benchmark --config experiment-sf10.toml \
  --baseline-json results/cpu-baseline.json

# Five samples per backend, each with its own baseline and N=2 through N=10.
python sweep.py --config experiment-sf10.toml

# Or a short trial: three samples, N=1 and N=2 only.
python sweep.py --config experiment-sf10.toml --samples 3 --connections 1 2

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
# seed = 42
# baseline_json = "results/cpu-baseline.json"

[sweep]
connections = [2, 3, 4, 5, 6, 7, 8, 9, 10]
samples = 5
devices = ["cpu", "gpu"]
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

The sweep runs sequentially, with a fresh Python process per measurement.
Every sample/backend combination gets one N=1 baseline, followed by
its requested N>1 runs. Requested N=1 points reuse that baseline; they do not
trigger a second run or receive an artificial slowdown value. The main entry
point prints a warning explaining this. Sweep references are always generated
automatically, regardless of a single-run baseline configured in TOML.

## Cache and timing

Each measurement requests database-file eviction before opening DuckDB:
`fsync`, followed by `POSIX_FADV_DONTNEED`. **This is best effort**, not proof
of a physical-disk cold start. It does not clear storage-device/server caches;
other readers can refill the shared Linux page cache. No root access is needed.
This caveat lives here rather than in a repeated JSON description field.

Each worker executes its stream once, starting at a shared barrier. There is
no preliminary query pass. Cache contents naturally change during execution;
the eviction request happens before the workload, not between queries.

Each query timer includes `execute(sql).fetchall()`. Overall elapsed time is
measurement-barrier release to the last stream's completion. Setup, cache
reset, output writing, and connection cleanup are excluded. QPS counts
only the measured 22N queries. Slowdown is a latency factor: 1 means unchanged,
1.2 means 20% higher latency, and 0.8 means 20% lower latency.

The [timing findings](experiments/frontend_overhead_timing/TIMING_FINDINGS.md)
explain the retained end-to-end timing method. The preserved profiling
experiment is independent of the active modules and remains unchanged.

## Results and figures

Each result contains effective run metadata, QPS, elapsed seconds, optional
mean slowdown, and every stream's query order and latencies. Baseline links
are relative to the result file. Sweeps also write effective `settings.json`
and an `index.json` linking all samples, including their baseline associations.
Copy the whole sweep directory to plot it on another machine; plotting needs
NumPy and Matplotlib, but no DuckDB, Sirius, or GPU.

The plotter produces PNG and PDF figures for QPS versus N, mean slowdown
versus N, and per-query latency/slowdown. Connection-count plots use median
curves with 95% bootstrap confidence bars. Per-query figures use separate
backend panels and unconnected median points, with connection counts placed
side by side within each query group. Their confidence bars use the same
bootstrap method. Streams are first summarized
within each run; bootstrap resampling preserves sample blocks and references.

QPS plots always include the N=1 baseline, even if the sweep starts at N=2.
Slowdown plots show only N>=2, with a reference line at factor 1. Per-query
latency plots retain the connection counts requested by the sweep.
Required missing baseline files or data cause an error, with no substituted
values or dropped measurements. To plot baseline-only results, request:

```bash
python plot.py path/to/index.json --metrics qps latency
```

A single result JSON is also accepted; it supplies a single measured sample.
Use `--output path/to/figures` to choose the figure directory.
See [RESULTS.md](RESULTS.md) for the JSON fields, formula, and output layout.

## Code map

- `benchmark.py`: entry point, progress/reporting, and writing successful runs.
- `core.py`: typed settings/results, database setup, workers, and timing.
- `cli.py`: argument parsing and TOML settings, plus sweep settings.
- `json_io.py`: JSON readers/writers and relative result references.
- `sweep.py`: repeated subprocess measurements and the completed index.
- `plot.py`: result aggregation, paired bootstrap medians, and Matplotlib.

GPU workers keep `enable_duckdb_fallback = false`; unsupported GPU execution
fails. The generated per-run Sirius YAML sets startup-only admission to N.
This setting has no equivalent runtime `SET`/connection option, so the YAML
must exist before loading Sirius. Other Sirius defaults, including Quent
telemetry, remain intact. Scheduler GPU visibility is preserved.

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
