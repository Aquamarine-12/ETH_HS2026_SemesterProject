# Result files and measurement details

## One result per measurement

The JSON producer consumes `core.BenchmarkResult`. The execution core does not
write result JSON. Each file describes one measured workload with N streams.

| Field | Meaning |
| --- | --- |
| `device` | `cpu` or `gpu` |
| `cpu_model` | CPU model detected on the measurement node, or null |
| `gpu_model` | NVIDIA GPU models reported by `nvidia-smi`, or null if unavailable |
| `database` | Absolute source database path, retained as metadata |
| `sample` | Experiment sample number, starting at 1 |
| `duckdb_version` | Engine version reported by `PRAGMA version` |
| `cpu_fallback` | False, matching current execution settings |
| `io_backend` | `io_uring` or `kvikio` for GPU runs; null for CPU runs |
| `process_id` | Process PID; shared by all measurements within a warm batch |
| `connections` | Number of concurrent streams, N |
| `seed` | Integer shuffle seed, `ASC` (default), `DESC`, or `ASC-DESC` |
| `warmup_streams` | Concurrent warmup streams, independent of N; 0 when disabled |
| `warmup_query_orders` | Shuffled query IDs for each warmup stream; empty when disabled |
| `role` | `baseline` for an unreferenced N=1 run; otherwise `measurement` |
| `baseline_json` | Reference path relative to this JSON file, or null |
| `baseline_latencies_s` | Loaded reference query latencies, or null |
| `total_queries` | Measured queries only: 22N |
| `elapsed_s` | Measurement barrier release to final stream completion |
| `qps` | Measured queries divided by elapsed time |
| `mean_slowdown` | Mean latency factor, present only with a baseline |
| `streams` | Each worker's measured query order and query latencies |

Every `streams` entry contains `stream_id` (zero-based), `query_order` (all 22
query IDs), and `query_latencies_s` (query ID to seconds). JSON object keys
are strings. Baseline latency keys therefore also become strings on disk.
Query-result rows are not saved.

`warmup_streams` defaults to zero, meaning cold cache. For a positive count,
each warmup stream executes all 22 queries and fully fetches the results.
The shuffle seed is the integer measurement seed, or the sample number for
named measurement orders. This gives matching warmup orders across baseline
and comparison runs in a sample. Only measured queries appear in `streams`,
`total_queries`, QPS, and slowdown. Settings, index, and result JSON store
only `warmup_streams`; there is no separate warmup boolean.
For warm sweeps, the count and query orders describe one shared warmup before
the N=1 baseline, not a new warmup before each N. Later Ns inherit the cache
state of the preceding measurements. Result filenames and fields are unchanged.

`io_backend = "kvikio"` means the fixed workaround configuration: one GPU,
`KVIKIO_COMPAT_MODE=ON`, and `KVIKIO_AUTO_DIRECT_IO_READ=0`. The generated
Sirius YAML retains the selected backend, admission count, and GPU topology;
the settings JSON retains the requested selection even for CPU runs, where
it has no effect. Both I/O backends explicitly set `topology.num_gpus: 1`.
GPU comparisons require a baseline with a matching recorded I/O backend.

The old `baseline` and `isolated_latencies_s` fields are now named
`baseline_json` and `baseline_latencies_s`. The old `cache_reset` description
is removed; cache-reset limitations are documented in the README. The plotter
expects the new metadata/field names; the frozen timing experiment retains
its historical format.

## Slowdown

For stream s and query q, let C(s,q) be measured execute-and-fetch latency,
and L(q) the corresponding latency in the separate N=1 baseline:

```text
mean_slowdown = sum(C(s,q) / L(q) for s,q) / (22 * N)
qps = 22 * N / elapsed_s
```

One means unchanged latency on average, 1.2 means 20% higher, and 0.8
means 20% lower. Query ratios receive equal weight, irrespective of their
absolute runtimes. This is not a ratio of summed stream runtimes. Each backend
uses its own reference; this metric is not CPU-versus-GPU speedup.

The plotter expects this factor convention in `mean_slowdown`. It does not
convert results produced with the earlier zero-centered formula.

N=1 baseline records have no slowdown. Explicitly comparing another N=1
invocation against a baseline is still supported, but sweeps do not repeat
N=1 for that purpose. A standalone N>1 run without a baseline can report QPS
and query latencies, but cannot be plotted as a slowdown measurement.

## Files and references

An automatically named single run contains `result.json`, `settings.json`,
and, for GPU execution, `result.sirius.yaml`. With an explicit JSON output
path, settings are saved alongside it as `<stem>.settings.json`.

Sweeps have this structure (additional backends have their
own directories):

```text
sweep-20261104-143000-123456/
    settings.json
    index.json
    gpu/
        warmup-0/
            seed-ASC/
                sample-01/
                    baseline.json
                    baseline.settings.json
                    baseline.sirius.yaml
                    n2.json
                    n2.settings.json
                    n2.sirius.yaml
                    ...
                sample-02/
                    ...
        warmup-5/
            seed-DESC/
                ...
```

`settings.json` records the effective `[run]` and `[sweep]` settings.
`index.json` contains `settings_json` and a `runs` list. Each entry records
sample, backend, connections, warmup stream count, seed, role, `result_json`,
`baseline_json`, and `included`. References here are relative to the index.
`included` determines
whether N=1 was requested for per-query latency plots. The QPS plot always
includes its reference result, even if only N>1 values were requested. References inside each result are relative
to that result instead (typically just `baseline.json`).

All measurements run sequentially using `sys.executable`. One reference is
shared by the requested connection counts within a sample/backend/cache/order
combination. Cold runs use separate processes; warm runs share one process
and one warmup across the baseline and all requested Ns. Repetitions start
fresh processes and generate new references. Progress counts each measured N
even when several Ns share a process. The runner stops on failure,
retains already completed raw files, and writes the index only after the full
sweep succeeds. A failed measurement writes no new result metrics. An old
explicit output file can remain after failure; use new filenames/directories.

## Plot aggregation

The plotting metrics are `total_qps`, `per_query_latency`, `total_slowdown`,
and `per_query_slowdown`. They read the existing JSON fields; the producer
still records `qps`, `mean_slowdown`, and each stream's query latencies.

Plot settings filter devices, cache modes, query orders, connection counts,
and query IDs. Connection filters apply to all metrics; query filters apply
to per-query metrics. `group_by` selects separate series for `device`,
`cache`, and `query_order`. A dimension with multiple selected values must
be grouped. Omitted filters keep all available measurements, and grouping
defaults to all three dimensions. Cold means zero warmup streams; hot means
a positive count.

All selected comparison series share one figure per metric. PNG is always
written; PDF is additionally exported only with `[plot].pdf = true`.
Every figure is written directly to the output folder with its metric,
devices, query orders, cache regimes, and displayed connection counts in the
filename, such as
`total_qps__devices-cpu+gpu__order-ASC__cache-cold+hot__connections-1+2+4+7.png`.
Plotting no longer creates warmup/order subdirectories.
Per-query figures have one point series per condition/connection combination,
with horizontal offsets within each query and no connecting lines. Total
metrics use connection count as the x-axis.

The first `group_by` dimension assigns sequential palettes in sorted group
order: `Blues`, `Oranges`, `Greens`, `RdPu`, then repeat. Remaining grouping
dimensions and per-query connection counts select shades within a palette.
Matching secondary conditions use the same palette position across groups.

For total QPS and slowdown, each repeated run contributes its recorded value.
Per-query latency first averages stream latencies within a run. Per-query
slowdown first averages each stream's latency ratio against that run's
reference. These summaries contribute one value per sample/query, rather
than treating streams as independent samples.

The plotter calculates medians and percentile-bootstrap 95% confidence
intervals using 10,000 resamples and random seed 0. Sample rows are resampled
as complete blocks, preserving parameter columns and baseline associations.
The same procedure applies across query columns. One sample is drawn without
a confidence bar. Missing sample columns and duplicate sample/parameter
entries raise errors before figure output.

Both slowdown metrics require a real N=1 reference with matching backend,
database, query order, engine version, fallback/I/O settings, and exact warmup
stream count. Missing counts default to zero for older cold results. Saved
baseline latencies must match the referenced file. References may come from
an earlier sample when explicitly selected through the benchmark CLI. Shared
warm-batch process IDs do not change grouping or sample aggregation.

Baseline-role entries are excluded from slowdown figures, and both slowdown
metrics show only N>=2 with a factor-1 reference line. Total QPS includes
N=1 references by default. An explicit connection filter controls all metric
points: N=1 is shown for QPS/latency when selected, remains usable as a
reference, and is omitted from the slowdown metrics.

CLI options override `[plot]` TOML fields. The README lists each filter and
includes the CPU/GPU cold/hot and GPU-only comparison examples.

## Current scope

Each measured worker executes its 22-query stream once after synchronized
release. Optional warmup runs an independently sized group of shuffled streams
once before the warm batch's baseline, with no reset or reopening between Ns.
The cold-start reset is file-specific and best effort, as described in the
README. Fresh cold processes prevent reuse of prior in-process CPU/GPU buffers;
warm batches intentionally retain those buffers across their measurements.

There is no result-row validation or planning-free/prepared-statement timer.
DuckDB chooses its resource defaults. Native GPU execution still needs a
smoke test on the actual cluster after refactoring.
