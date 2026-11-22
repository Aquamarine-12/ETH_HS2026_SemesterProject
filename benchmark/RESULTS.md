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
| `process_id` | PID of this fresh measurement process |
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
combination. Repetitions generate new references. The runner stops on failure,
retains already completed raw files, and writes the index only after the full
sweep succeeds. A failed measurement writes no new result metrics. An old
explicit output file can remain after failure; use new filenames/directories.

## Plot aggregation

Warmup counts and query orders are separated before aggregating samples. A mixed
index produces separate figure sets in `warmup-<count>/seed-<order>/`
subdirectories; a single combination keeps the original figure paths.

For QPS and mean slowdown, every independent repeated run contributes one
value at its connection count. Per-query latency plots first average stream
latencies within each run; per-query slowdown plots average stream latency
ratios against that run's reference. Those summaries then contribute one
value per sample/query, rather than treating streams as independent samples.

The plotter calculates medians and percentile-bootstrap 95% confidence
intervals using 10,000 resamples and random seed 0. Sample rows are resampled
as complete blocks, preserving parameter columns and their existing baseline
associations. The same procedure applies across query columns. A lone sample
is drawn without a confidence bar. Per-query figures have one backend per
panel; connection-count points are offset horizontally within each query
group, without connecting lines. Overall QPS/slowdown figures retain curves.

For per-query figures, `--queries` selects query IDs and `--query-connections`
selects connection counts. Optional TOML `[plot].queries` and
`[plot].connections` provide the same settings via `--config`; explicit CLI
values take precedence. An omitted dimension retains its normal selection.
Selected query columns retain their original within-run averages and paired
sample resampling. Connection filters retain each selected run's reference.
Overall QPS and slowdown figures ignore these per-query filters.

Missing requested counts for a backend or requested query data in a selected
stream cause an error before figure output. Slowdown figures still require
N>=2. Explicit N=1 latency selections can use the saved baseline record.

Slowdown plots require the referenced file and saved baseline latencies to
agree, with matching backend, database, sample, seed, engine version,
and fallback/I/O settings. Both files must agree on whether the warmup stream
count is positive. Missing counts default to zero (cold). Different positive
counts and warmup query orders do not invalidate a baseline comparison.
Older result pairs without an I/O field remain
plottable together, but cannot establish a match with a newly recorded GPU
I/O backend. GPU labels include the recorded I/O backend to keep separate
configurations in separate series. Missing required data, incomplete sample blocks, or
missing files raise an error. Values are never filled with zero, and broken
comparisons are never silently omitted. Baseline-role entries are explicitly
excluded from slowdown plots. All slowdown plots show only N>=2, with a
reference line at factor 1. QPS plots always include N=1 references.

## Current scope

Each measured worker executes its 22-query stream once after synchronized
release. Optional warmup runs an independently sized group of shuffled streams
before that release, with no cache reset or database reopening between phases.
The cold-start reset is file-specific and best effort, as described in the
README. Fresh processes prevent reuse of prior in-process CPU/GPU buffers.

There is no result-row validation or planning-free/prepared-statement timer.
DuckDB chooses its resource defaults. Native GPU execution still needs a
smoke test on the actual cluster after refactoring.
