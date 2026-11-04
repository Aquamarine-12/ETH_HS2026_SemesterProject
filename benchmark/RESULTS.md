# Result files and measurement details

## One result per measurement

The JSON producer consumes `core.BenchmarkResult`. The execution core does not
write result JSON. Each file describes one measured workload with N streams.

| Field | Meaning |
| --- | --- |
| `device` | `cpu` or `gpu` |
| `database` | Absolute source database path, retained as metadata |
| `sample` | Experiment sample number, starting at 1 |
| `duckdb_version` | Engine version reported by `PRAGMA version` |
| `cpu_fallback` | False, matching current execution settings |
| `process_id` | PID of this fresh measurement process |
| `connections` | Number of concurrent streams, N |
| `seed` | Integer shuffle seed, `ASC` (default), `DESC`, or `ASC-DESC` |
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
```

`settings.json` records the effective `[run]` and `[sweep]` settings.
`index.json` contains `settings_json` and a `runs` list. Each entry records
sample, backend, connections, role, `result_json`, `baseline_json`, and
`included`. References here are relative to the index. `included` determines
whether N=1 was requested for per-query latency plots. The QPS plot always
includes its reference result, even if only N>1 values were requested. References inside each result are relative
to that result instead (typically just `baseline.json`).

All measurements run sequentially using `sys.executable`. One reference is
shared by the requested connection counts within a sample/backend
combination. Repetitions generate new references. The runner stops on failure,
retains already completed raw files, and writes the index only after the full
sweep succeeds. A failed measurement writes no new result metrics. An old
explicit output file can remain after failure; use new filenames/directories.

## Plot aggregation

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

Slowdown plots require the referenced file and saved baseline latencies to
agree, with matching backend, database, sample, seed, engine version,
and fallback settings. Missing required data, incomplete sample blocks, or
missing files raise an error. Values are never filled with zero, and broken
comparisons are never silently omitted. Baseline-role entries are explicitly
excluded from slowdown plots. All slowdown plots show only N>=2, with a
reference line at factor 1. QPS plots always include N=1 references.

## Current scope

Each worker executes its 22-query stream once after synchronized release.
The cold-start reset is file-specific and best effort, as described in the
README. Fresh processes prevent reuse of prior in-process CPU/GPU buffers.

There is no result-row validation or planning-free/prepared-statement timer.
DuckDB chooses its resource defaults. Native GPU execution still needs a
smoke test on the actual cluster after refactoring.
