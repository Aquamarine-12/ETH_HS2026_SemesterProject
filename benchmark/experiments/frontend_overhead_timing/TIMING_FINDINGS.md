# Query timing and frontend overhead findings

Experiment run on the cluster on 2026-11-03; findings documented on 2026-11-04.
The supplied [complete console output](observed-output-2026-11-03.txt)
is the evidence for the tables below. Raw CPU/GPU profile excerpts supplied in
chat additionally confirmed DuckDB 2.0.0 and the missing parser field.

## Summary

The benchmark retains complete `execute(sql).fetchall()` latency. A diagnostic
experiment found that reported CPU planning phases usually accounted for less
than 5% of latency, with a few short-query cases approaching 10%. Reported GPU
planning shares were also small. However, SQL parsing was absent from both
profiles, and Sirius-specific planning coverage remains unverified. These are
measurements of the available phases, not an upper bound on all frontend work.

For the initial results, comparable modest GPU planning cost is accepted as a
working assumption, not a verified less-than-10% bound. Prepared statements and
native timers are deferred. Ordinary unprofiled runs remain the primary source
of complete-query latency and QPS.

## Question and setup

The original Python timer includes SQL parsing, binding, optimization, planning,
execution, waiting, necessary reads/transfers, and result fetching. The question
was whether frontend costs were large enough to justify changing this boundary.

The experiment used the existing SF10 database and DuckDB 2.0.0, comparing CPU
and Sirius GPU with N=1 and N=2 connections. Each connection ran Q1 through Q22
sequentially, fetching each answer before submitting the next query. Connections
shared one DuckDB instance and started at a barrier. DuckDB resource settings
were left at their defaults.

Each backend/N combination had three ordinary runs and three profiled runs:
24 fresh Python processes in total. Ordinary/profiled order was reversed in
repetition two. Each process requested database-file page-cache eviction using
`fsync` and `POSIX_FADV_DONTNEED` before opening DuckDB. There was no warmup;
caches could warm within each stream. Eviction remained best effort, not proof
of physical-disk cold reads. No isolated-baseline pass preceded measurement.

## Measurement method

Ordinary runs timed the full `execute(sql).fetchall()` call. Diagnostic runs
split the same interval into `execute_s` and `fetch_s`, retaining their sum as
`wall_s`. DuckDB profiling requested parser, logical planner, optimizer, and
physical planner parent-phase totals. After each complete fetch, the wrapper
retrieved the raw profile; decoding and writing occurred after the workload.
Retrieval was outside the query timer but added gaps before subsequent queries.

For each diagnostic observation, the report calculated:

```text
reported_frontend = sum(available parent-phase times)
reported_share = reported_frontend / wall_s
```

The terminal report showed the median frontend time and the median share times
100, grouped by query, backend, and N. Missing fields were retained as missing,
not replaced with zero. Each N=1 row pooled three observations; each N=2 row
pooled six observations from three processes, not six independent trials.

`P/U` compared the median profiled complete latency with the median ordinary
complete latency for the same query/backend/N. It includes run variation and
changes in stream overlap, not only instrumentation cost. QPS was calculated
from ordinary runs as completed queries divided by barrier-release-to-last-
completion wall time. No planning durations were subtracted from QPS.

## Results

### Reported planning share

The following ranges are across the 22 per-query report rows in each group.
The mean equally weights those 22 median shares; it is not a stream-wide time
fraction or a confidence interval.

| Backend | N | Frontend median range (ms) | Share range | Mean share | Queries above 5% |
| --- | ---: | ---: | ---: | ---: | --- |
| CPU | 1 | 0.60–1.89 | 0.12–9.68% | 2.96% | Q6, Q15, Q19, Q20 |
| CPU | 2 | 0.74–2.38 | 0.13–8.87% | 2.82% | Q6, Q15, Q19 |
| GPU | 1 | 0.74–2.02 | 0.17–3.69% | 1.16% | None |
| GPU | 2 | 0.85–2.39 | 0.17–4.04% | 1.02% | None |

For CPU N=1, representative shares were Q1: 0.12%, Q6: 7.24%, Q15: 9.01%,
and Q19: 9.68%. Short queries made a similar absolute planning cost more visible.
Our slowdown metric weights query ratios equally, so those short queries remain
relevant even though they contribute little to total workload duration.

As a separate approximate view, summing CPU N=1 frontend medians gives 29.27 ms,
versus 3,725.49 ms for summed complete-query medians, approximately 0.79%.
This ratio of sums of medians is not an exact measured stream breakdown and
differs from the equally weighted mean share above.

### Fetching and profiling effects

Q16 had substantial fetch time on both backends:

| Backend | N | Complete latency median (ms) | Fetch median (ms) |
| --- | ---: | ---: | ---: |
| CPU | 1 | 51.35 | 11.54 |
| CPU | 2 | 60.62 | 16.23 |
| GPU | 1 | 62.17 | 9.63 |
| GPU | 2 | 80.31 | 16.85 |

Ratios of these displayed medians put fetch at roughly 15–27% of complete Q16
latency. Timing only `execute()` would omit this interval. Fetch can contain
remaining execution as well as result conversion; the experiment did not
separate those costs. Independently calculated medians need not add exactly.

Most P/U values were near one, but CPU N=2 Q6 reached 1.15, GPU N=2 Q7 reached
1.13, and GPU N=2 Q15 was 0.78. Lower profiled latency does not imply negative
instrumentation cost. Profiling and profile retrieval change stream pacing,
and independent processes can experience different runtime conditions.

### Ordinary throughput

| Backend | N=1 QPS median [min, max] | N=2 QPS median [min, max] | N=2 / N=1 |
| --- | --- | --- | ---: |
| CPU | 5.937 [5.637, 5.958] | 11.317 [11.274, 11.369] | 1.91× |
| GPU | 4.472 [3.819, 4.527] | 7.382 [7.356, 7.528] | 1.65× |

Both backends benefited from two streams; CPU throughput was higher in this
experiment. These throughput ratios are not the benchmark's mean slowdown.
The first GPU N=1 ordinary run was slower than the later two; the output does
not establish whether caching, clocks, other workloads, or another cause was
responsible. Three repeats are exploratory, not a precise uncertainty estimate.

## Coverage and decision

All rows reported three of four requested phase fields. The supplied CPU and
GPU JSON excerpts contain logical planner, optimizer, and physical planner
totals but no parser total. The missing parser field is absent in the engine
output itself, not dropped by our report. It does not mean parsing took zero
time. DuckDB's documented metrics and Python execution boundaries are described
in the [metrics reference](https://duckdb.org/docs/2.0/dev/metrics) and
[Python API](https://duckdb.org/docs/2.0/clients/python/reference/).

Sirius can also generate or rebuild plans inside extension callbacks and its
execution operator. Current upstream has such paths in
[initial preparation](https://github.com/sirius-db/sirius/blob/main/src/sirius_context.cpp)
and [transparent execution](https://github.com/sirius-db/sirius/blob/main/src/transparent/physical_sirius_execution.cpp).
These moving upstream sources do not establish which path the installed binary
used or whether DuckDB's fields already include part of that work. Consequently,
the GPU shares above are not a verified total frontend cost. Simply doubling
the CPU share is a hypothesis, not an empirical GPU bound.

The chosen initial metric remains complete client-visible query latency,
including planning and fetching. Native instrumentation is deferred because
the reported phases are generally modest and equivalent CPU/GPU native timing
would require source builds and completion-boundary validation. If stronger
execution-only claims become necessary, instrument the exact Sirius revision
and retain complete-query timings alongside the new measurements.

## Reproduction

Run [run_timing_experiment.sh](run_timing_experiment.sh) using the
[preserved Python snapshot](README.md).
[TIMING_EXPERIMENT.md](TIMING_EXPERIMENT.md) documents setup and report fields.
The original cluster results are under `results/timing-20261103-181407/`.
Only the supplied console output is currently preserved locally; complete raw
JSON/CSV files and the exact Sirius binary provenance must also be retained
for full reanalysis and environment reconstruction.
