# Timing research: DuckDB and Sirius

Research date: 2026-11-03. No benchmark implementation changes were made.
Scope: public DuckDB 2.0 documentation/source and current upstream Sirius
`main`. The exact revision of the user's installed cluster extension has not
been established. Upstream behavior must be checked against that binary before
implementation; links to `main` can change.

## Recommendation

Keep the existing cold-start procedure while establishing a reliable result.
Use existing profilers for a small diagnostic experiment before adding a new
measurement method. Retain unprofiled end-to-end timing as a reference.

Prepared statements are a plausible low-effort improvement for CPU queries,
but they do not establish an equivalent planning-free GPU measurement:
current Sirius deliberately requests prepared-statement rebinding. If strict
execution-only latency is needed, use validated execution-phase spans or add
small per-query native timers in both engines. Do not silently label a
profiling residual or a GPU kernel sum as query execution latency.

## Define the interval before choosing an API

The current timer encloses `connection.execute(sql).fetchall()`. It includes
SQL frontend work and Python result conversion, as well as actual execution.

For an execution-only metric, a useful proposed boundary is:

- Start after binding, optimization, and CPU/GPU physical plan construction,
  before scheduling executable work.
- Stop when query work has completed and the agreed native result is ready.
- Include scheduling/resource waits during execution, scans, decoding, and
  required CPU/GPU transfers. Decide explicitly whether admission waiting is
  included; recording it separately would make this interpretable.
- Exclude Python result conversion. Specify native result materialization and
  GPU-to-host output handling consistently for both backends.

This is a proposed definition, not an existing verified common API. Native
result construction and Python conversion can overlap execution for streaming
results, so merely subtracting separately timed `fetchall()` is not valid.
DuckDB 2.0 documents that `execute()` can return when the first rows are ready,
and exposes `complete()` to finish a result without reading its rows into
Python. Timing `execute()` alone therefore does not guarantee completion.
[Python API](https://duckdb.org/docs/2.0/clients/python/reference/)

Timing a narrower interval also does not remove frontend CPU contention or
the gaps between executions from the actual workload. Removing planning from
the experiment requires moving it outside the workload, not just changing
which duration is reported.

## Prepared statements

### DuckDB CPU

Prepare the fixed SQL on each worker connection before the start barrier,
then time execution of the prepared statement. SQL `PREPARE`/`EXECUTE` is
available through the current Python connection interface. DuckDB documents
reuse of initial parsing/planning work.
[Prepared statements](https://duckdb.org/docs/2.0/sql/query_syntax/prepared_statements)
and [workload tuning](https://duckdb.org/docs/2.0/guides/performance/how_to_tune_workloads#prepared-statements).

Reuse is conditional. `PreparedStatementData::RequireRebind` checks unresolved
or changed parameter types, catalog identity/version, and an always-rebind
flag. Extension callbacks may request rebinding too. Fixed, nonparameterized,
read-only queries avoid some of these triggers, but the actual workload must
be checked. SQL `EXECUTE` also has its own parsing/binding and dispatch costs.
[Rebind conditions](https://github.com/duckdb/duckdb/blob/v2.0.0/src/main/prepared_statement_data.cpp)
and [EXECUTE binding](https://github.com/duckdb/duckdb/blob/v2.0.0/src/planner/binder/statement/bind_execute.cpp).

Benefit: most original-query frontend work can move outside timing when plans
are reusable. Limit: the measurement remains prepared execute-and-fetch
latency, not strictly engine execution time. Preparation can also touch
metadata or other cached state; it is not the same experiment as raw SQL from
a fresh process.

### Sirius GPU

Current `SiriusContext::OnRebindPreparedStatement` explicitly requests a rebind
for a cached transparent Sirius execution operator; parameterized GPU SELECTs
also trigger this path. Consequently, preparing the SQL does not guarantee
that its GPU planning work stays outside execution.
[Sirius callback](https://github.com/sirius-db/sirius/blob/main/src/sirius_context.cpp#L1937).

`PhysicalSiriusExecution` can consume a validated plan once, but subsequent
executions can rebuild it. Its fallback rebuild can parse, bind, and optimize
the original SQL. Both code paths prevent a general claim that PREPARE alone
gives planning-free GPU timing.
[Transparent execution](https://github.com/sirius-db/sirius/blob/main/src/transparent/physical_sirius_execution.cpp#L233).

Using the explicit `gpu_execution(...)` table function is not a shortcut:
its execution function extracts and generates a fresh plan inside execution.
The logged "Execute query time" surrounds that work and is not automatically
the desired interval.
[Explicit execution source](https://github.com/sirius-db/sirius/blob/main/src/sirius_extension.cpp).

These findings concern inspected upstream code. We have not executed a
prepared-statement experiment against the cluster installation.

## Existing profiling APIs

### DuckDB profiling

DuckDB 2.0 supports selectable metrics through `tracked_metrics`. Useful
categories are query total time, parser time, logical planner time, optimizer
time, and physical planner time. Binding and physical planner subphases nest
inside their parent timings; do not subtract both parents and children.
[Metrics](https://duckdb.org/docs/2.0/dev/metrics).

An implementation could enable profiling separately on each worker, request
only relevant metrics, use `no_output`, and retrieve JSON through
`get_profiling_information()` after fully completing each query and before
submitting another. Unique output files are an alternative. The in-memory API
avoids file races and file writes, but collecting/parsing profiles still adds
work between queries. Use diagnostics separately from primary throughput runs.
[Profiling controls](https://duckdb.org/docs/2.0/dev/profiling).

The source starts total profiling before query planning and ends it during
query cleanup. Thus query total time is not a direct execution-only timer.
[Query preparation](https://github.com/duckdb/duckdb/blob/v2.0.0/src/main/client_context.cpp)
and [query completion](https://github.com/duckdb/duckdb/blob/v2.0.0/src/main/client_context_execution.cpp).

Subtracting verified non-overlapping frontend phases from a containing query
interval can give a diagnostic residual. It is not proof of pure execution:
untracked setup/teardown, streaming consumption, and extension work may remain.
Sirius plan construction inside extension callbacks/operators is a particular
reason not to assume DuckDB's standard frontend metrics cover everything.

Operator timings are accumulated over calls and threads. Their sum, including
the query CPU-time aggregate, is not query wall-clock latency. Overlapping work
must not be counted as serial elapsed time.
[Profiler implementation](https://github.com/duckdb/duckdb/blob/v2.0.0/src/main/query_profiler.cpp).

### EXPLAIN ANALYZE

It runs the query and returns profiling information instead of its original
answer. It is useful for diagnostics but changes the submitted statement and
result path. Verify that the installed Sirius revision actually uses GPU
execution for the EXPLAIN form; do not infer GPU use from a successful call.
It does not by itself settle which total-time boundary excludes planning.
[DuckDB profiling statements](https://duckdb.org/docs/2.0/sql/statements/profiling).

### Sirius Quent traces and native diagnostics

Sirius already exports per-query plans, tasks, queues, threads, and execution
activity as NDJSON. Query labels can identify a particular stream/query/run;
an offline reader would correlate by query ID and derive an execution interval
from verified events. The emitted schema is explicitly experimental.
[Quent telemetry](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/quent-telemetry.md).

The first-operator-start to last-operator-end interval is useful but can omit
admission, initial scheduling, and final result handling. Summed task durations
are not wall time. Validate actual event semantics, including GPU completion,
rather than assuming an event named query begin/end has the desired boundary.

Current C++ runtime diagnostics also expose query identities and lifecycle
timestamps without telemetry. There is no documented Python getter for them
in the inspected documentation; using them may require an extension change.
Their admission/retirement span can contain more than execution, so exposing
them alone does not solve the boundary question.
[Concurrent-query diagnostics](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/concurrent-queries.md#diagnostics).

### Nsight Systems and CUDA events

Nsight Systems traces host activity, CUDA calls, kernels, and transfers. Sirius
has analysis tooling that derives an operator execution window, but its own
profiling workflow warns that profiled timing is perturbed and recommends
unprofiled runs for performance comparisons.
[Sirius profiling workflow](https://github.com/sirius-db/sirius/blob/main/.claude/skills/profile-analyzer/SKILL.md).

CUDA events measure device-stream work. They do not automatically capture
host scheduling or I/O, and one event pair on a Python/default stream cannot
bracket all of Sirius's worker streams. Device-wide synchronization around
each query would couple concurrent queries and alter this experiment. Prefer
verified per-query completion or correlated tracing for this workload.
[CUDA timing guidance](https://docs.nvidia.com/cuda/cuda-c-best-practices-guide/index.html#timing).

## Direct native timers

For a rigorously defined common execution metric, add a small monotonic
per-query interval at the appropriate native boundary, keeping records in
memory and exposing completed records after measurement. Include stream/query
identity. No disk logging is needed in the timed interval.

For CPU, hook after planning but before executor initialization schedules work,
then close the interval when the query's native execution/result production is
finished. For GPU, hook after GPU plan generation, before pipeline scheduling,
and close it only after that query's asynchronous work and agreed output
materialization finish. Avoid a global GPU synchronization.

A generic outer DuckDB executor timer would still include any planning Sirius
does lazily inside its execution operator, so GPU-specific phase accounting is
necessary. Existing native interfaces and diagnostics may reduce the patch,
but their boundaries require inspection in the exact revision to be built.
Maintain the same DuckDB engine build for CPU and GPU, and validate the
instrumented behavior under N=1 and concurrent streams.

## Effort versus benefit

These are engineering judgments, not measured costs or delivery commitments.

| Option | DuckDB CPU effort | Sirius GPU effort | Benefit and limitation |
| --- | --- | --- | --- |
| Keep execute-and-fetch timing | Already implemented | Already implemented | Actual client-visible latency/QPS; includes frontend and Python conversion. |
| Finish results with `complete()` | Low; Python-only | Low integration, verify installed API/path | Removes reading rows into Python; retains planning and native output costs. Changes the result-consumption experiment. |
| PREPARE before the barrier | Low; Python-only | Low API work, potentially high work to guarantee plan reuse | Often removes CPU frontend work; insufficient guarantee for current Sirius. |
| Built-in profiler phases | Low to medium; configure and collect JSON | Medium; first establish extension phase coverage | Best initial diagnosis of frontend share; residual is approximate and profiling perturbs work. |
| Quent execution spans | Not a direct CPU solution | Medium; label, parse, correlate, validate boundaries | Existing GPU activity timeline; must be paired with comparable CPU timing. |
| Nsight Systems | Limited as a matching CPU query timer | Medium setup/analysis, potentially high automation effort | Best detailed GPU diagnosis; heavyweight for every benchmark run. |
| Direct native execution timers | Medium to high | High, including matching source builds and async validation | Most control over a common execution definition and low instrumentation volume. |

A Python-only option is a small change to the current one-file program.
Native instrumentation is a source-build and qualification task, not just
adding two `perf_counter()` calls. Benefit cannot be quantified until the
frontend share is measured on the actual queries and hardware.

## Suggested next experiment

1. Identify the installed DuckDB and Sirius build revisions without changing
   them. Use those sources to verify profiler fields and callbacks.
2. Keep the present cache procedure, database, and query order fixed.
3. Collect an unprofiled reference and a small diagnostic profile for N=1,
   then N=2, with per-query identity and completion verified.
4. Inspect frontend time and GPU plan/rebuild behavior before deciding whether
   prepared statements have sufficient benefit.
5. If strict execution timing is still required, choose verified existing
   spans or the minimal native timer patch and validate boundaries on both
   backends. Keep end-to-end results alongside it for interpretation.

The existing slowdown formula can use either latency definition only when
baseline and comparison use the same definition. Keep them separately named.
QPS remains completed queries over actual workload wall time; do not sum
concurrent query durations or subtract overlapping frontend durations from
the stream makespan to construct an execution-only QPS.
