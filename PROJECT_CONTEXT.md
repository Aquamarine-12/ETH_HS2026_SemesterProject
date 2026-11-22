# Semester project handoff

Saved at the user's request on 2026-10-29. This is a durable summary of the
conversation and implementation decisions, not a verbatim chat transcript.
Read the current source and README before changing behavior; update this
document when important decisions change.

## Project and sources

This ETH semester project / mini-thesis compares CPU execution in DuckDB with
GPU execution through Sirius using concurrent TPC-H query streams.

- Project PDF: `/home/mauve/ETH/HS 2026/Semester project/CPU_vs_GPU_DB_anon.pdf`.
- Original task discussion supplied by the user:
  <https://chatgpt.com/share/6ae0fb49-ce08-83ed-a97f-7664ab44919d>.
- Current implementation: [benchmark/benchmark.py](benchmark/benchmark.py).
- Commands, installation, and measurement details:
  [benchmark/README.md](benchmark/README.md).
- Persistent collaboration preferences: [AGENTS.md](AGENTS.md).

The user has a local Git repository and a GitHub remote, and accesses an Ubuntu
24.04 cluster over SSH. Cluster examples used
`~/projects/ETH_HS2026_SemesterProject/benchmark` and a `tpch-bench` environment.
Cluster commands and installation instructions were supplied, but the agent
has not installed anything on or validated GPU execution on that cluster.

## Scope and architecture decisions

An earlier implementation had more infrastructure: configuration classes,
backend abstractions, validation, metrics/artifact modules, hashes, and several
files. The user found it noisy and explicitly authorized starting over because
that work was already committed. Those abstractions are not requirements to
restore. The old throwaway prototype is outside the current benchmark's scope.

The user briefly considered multiple modules, then explicitly chose one file
for now, to decide how to split it after seeing the core code. There are only
two commands: `generate-table` and `benchmark`. No large config class, extra
execution modes, result validation, or general experiment framework is wanted
yet. Build the research methodology iteratively.

The code uses type aliases for query collections, a `Device` enum, and a small
`StreamResult` dataclass. These clarify the core logic without recreating the
old framework. Functions should have concise docstrings, annotations, useful
comments, and generally fewer than 40 lines. The user requested Google Python
style, with inspiration from Google's C++ style guide.

Do not use Git unless explicitly asked. The user performs Git operations.
The repository's ignore rules already cover `.venv` directories,
`.specstory/*`, Python caches, DuckDB database files and associated files,
and generated benchmark data/results. Large database files caused a slow
`git add` earlier; do not add databases to version control.

## Workload and DuckDB API

- Generate TPC-H tables once into a persistent database using DuckDB's `tpch`
  extension and `CALL dbgen(sf=...)`.
- Obtain the 22 fixed SQL queries from `tpch_queries()`.
- Create N worker connections using `root.cursor()`. They have independent
  connection contexts and share one DuckDB database instance.
- Use a `ThreadPoolExecutor` with one worker per connection. Each connection
  executes its complete stream sequentially, fetching the whole result before
  submitting the next query.
- Each stream contains all 22 queries. Without `--seed`, all streams use
  Q1 through Q22. An explicit seed creates reproducible per-stream
  permutations using one seeded random generator.
- Release streams together at one starting barrier. There is no barrier
  between later queries. N=1 follows the same worker/connection path as N>1.
- Leave DuckDB's resource settings at engine defaults. An earlier suggestion
  to default memory to 32 GB was superseded by this decision. Connections are
  client streams, not DuckDB's internal worker thread count.
- No prepared statements, warmup, repetitions, or validation are implemented.

DuckDB is embedded in the Python process; there is no database server or
network query submission. Each per-query timer currently wraps
`connection.execute(sql).fetchall()`. It therefore includes Python/API costs,
parsing, binding, optimization/planning, execution/queueing, necessary data
reads and transfers, and result fetching/conversion. The starting barrier
synchronizes client workers, not the instant execution operators begin.

## Metrics and their evolution

Let `L_q` be query q's latency from a separate N=1 baseline, and `C_s,q` its
latency in stream s of the measured run.

```text
qps = 22 * N / elapsed_s
mean_slowdown = sum(C_s,q / L_q) / (22 * N) - 1
```

`elapsed_s` runs from the starting barrier's release to the last stream's
completion. Setup, cache reset, and connection cleanup are excluded.

The slowdown equally weights all query latency ratios across all streams;
it is not a ratio of total stream runtimes. Zero means unchanged latency,
`+0.5` means 50% higher latency on average, and `-0.2` means 20% lower. The
user explicitly requested an absolute number, not a percentage: no factor of
100. Preserve negative measurements rather than clamping them.

Each backend has its own isolated baseline. This slowdown measures the effect
of concurrency within that backend; it is not CPU-versus-GPU speedup.

Earlier `aggregate_slowdown` averaged a sum of 22 ratios across N streams,
so its no-change value was 22. Convert it with `aggregate_slowdown / 22 - 1`.
A short-lived `mean_slowdown_percent` field used a factor of 100; divide that
by 100. Current output uses `mean_slowdown` only when a baseline is supplied.

Per-query concurrent latencies and loaded isolated latencies are retained in
JSON, together with every stream's order. This was an explicit user request.

## Cold-start mechanism: latest implemented work

The old program timed isolated queries and then concurrent streams in the
same invocation. The user observed negative slowdown even with N=1, including
at SF10. This suggested a measurement bias from the isolated pass warming
caches for the later pass, although negative values alone do not prove its
cause. The user's goal is an N=1 comparison near zero without a systematic
negative bias. This is an experimental goal, not a guaranteed outcome.

The user chose to address caching first, then revisit timing/barriers and
prepared statements. Only the cold-start mechanism was authorized for now.

Current behavior:

1. Each `benchmark` invocation performs one measured workload. There is no
   hidden preliminary isolated pass.
2. Run N=1 without `--baseline` in one Python process to save reference
   per-query times and QPS. It does not invent a zero slowdown.
3. Run another fresh process with `--baseline results/reference.json` and
   the desired N. The baseline's first stream supplies `L_q`; those queries
   are not rerun before measurement.
4. Before opening DuckDB, `reset_database_cache` opens the database file,
   calls `os.fsync`, then requests whole-file Linux page-cache eviction via
   `os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)`.
5. Each fresh CLI process creates a new DuckDB instance and, for GPU, a new
   Sirius runtime. Calling the Python function repeatedly in one process is
   not the documented fresh-process workflow.

Flushing first makes dirty pages eligible for eviction. This is a user-local,
file-specific, best-effort request: it does not guarantee physical-disk cold
reads or clear storage controller/server caches. Other processes can refill
the same file's shared page cache. The program records the cache reset as
unverified; it does not inspect page residency during production runs.
No root access, global `drop_caches`, or fabricated success fallback is used.
Cache-reset failures propagate rather than silently producing new results.

The reset is before the stream, not before every query. Caches naturally warm
while Q1 through Q22 execute. Identical query order is the recommended starting
point: shuffled streams have different cache histories and would need matched
isolated references to isolate concurrency cleanly.

Baseline files should use the same backend, database, and query order, and
should be regenerated with this workflow rather than reused from the old
combined-run implementation. The code trusts users to choose compatible
files; it does not implement a configuration compatibility framework.

## Sirius behavior

The default local extension path is:

```text
$HOME/.local/lib/sirius/duckdb-2.0.0/sirius.duckdb_extension
```

`--sirius-extension PATH` overrides it. Earlier instructions and code used
inconsistent unversioned/relative paths and caused `FileNotFoundError` on the
cluster. The default now matches the documented install path. An argument
cannot create a missing binary: locate the actual uncompressed extension or
install it first.

Before loading Sirius, the program writes the output basename with suffix
`.sirius.yaml`, containing:

```yaml
sirius:
  max_concurrent_queries: N
```

It sets `SIRIUS_CONFIG_FILE` to this file's absolute path, then loads the
unsigned local extension. Admission means the maximum number of active
queries; setting it to N permits all N streams to have a query active.
The YAML is necessary because this setting is startup-only in Sirius's
documented DuckDB interface, with no corresponding runtime SQL setter or
`duckdb.connect(config=...)` setting. The user explicitly requested an inline
comment explaining this; it is in `load_sirius`.

The generated file replaces inherited configuration selection and leaves
other Sirius settings at their defaults. Scheduler-provided GPU visibility
is preserved. The root connection sets `gpu_execution = false` for metadata
queries. Timed worker connections explicitly set `gpu_execution = true` and
`enable_duckdb_fallback = true`.

CPU fallback being enabled is a user edit to preserve. GPU-labeled runs can
include CPU fallback; they do not prove all operators or queries ran on GPU.

Sirius's default Quent telemetry is currently left enabled. Its local traces,
normally in `telemetry_data/`, describe plans, operators, tasks, threads,
queues, and timing; they are separate from our benchmark JSON. The README
explains disabling it with `sirius.telemetry.enable_quent: false`. The user
has not requested that behavior change, so do not silently apply it.

## Installation decisions

All installation is private to the user's account on Ubuntu 24.04, with no
machine-wide packages installed by this project. Python 3.11+ is required.
The README installs `duckdb==2.0.0` into a venv and a matching Sirius extension
under the versioned path above. CPU and GPU must use the same compatible
DuckDB engine/environment for a fair comparison. Verify the actual installed
pair by loading Sirius inside that environment before running benchmarks.

The documented Sirius download used its `latest` release, described as
`13dbefe` and DuckDB 2.0.0 when checked in this conversation. That release tag
is moving: this is a historical observation, not an immutable version pin.
Recheck official release compatibility before future installation updates.
Use an allocated NVIDIA GPU node and a binary appropriate to the architecture
and driver/CUDA support. Full commands and upstream links are in the README.

## Result files

Current JSON fields include device, process ID, N, seed, baseline path or
null, cache-reset method, total queries, elapsed seconds, QPS, optional mean
slowdown, loaded isolated latencies or null, and each stream's order and
per-query latencies in seconds. JSON query IDs are string keys.

Output is written only after successful measurement. Reusing a filename
replaces it on success; failure can leave an older file in place. Choose new
output names for separate experiments. GPU startup YAML is a separate file
and can be written before measurement succeeds.

## Validation performed and limits

The cold-start implementation was checked using the existing local prototype
venv's DuckDB 1.5.6, not the documented cluster DuckDB 2.0.0/Sirius pair.

- Generated an SF0.01 database and ran separate-process N=1 baseline, N=1
  comparison, and N=2 comparison with all 22 queries per stream.
- Checked different process IDs, exact loaded baseline timings, and recomputed
  QPS and slowdown from the saved JSON.
- Checked that N=2 executes only its two measured streams, with no hidden
  isolated pass, and that cache reset precedes opening DuckDB.
- Checked flush/eviction call order and propagation of cache-reset failures.
- Independently observed page eviction on a local 8 MiB file using `mincore`:
  2048 resident pages before the request, zero afterward. This does not prove
  eviction behavior for the cluster filesystem or establish disk-cold reads.
- Checked syntax, annotations/docstrings, function/line lengths, and README
  shell block syntax. No permanent testing framework was introduced.

One illustrative local smoke test still produced roughly -0.084 slowdown
for N=1 and +0.006 for N=2 at SF0.01. One pass on a tiny dataset is noisy;
the implementation does not guarantee that all N=1 results are nonnegative.
No actual GPU/cluster validation has been performed by the agent.

Earlier user-reported SF0.01/N=4 results, under the old metric/workflow, were
CPU 540.672 QPS with aggregate slowdown 17.703, and GPU 84.851 QPS with
aggregate slowdown 19.955. These are historical exploratory observations,
not results from the new cold-start method or a validated comparison.

## Immediate next steps and deferred work

Run separate-process N=1 baseline and N=1 comparison on the real cluster,
then N=2 or more. Use fresh output filenames, a separate baseline for each
backend, and identical query order while examining cache effects.

From the `benchmark` directory, for the SF10 database already used there:

```bash
python benchmark.py benchmark --db data/sf10.duckdb --connections 1 \
  --output results/cpu-baseline.json
python benchmark.py benchmark --db data/sf10.duckdb --connections 1 \
  --baseline results/cpu-baseline.json --output results/cpu-n1.json
python benchmark.py benchmark --db data/sf10.duckdb --connections 2 \
  --baseline results/cpu-baseline.json --output results/cpu-n2.json
```

For GPU, add `--device gpu`, use GPU-specific output/baseline files, and
override `--sirius-extension` if the binary is installed elsewhere.

After assessing cache comparability, return to the user's timing/barrier
question and prepared statements for both DuckDB and Sirius. Preparing queries
may remove some frontend planning work, but does not eliminate execution
dispatch, result fetching, scheduling, I/O, or GPU transfers; exact reuse and
Sirius support must be verified before implementation. Hot-cache warmup rounds,
repeated trials, result validation, telemetry changes, and richer experiment
infrastructure remain deferred.

The user earlier requested a reminder to revisit walkthrough points 4–10.
Their complete original numbered text is not available in this handoff;
do not invent its contents or claim it has already been reviewed.

## Memory status at the time of saving

Local Codex configuration now has `features.memories`,
`memories.use_memories`, and `memories.generate_memories` set to true.
`~/.codex/memories/` is accessible but was empty when checked. This session
has no exposed tool to force a write to the automatic memory store. These
repository files provide immediate durable context; they are not a claim that
automatic memory generation has already completed. No Git commands were used
to save them.

## Update: 2026-11-03 timing research

The user reports that repeated cold N=1 comparisons are approximately centered
at zero, despite individual GPU runs varying. They now prioritize a reliable
end-to-end result and understanding execution timing before trying more cache
scenarios. A one-pass warmup flag was planned but has not been implemented.

The installed cluster Sirius binary was found at the unversioned path
`$HOME/.local/lib/sirius/sirius.duckdb_extension`; pass it explicitly with
`--sirius-extension`. The versioned default remains in the local benchmark.

The current source has `enable_duckdb_fallback = false`, reflecting a user edit
since the original handoff. Preserve the actual source setting. Earlier notes
and instructions describing enabled fallback are historical and outdated.

The user requested research, not implementation, on internal execution timing
and whether prepared statements exclude planning. Findings and primary sources
are saved in [benchmark/experiments/frontend_overhead_timing/TIMING_RESEARCH.md](benchmark/experiments/frontend_overhead_timing/TIMING_RESEARCH.md).
An important source-level finding is that current upstream Sirius explicitly
requests prepared GPU statement rebinding and can rebuild plans at execution.
Do not claim that PREPARE guarantees planning-free timing on both backends.
DuckDB profiling phases and Sirius tracing/native timing hooks have different
boundaries; the exact installed extension revision still needs verification.
No benchmark code or Git operations were changed for this research.

## Update: timing experiment implementation

The user authorized implementing the temporary profiling wrapper and supplying
cluster commands. `benchmark/profile_timing.py` reuses `benchmark.py` with
temporary connection/stream hooks; the core benchmark file remains unchanged.
It preserves current backend settings, times execute and full fetch separately,
and retrieves a raw per-query engine profile after timing and before the next
query. Profiles are stored in per-stream memory and written after the workload.
Diagnostic results are explicitly labeled because collection changes stream
pacing and profiled QPS is not the primary performance result.

`benchmark/summarize_timing.py` normalizes legacy and DuckDB 2.0 metric names,
preserves unavailable phases, and prints per-query medians plus ordinary-run
QPS ranges. It writes `timing-summary.csv` with phase medians and latency
ranges. Raw diagnostic profiles carry stream/query IDs and execute/fetch/full
intervals. GPU frontend coverage remains partial until verified on the cluster.

Commands in [benchmark/experiments/frontend_overhead_timing/TIMING_EXPERIMENT.md](benchmark/experiments/frontend_overhead_timing/TIMING_EXPERIMENT.md)
run CPU/GPU N=1/N=2 ordinary/profiled pairs three times in fresh processes, with
alternating pair order and a fresh directory. The experiment retains the current
cold-start procedure; warmups and prepared statements remain deferred.

The inspected newer upstream Sirius Python runner times `execute()` and fetches
afterward. Older cached source views timed execute-and-fetch. The upstream
comment assumes execute fully materializes results, whereas public DuckDB 2.0
documents a first-rows boundary. Our experiment deliberately fetches fully and
does not assume that fetch time is purely Python conversion.

## Update: 2026-11-04 timing findings and experiment preservation

The user supplied the completed 24-run cluster experiment and CPU/GPU profile
excerpts confirming DuckDB 2.0.0. Findings are documented in
[benchmark/experiments/frontend_overhead_timing/TIMING_FINDINGS.md](benchmark/experiments/frontend_overhead_timing/TIMING_FINDINGS.md). Reported CPU
frontend shares are usually below 5%, reaching 9.68% in a short-query case;
reported GPU shares stay below 5%. Parser timing is absent from the supplied
engine profiles. Complete Sirius planning coverage is unverified. The user
accepts modest comparable GPU planning overhead as a working assumption and
retains end-to-end execute-and-fetch timing. Prepared/native timers are deferred.

The user requested reproducible profiling while allowing future benchmark
refactoring, including splitting the active file. Unchanged copies of all
three current Python files now live in
[benchmark/experiments/frontend_overhead_timing](benchmark/experiments/frontend_overhead_timing/README.md).
This includes the benchmark dependency, so the historical profiling wrapper
does not depend on future active code. Existing top-level Python files remain
unchanged in their original locations. Preserve the snapshot; new methodology
belongs in a new named experiment. No active module split has been implemented.

[benchmark/experiments/frontend_overhead_timing/run_timing_experiment.sh](benchmark/experiments/frontend_overhead_timing/run_timing_experiment.sh) replaces
the long inline shell block. It runs the frozen sources with CPU/GPU, N=1/N=2,
three ordinary/profiled pairs, and the original alternating pair order. It
creates a unique results directory, records the Python/DuckDB engine version,
and supports PYTHON, DB_PATH, and SIRIUS_EXTENSION path overrides. It stops on
failure and anchors relative paths to the benchmark directory.

The supplied console output is archived with the snapshot. Full original raw
JSON/CSV remain on the cluster under `results/timing-20261103-181407/`; exact
Sirius provenance and complete hardware metadata have not been supplied. Local
sources have not been compared with the actual cluster files. No Git commands
were used. Current source fallback=false remains preserved; the main README
now reflects it instead of the outdated fallback-enabled description.

Verification: the snapshot's three Python files match the active originals
byte-for-byte, and the archived output matches the supplied attachment. The
documented ranges and means were recomputed from all 88 printed query rows.
Runner checks covered all 24 invocations, pair order, frozen import paths,
space-containing path overrides, arbitrary starting directory, and immediate
failure exit. Frozen CPU SF0.01 ordinary/profiled N=1/N=2 runs passed on local
DuckDB 1.5.6 and produced the expected 44-row summary CSV. This is not a new
GPU/cluster validation. The main benchmark and both original profiling Python
files were left unchanged.

At the user's request, the experiment folder was renamed to
`benchmark/experiments/frontend_overhead_timing/`. All three `TIMING*.md`
documents and `run_timing_experiment.sh` now live alongside its preserved
Python sources. From `benchmark/`, run
`bash experiments/frontend_overhead_timing/run_timing_experiment.sh`.
The runner still anchors relative database paths and generated results to
`benchmark/`; the experiment's Python code and measurement protocol are unchanged.

## Update: 2026-11-22 fixed-size warmup

The active benchmark is split into `benchmark.py`, `core.py`, `cli.py`,
`json_io.py`, `sweep.py`, and `plot.py`. The user authorized an opt-in
`--warmup` phase with an independent `--warmup-streams` count (default 5).
Every warmup stream executes all 22 queries once in shuffled order and fully
fetches results. Baseline and concurrent measurements use the same warmup
count rather than tying it to measurement N. Warmup shuffling uses the integer
measurement seed, or the sample number for named measurement orders, so paired
runs have identical warmup orders.

Warmup completes before the measurement barrier and contributes no query
timings or QPS/slowdown samples. The shared database instance and connection
pool stay open without cache reset between phases. The pool and Sirius's
startup admission accommodate the larger of N and the enabled warmup count.
For N above that count, additional measurement connections use the shared
engine caches without executing warmup themselves. Cold execution remains the
default. Single-run and sweep CLI/TOML settings support both options; sweeps
forward them to all children, including baselines. JSON records warmup
enablement, effective count, and actual shuffled orders. At the user's request,
the count remains an ordinary tunable, with default 5 and effective 0 meaning
no warmup. Baseline loading and plotting check warmup enablement, not counts
or orders. Plot grouping does not use warmup. Baseline loading accepts the
existing settings object rather than separate I/O and warmup arguments.
Sweep overrides forward shared run options without naming warmup separately.
The frozen frontend timing experiment and current fallback setting are preserved.

Verification passed with local DuckDB 1.5.6: real SF0.01 CPU warm baseline and
comparison processes, custom warmup counts, cold execution, CLI/TOML overrides,
sweep forwarding, saved orders, and recomputed metrics. Instrumented workers
confirmed phase separation, connection reuse, timing exclusion, and warmup
failure propagation. Sirius admission and plot metadata checks passed without
running the GPU runtime or rendering figures. GPU/cluster validation is pending.

## Update: 2026-11-22 plot hardware and cache titles

The user requested automatic CPU/GPU detection through a small additional
Python file. `benchmark/hardware.py` reads Linux CPU model names and queries
`nvidia-smi` for NVIDIA GPU models. `json_io.py` records `cpu_model` and
`gpu_model` when writing each successful result, after all measurement timers
have stopped. Missing NVIDIA tooling or an unavailable GPU produces null GPU
metadata. The helper also prints a short summary when run directly.

All four plot types now show hot/cold cache mode from the recorded `warmup`
flag, with the recorded CPU/GPU models on a subtitle line. Plotting never
detects the plotting machine's hardware. Older results without model metadata
show "not recorded". Mixed cache modes are labeled explicitly. This does not
change series grouping or require additional plotting arguments.

## Update: 2026-11-22 one GPU for all I/O backends

At the user's request, generated Sirius YAML now sets `topology.num_gpus: 1`
for both `io_uring` and `kvikio`. All query streams share that GPU. The runner
continues to preserve scheduler-provided GPU visibility. Generated YAML was
checked for both backends; GPU runtime validation remains a cluster task.

## Update: 2026-11-22 sweep cache and query-order flags

Sweeps accept `--warmup` / `--no-warmup` for hot/cold cache and now accept
`--seed` with the same `ASC`, `DESC`, `ASC-DESC`, or integer shuffle choices
as standalone runs. Explicit CLI values override `[run]` TOML settings and
are forwarded to every baseline and comparison process. The requested
`warmup_streams` default is 5; effective zero still means disabled warmup.

## Update: 2026-11-22 sweep dimensions and preserved manual defaults

The user clarified that cache modes and query orders must be sweep dimensions,
not just single-run overrides. `SweepSettings` now has `cache_modes` and
`seeds` lists. TOML uses `[sweep].cache_modes = ["cold", "hot"]` and
`[sweep].seeds = ["ASC", "DESC", "ASC-DESC", 42]`. CLI `--cache` and
`--seed` / `--seeds` accept lists and override TOML; `--warmup` and
`--no-warmup` remain shortcuts for single-mode sweeps. Missing lists fall back
to the corresponding `[run]` setting.

Every sample/backend/cache/order combination gets an independent N=1 reference
and a directory `<device>/<hot|cold>/seed-<order>/sample-XX/`. Plotting splits
mixed indexes into separate cache/order figure sets; warmup stream counts
still do not group data. Existing results stay in their original locations.

The user manually restored `RunSettings.warmup_streams = 0` and explicitly
asked that manual edits be preserved. This implementation does not edit
`core.py`. Configure `[run].warmup_streams = 5` explicitly when actual warmup
is wanted with that manual default. Do not restore earlier defaults silently.

Verification: an SF0.01 CPU sweep covered both cache modes, four query orders,
and N=1/N=2 in 16 fresh processes. Baseline associations, unique paths, counts,
and metrics passed. Plot partitioning/aggregation/dispatch checks passed without
rendering figures. A byte-level hash confirmed `core.py` was unchanged.

## Update: 2026-11-22 one internal warmup count

The user authorized replacing the internal warmup boolean with a single
`warmup_streams` count. Zero means cold cache; a positive count runs that many
shuffled 22-query warmup streams. The default is zero in active settings,
CLI help, result readers, and the SF10 example configuration.

The standalone benchmark CLI still has `--warmup` / `--no-warmup` and
`--warmup-streams`. CLI settings loading resolves them to a single count:
without `--warmup`, the effective count is zero; with it, the configured count
is used (still zero unless set explicitly). Internal run settings and new
settings/index/result JSON no longer store a warmup boolean.

Sweeps use `[sweep].warmup_streams` integer lists instead of `cache_modes`;
`[0, 5]` sweeps cold and warm cache. CLI `--warmup-streams 0 5` overrides that
list. The provided TOML defaults to `[0]`. Each count/order combination has
its own baseline and `warmup-<count>/seed-<order>/` result/plot directories,
including distinct directories for different positive counts. Cache titles
and baseline mode checks derive hot/cold from whether the count is positive.
Previous result JSON with a stream count remains readable; a missing count
defaults to zero. The frozen profiling experiment remains unchanged.

Verification passed on local DuckDB 1.5.6: benchmark CLI normalization and
actual warmup worker counts; default cold execution; and a 12-process SF0.01
CPU sweep with counts 0/2/3, two query orders, and N=1/N=2. References, metrics,
single-field JSON, plot aggregation/dispatch, and derived hot/cold titles
passed. Sirius admission was checked with a mock; GPU runtime and figure
rendering were not exercised.

## Update: 2026-11-22 sweep progress

Before each benchmark subprocess, `sweep.py` prints completed/total benchmark
invocations and the percentage completed, starting at zero and finishing with
a final 100% report. The total includes each sample/backend/warmup/order
combination's N=1 baseline and each requested N>1 run. Requested N=1 reuses
the baseline without being counted twice. Only successful children advance
the counter; warmup queries are part of a benchmark, not separate progress units.

## Update: 2026-11-22 one warmup per shared warm batch

The user approved replacing repeated warmup/process startup at every N with
a shared warm session. For each sample/backend/warmup-count/query-order
combination with a positive count, the sweep launches one child with the
whole connection list. The child opens one database/runtime, creates capacity
for the largest N or warmup count, warms once, then measures N=1 and the
requested larger Ns in ascending order. Connections, worker pool, and engine
caches remain alive between measurements. The user explicitly accepts later
Ns inheriting cache state from the baseline and previous measurements.

Cold measurements still use a separate fresh process for the baseline and
each requested N. The benchmark CLI accepts a connection list for warm
batches while retaining single-count calls and the existing warmup flags.
The execution core separates session setup, warmup, and measured workloads;
its batch generator yields results as they complete and reuses baseline
latencies in memory. The entry point saves each result before continuing.

Existing per-N JSON/settings/YAML paths, result schema, baseline links,
metrics, and overall per-measurement progress are preserved. Warm results
share a process ID; each GPU YAML reflects the shared startup capacity.
Batch failure retains earlier successful files, stops the sweep before its
index is published, and closes the shared resources. No plotting code was
edited; the user says another agent is working on plotting.

Verification passed on local DuckDB 1.5.6: instrumented exact warmup/query
counts, phase separation and timing exclusion, one eviction/engine/pool per
warm batch, and cleanup after warmup/measurement/output failure. An SF0.01
CPU sweep produced 24 measurements in 4 warm-batch processes and 12 cold
processes with correct progress, filenames, schema, references, and metrics.
Explicit N=1 reuse, standalone warm batches, and existing external-baseline
single runs passed. GPU admission, one-GPU selection, and per-result YAML
were verified with a mocked runtime; real GPU validation remains pending.

## Update: 2026-11-22 warm-batch review

Progress belongs only to `sweep.py`: a local completed/total tuple and print
statements replace the progress class. The benchmark has no progress flag,
counter, or offset. The sweep reports before each child launch and at completion;
a warm batch advances the measured-benchmark count after its child succeeds.

Warm batches now accept an external `--baseline-json`. Loading a reference
requires the exact same warmup stream count, including for single-count calls.
With an external reference, only requested Ns are measured, in ascending order;
even an explicitly requested N=1 retains that reference. Otherwise the batch
still measures its own N=1 first. An external batch's `--output` names its first
measurement; subsequent files use `nN.json`. The existing reference is read only.
The session context manager and batch iterator use `Generator` annotations so
type checkers recognize their yielded values and generator cleanup methods.

Review verification passed on DuckDB 1.5.6: the 24-measurement CPU sweep,
one warmup per shared session, failure cleanup, sweep-only progress, external
baseline reuse (including explicit N=1), exact-count rejection before setup,
and preserved references. External GPU batch YAML/capacity checks passed with
a mocked Sirius runtime. Plotting code was not edited.
