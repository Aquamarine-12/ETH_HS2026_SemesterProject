# Measurement and artifacts


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
| Isolated latency `L_i` | Completion time using the one-party version of the same protocol |
| Per-query slowdown | `C_i / L_i`, using isolated controls in the same repetition |
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

Resources are not halved for concurrent execution. Effective `external_threads`
and `threads` values are recorded without imposing defaults. These settings
are not a strict process CPU thread cap because caller threads can execute work. CPU affinity is recorded;
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
- `paired_latencies.csv`: one row per query slot and complete measured repetition,
  with `isolated_latency_s` (L_i), `concurrent_completion_s` (C_i), and slowdown.
- `summary.json`, `status.json`: statistics, success/failure, and last phase.
  `summary.json` also retains every paired L_i/C_i observation and separate
  per-slot distributions. The terminal prints both latency medians.
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


## Warmups: what they buy and what they hide

Warmups execute both isolated queries and their concurrent pair before the
measured rounds. Their observations remain in `samples.csv` with `warmup=True`
and are excluded from every summary statistic.

Advantages:

- Reduce the effect of first-use initialization, allocations and lazy compilation.
- Give filesystem/database caches and CPU/GPU operating state time to settle.
- Make repeated-use latency easier to compare without charging one backend for
  initialization that the other already completed.

Disadvantages:

- Exclude startup/first-query cost and make cold-use performance invisible.
- Favor repeated fixed SQL/data access, which may differ from production workloads.
- Add runtime, can change thermal state, and may warm the two backends differently.
- A fixed number of rounds does not prove convergence or complete cache residency.

For this concurrency MVP, warmups are useful because the target is slowdown
during repeated query execution. Inspect the warmup samples and variability
instead of assuming two rounds always suffice.

`--warmups 0` only disables the explicit warmup phase. With validation enabled,
CPU reference queries still touch data before timing. Even
`--warmups 0 --no-validate --no-explain` does not flush OS/database caches or
make later repetitions cold. A controlled cold-run experiment needs a separate
cache/reset protocol; this MVP does not claim to implement one.

## Why save SHA-256 hashes?

A hash is a compact fingerprint of exact bytes, useful for comparing runs:

| Fingerprint | What a difference tells you |
| --- | --- |
| SQL | The query instance changed, including whitespace |
| Runner source files | The measurement implementation changed |
| Sirius extension | A different compiled engine binary was used |
| Effective Sirius YAML | Startup configuration changed |

Hashes do not time anything, encrypt files, validate SQL answers, or prove two
experiments have equivalent hardware/settings. They are calculated outside
the measured interval. The full SQL and effective YAML are saved as well.
The dataset ID is a UUID, not a hash of database contents; editing the dataset
without changing its manifest is still possible and must be avoided.
