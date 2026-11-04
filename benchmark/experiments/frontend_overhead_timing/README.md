# Preserved frontend timing experiment

The runner, timing documents, source snapshot, and supplied output are all
contained in `benchmark/experiments/frontend_overhead_timing/`.

This directory contains unchanged copies of `benchmark.py`, `profile_timing.py`,
and `summarize_timing.py`, saved on 2026-11-04. They preserve the local code used
for the experiment discussed in [the findings](TIMING_FINDINGS.md).
The supplied cluster output is retained in
[observed-output-2026-11-03.txt](observed-output-2026-11-03.txt).

Run the matrix with [run_timing_experiment.sh](run_timing_experiment.sh).
Read [TIMING_EXPERIMENT.md](TIMING_EXPERIMENT.md) for setup and metrics.
The runner executes these copies directly; `profile_timing.py` imports the
`benchmark.py` beside it, independently of future changes to the active code.
No packages or shared benchmark modules are imported from the parent directory.

## Preservation rule

Treat these Python files as a historical snapshot. Do not refactor them or
redirect their imports to the active benchmark. For a new experiment with
different behavior, create a new named experiment directory. The original
top-level Python files remain in their original locations.

This deliberate small duplication keeps the experiment independent without
introducing a plugin API or freezing the main benchmark's internal functions.
The active benchmark may later be split into small modules when useful.

A possible future layout is a thin `benchmark.py` for CLI/coordination,
`database.py` for generation and backend setup, and `execution.py` for streams
and metrics. This split is a suggestion, not part of this experiment change.
No compatibility interface with these frozen scripts needs to be maintained.

## Environment and evidence

The reported cluster run used DuckDB 2.0.0, the existing SF10 database, and a
Sirius extension at `$HOME/.local/lib/sirius/sirius.duckdb_extension`. Preserve
that compatible extension binary and its provenance alongside the cluster's raw
results. The exact Sirius revision and complete hardware/environment metadata
were not supplied, so these sources alone do not guarantee identical timings.
The newly saved source copies have not been compared with the cluster files.

The frozen GPU setup explicitly disables CPU fallback, matching the current
local source. Other Sirius settings, including telemetry, retain engine
defaults except for admission concurrency set to N in generated startup YAML.

The original raw JSON profiles and CSV remain on the cluster under
`results/timing-20261103-181407/`. Only the supplied console output is archived
here; it contains rounded report values, not the complete underlying samples.
Retain the raw result directory for reanalysis. Future runner invocations create
new result directories and record the Python and DuckDB engine versions.
