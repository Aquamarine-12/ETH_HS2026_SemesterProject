# Configuration reference

Commands below are run from `benchmark/`.


TOML accepts `[benchmark]` and `[sirius]`; unknown keys are errors. Paths in
TOML are relative to the TOML directory. CLI paths are relative to the current
working directory. Paths inside the Sirius YAML follow Sirius's own resolution
rules (use absolute paths for storage/spill locations).

```bash
python -m parallel_bench run --config configs/cpu-smoke.toml --queries 6 6 --repetitions 10
python -m parallel_bench run --config configs/duckdb.toml --sql-files queries/a.sql queries/b.sql
python -m parallel_bench run --help
```

| `[benchmark]` option | Default | Meaning |
| --- | --- | --- |
| `backend` | `"duckdb"` | `duckdb` or `sirius` |
| `db`, `scale_factor` | `"data/tpch.duckdb"`, `1.0` | Preparation destination and size; run reads the actual manifest SF |
| `queries` | `[3, 6]` | Two IDs, 1–22; duplicates retain distinct slot identities |
| `sql_files` | `[]` | Two concrete SQL files; takes precedence over configured IDs |
| `warmups`, `repetitions` | `2`, `10` | Rounds, each containing both isolated controls and the concurrent pair |
| `seed` | `0` | Randomizes condition order within each measured round |
| `threads`, `external_threads`, `memory_limit` | omitted | Leave DuckDB defaults untouched; explicit values apply to isolated and concurrent runs |
| `temp_directory` | omitted | Optional DuckDB spill location; Sirius storage tiers are configured in YAML |
| `output_dir`, `label` | `"results"`, `"run"` | New timestamped artifact directory for every invocation |
| `barrier_timeout_s` | `30` | Maximum wait at the Python barrier |
| `trial_timeout_s` | `300` | External deadline for each reference, diagnostic, or trial, including setup/validation |
| `startup_timeout_s` | `120` | External deadline for imports, extension initialization and metadata collection |
| `validate` | `true` | Compare full answers with untimed CPU reference answers |
| `relative_tolerance`, `absolute_tolerance` | `1e-6`, `1e-8` | Numeric comparison tolerances; integers remain exact |
| `explain` | `false` | Save untimed EXPLAIN plans before warmups; not an execution trace |
| `expected_source_id` | `""` | Expected DuckDB commit prefix, 10–40 hex characters; required for Sirius |
| `tpch_extension` | `""` | Optional local TPC-H extension for preparation |

`[sirius]` accepts `extension_path`, `config_path`, `gpu` (optional single device index or UUID),
`enable_duckdb_fallback` (default false), and `allow_unsigned_extensions`
(default true for local builds).

`prepare` saves all 22 concrete SQL instances inside
`<database>.benchmark.json`, along with dataset ID, SF, generator revision,
and table row counts. Runs reuse these exact strings, not regenerated query
parameters. The actual SQL and hashes are also saved in every run directory.
Pass the same files to both backends when using externally generated SQL.
Existing databases without a manifest can be used with `--sql-files`.

Only a single SELECT/WITH statement per slot is accepted. DuckDB's built-in
Q15 uses a CTE and works even as Q15+Q15. External CREATE VIEW / SELECT /
DROP VIEW scripts must first be expressed as a CTE; rejecting such scripts
prevents shared view-name collisions and accidentally timing the DROP result.
All queries are exploratory TPC-H workloads, not certified TPC-H results.


## Engine defaults and explicit overrides

Omitting `threads`, `external_threads`, `memory_limit`, or `temp_directory`
means the runner sends no SET for that option. `config.json` stores `null`;
`metadata.json` stores the actual value returned by `duckdb_settings()`.
The main CPU/GPU profiles omit these values. The small smoke profile explicitly
uses two threads and 1 GB for a lightweight development run.

For the earlier requested 32 GB experiment, add this to both comparison configs:

```toml
[benchmark]
memory_limit = "32GB"
```

The two backend invocations must have matching *effective* resources. Engine
defaults can differ between cluster allocations; never infer equality merely
because both TOMLs omit an option. A DuckDB memory limit is not a limit on GPU
VRAM or every Python/native allocation. Benchmark repetitions, timeouts and
query choices are runner settings and retain explicit runner defaults. Progress
output is disabled to avoid contaminating measurements.

## Finding and previewing the Sirius YAML

`configs/sirius-runtime.yaml` is the input template. A real Sirius run writes
`results/<run-id>/sirius-effective.yaml` before loading the extension and prints
that path. CPU runs do not generate it. The effective copy explicitly admits
two queries and preserves other YAML settings.

To inspect the exact startup configuration without a GPU or Sirius binary:

```bash
python -m parallel_bench sirius-config --config configs/sirius.toml
python -m parallel_bench sirius-config --config configs/sirius.toml \
  --output /tmp/sirius-preview.yaml
```

The output path must be new; preview never overwrites an existing file.
PyYAML is required (`python -m pip install -e '.[sirius]'`).

Omitting `sirius.gpu` preserves a single device assigned through
`CUDA_VISIBLE_DEVICES`. If that variable is absent, the single-GPU protocol
selects device 0. Multiple inherited devices require an explicit selection;
the runner does not silently expand a scheduler allocation. If you set `gpu`,
use the allocated physical device index or GPU UUID expected by CUDA.

Custom paths *inside* the YAML are passed to Sirius unchanged; use absolute
paths for cluster spill/storage directories.
