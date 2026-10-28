# Two-query CPU/GPU benchmark

## TL;DR

- Compare two fixed TPC-H queries alone and concurrently, on DuckDB or Sirius.
- Two caller threads share one database instance and a Python start barrier.
- Report each query's **isolated latency L_i**, **concurrent completion C_i**, and
  **slowdown C_i/L_i**, plus pair makespan and submission skew.
- Omitted engine resource settings use DuckDB's defaults. Actual values are saved.
- Results live in a fresh `results/<run-id>/` directory. Start with
  `paired_latencies.csv` for individual L_i/C_i values and `summary.json` for stats.
- CPU integration is tested locally; Sirius execution still needs GPU validation.

## Quick CPU check

From `benchmark/`, with Python 3.11+:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-cpu-smoke.txt
python -m parallel_bench prepare --config configs/cpu-smoke.toml
python -m parallel_bench run --config configs/cpu-smoke.toml
```

Preparation runs once and refuses to overwrite an existing database. Skip it
if that dataset already exists. SF0.01 is a functionality check, not a useful
basis for performance conclusions. This CPU wheel is not automatically
compatible with a different Sirius build.

## More detail when needed

- [Cluster installation](docs/cluster-setup.md): Ubuntu 24.04, user-local setup,
  DuckDB and Sirius versions, build commands, and GPU prerequisites.
- [Configuration](docs/configuration.md): all options, overrides, SQL sources,
  engine defaults, and where the effective Sirius YAML is written.
- [Methodology](docs/methodology.md): timing definitions, warmup tradeoffs,
  validation, artifacts, SHA hashes, and limitations.
- [Code map and development](docs/development.md): module responsibilities,
  tests, formatting and type checks.

Preview Sirius YAML without running queries or requiring a GPU:

```bash
python -m pip install -e '.[sirius]'
python -m parallel_bench sirius-config --config configs/sirius.toml
```

All commands here only affect local files/environments. The runner performs no
Git operations. Git ignores generated databases, environments, and run artifacts.
