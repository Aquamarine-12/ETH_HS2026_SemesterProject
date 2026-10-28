# Code map and development

## Execution flow

```text
cli → config → prepare (dataset)
             → run (supervisor) → experiment (child) → timing → backend
                               ← completed samples ← validation
             → metrics → artifacts
```

| Module | Responsibility |
| --- | --- |
| `cli.py` | Parse prepare/run/YAML-preview commands and CLI overrides |
| `config.py` | Typed config objects, validation and path resolution |
| `models.py` | Shared observation schemas, enums and connection protocols |
| `backend.py` | One native database instance, worker connections and explicit SETs |
| `sirius.py` | Render startup YAML and configure a single allocated GPU |
| `dataset.py` | Generate TPC-H once, save its manifest and freeze exact SQL |
| `timing.py` | Barrier release, caller timestamps and complete execute/fetch calls |
| `experiment.py` | References, warmups, repetition order and result validation |
| `runner.py` | Separate supervisor process, deadlines and failure handling |
| `validation.py` | Full-result comparison outside timed query work |
| `metrics.py` | Pure calculations from successful measured observations |
| `artifacts.py` | JSON/CSV/event files, hardware information and content fingerprints |

`metrics.py` groups observations by repetition, excludes warmups and incomplete
or failed groups, pairs each query's L_i and C_i, and computes slowdown,
makespan and distributions. It never runs SQL or writes files. Raw latencies
remain available independently of the slowdown ratio.

`artifacts.py` writes the experiment's evidence to disk. JSON snapshots use a
temporary file followed by replacement, while events and completed CSV batches
are flushed incrementally. It also gathers CPU/GPU identity and computes hashes.
Those operations are outside query timing; it never decides which measurements
are statistically valid.

## Style

Follow the [Google Python guide](https://google.github.io/styleguide/pyguide.html),
using the [C++ guide's emphasis on readable, small components](https://google.github.io/styleguide/cppguide.html)
where it applies. Functions have short docstrings and annotated signatures;
longer modules start with a short overview. Comments explain synchronization,
lifetime or measurement choices rather than narrating every statement.
Prefer functions shorter than 40 lines. Shared measurement structures use
TypedDict, lifecycle states use enums, and stateful components use small classes.
`JsonObject` is reserved for flexible metadata/serialization boundaries;
database result cells necessarily retain their native heterogeneous types.

## Checks

From `benchmark/`, with the appropriate DuckDB installed in the active environment:

```bash
python -m pip install -e '.[dev,sirius]'
ruff check .
ruff format --check .
mypy parallel_bench
python -m unittest discover -s tests -v
```

Ruff checks 80-column formatting, imports, annotations and Google-convention
docstrings. Mypy checks the application package. These checks cover selected
mechanical style rules, not every judgment in the Google guide.

Tests cover real CPU execution, all 22 TPC-H queries, duplicate Q15, connection
isolation, synchronization, full answers, metrics, explicit latency exports,
engine defaults/overrides, revision errors, timeouts and CPU-only YAML rendering.
TPC-H tests skip explicitly if its extension is unavailable. YAML tests skip
explicitly if PyYAML is absent; install the Sirius extra to exercise them.
No mock configuration test qualifies actual GPU query execution.

## References for engine behavior

- [DuckDB threaded connections](https://duckdb.org/docs/2.0/guides/python/multiple_threads)
- [DuckDB TPC-H extension](https://duckdb.org/docs/2.0/core_extensions/tpch)
- [Sirius concurrency](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/concurrent-queries.md)
- [Sirius configuration](https://github.com/sirius-db/sirius/blob/main/docs/super-sirius/configuration.md)
