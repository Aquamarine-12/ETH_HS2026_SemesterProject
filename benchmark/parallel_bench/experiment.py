"""Controls query order and validation inside the engine child process.

Reference answers and optional plans are collected before warmup rounds.
Each measured round shuffles two isolated controls and one concurrent pair.
Only small metadata/timing messages cross the process boundary, after timing.
"""

import concurrent.futures
import dataclasses
import multiprocessing.connection
import os
import random
import time
import traceback
from pathlib import Path

from parallel_bench import (
    artifacts,
    dataset,
    models,
    sirius,
    timing,
    validation,
)
from parallel_bench import backend as engines
from parallel_bench import config as configuration


@dataclasses.dataclass
class Experiment:
    """State owned by the experiment controller, never by timed workers."""

    config: configuration.Config
    output: Path
    engine: engines.Backend
    channel: multiprocessing.connection.Connection
    queries: list[models.Query]
    references: list[models.ResultRows] = dataclasses.field(
        default_factory=list
    )

    def send(self, kind: models.EventKind, payload: models.JsonObject) -> None:
        """Sends a typed message after any measured query work has finished."""
        self.channel.send(models.Message(kind, payload))


def _metadata(
    experiment: Experiment, data: models.JsonObject
) -> models.JsonObject:
    """Collects run identity without claiming that GPU overlap was observed."""
    config = experiment.config
    result: models.JsonObject = {
        "schema_version": 2,
        "engine": experiment.engine.version,
        "effective_settings": experiment.engine.settings,
        "dataset": data,
        "queries": experiment.queries,
        "hardware": artifacts.hardware_info(),
        "pid": os.getpid(),
        "gpu_overlap_verified": False,
        "cache_policy": {
            "eviction": "none",
            "warmup_rounds": config.warmups,
            "cpu_references_before_warmups": config.validate,
        },
        "clock": "perf_counter_ns; timestamps relative to engine process epoch",
        "validation": "CPU reference answers"
        if config.validate
        else "disabled",
        "execution_path": "cpu",
        "environment": {
            key: value
            for key, value in os.environ.items()
            if key.startswith("SIRIUS_")
            or key in {"CUDA_VISIBLE_DEVICES", "OMP_NUM_THREADS"}
        },
        "runner_sources": {
            path.name: artifacts.sha256_file(path)
            for path in Path(__file__).parent.glob("*.py")
        },
    }
    if config.backend == models.BackendKind.SIRIUS:
        result.update(_sirius_metadata(experiment))
    return result


def _sirius_metadata(experiment: Experiment) -> models.JsonObject:
    """Records the loaded binary/config identity and applied worker options."""
    config = experiment.config.sirius
    connection = experiment.engine.connection()
    # Fail before timing if required session settings are absent.
    connection.close()
    effective = experiment.output / "sirius-effective.yaml"
    return {
        "execution_path": (
            "fallback_permitted"
            if config.enable_duckdb_fallback
            else "gpu_requested_fallback_disabled"
        ),
        "sirius_extension_sha256": artifacts.sha256_file(
            Path(config.extension_path)
        ),
        "sirius_config_sha256": artifacts.sha256_file(effective),
        "sirius_effective_config": str(effective),
        "sirius_applied_session_settings": {
            "gpu_execution": True,
            "enable_duckdb_fallback": config.enable_duckdb_fallback,
        },
    }


def _collect_references(experiment: Experiment) -> None:
    """Computes untimed CPU answers and optional worker execution plans."""
    for slot, query in enumerate(experiment.queries):
        if experiment.config.validate:
            experiment.send(
                models.EventKind.PHASE, {"name": f"reference_{slot}"}
            )
            rows = experiment.engine.root.execute(query["sql"]).fetchall()
            experiment.references.append(rows)
        if experiment.config.explain:
            experiment.send(models.EventKind.PHASE, {"name": f"explain_{slot}"})
            connection = experiment.engine.connection()
            try:
                plan = connection.execute("EXPLAIN " + query["sql"]).fetchall()
                (experiment.output / f"plan-slot-{slot}.txt").write_text(
                    "\n".join(str(row) for row in plan), encoding="utf-8"
                )
            finally:
                connection.close()


def _validate_samples(
    experiment: Experiment,
    results: list[tuple[models.Sample, models.ResultRows | None]],
) -> list[models.Sample]:
    """Validates complete results only after the whole timed batch finishes."""
    records = []
    for sample, rows in results:
        if (
            experiment.config.validate
            and sample["status"] == models.SampleStatus.OK
        ):
            assert rows is not None
            try:
                validation.compare_results(
                    experiment.references[sample["slot"]],
                    rows,
                    experiment.config.relative_tolerance,
                    experiment.config.absolute_tolerance,
                )
            except ValueError as exc:
                sample["status"] = models.SampleStatus.VALIDATION_ERROR
                sample["error"] = str(exc)
        records.append(sample)
    return records


def _run_condition(
    experiment: Experiment,
    pool: concurrent.futures.ThreadPoolExecutor,
    condition: models.Condition,
    repetition: int,
    warmup: bool,
    epoch: int,
) -> None:
    """Reports a phase, runs its queries and publishes one completed batch."""
    phase = {
        "name": condition,
        "repetition": repetition,
        "warmup": warmup,
        "slots": condition.slots(),
    }
    experiment.send(models.EventKind.PHASE, phase)
    results = timing.run_batch(
        experiment.engine,
        pool,
        experiment.queries,
        condition.slots(),
        experiment.config,
        epoch,
    )
    records = _validate_samples(experiment, results)
    batch: models.Batch = {
        "condition": condition,
        "repetition": repetition,
        "warmup": warmup,
        "records": records,
    }
    experiment.send(models.EventKind.BATCH, dict(batch))
    if any(record["status"] != models.SampleStatus.OK for record in records):
        raise RuntimeError("Trial failed; see samples.csv and engine.log")


def _run_rounds(experiment: Experiment) -> None:
    """Runs fixed-order warmups, then randomly ordered measured repetitions."""
    config = experiment.config
    rng = random.Random(config.seed)
    epoch = time.perf_counter_ns()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=2, thread_name_prefix="query"
    ) as pool:
        for warmup, count in (
            (True, config.warmups),
            (False, config.repetitions),
        ):
            for repetition in range(1, count + 1):
                conditions = list(models.Condition)
                if not warmup:
                    rng.shuffle(conditions)
                for condition in conditions:
                    _run_condition(
                        experiment, pool, condition, repetition, warmup, epoch
                    )


def _execute(
    config: configuration.Config,
    output: Path,
    channel: multiprocessing.connection.Connection,
) -> None:
    """Owns the backend lifetime across setup, references and all rounds."""
    sirius.configure_environment(config, output)
    engine = engines.Backend(config)
    try:
        queries, data = dataset.queries_for_run(config, engine, output)
        experiment = Experiment(config, output, engine, channel, queries)
        experiment.send(models.EventKind.METADATA, _metadata(experiment, data))
        _collect_references(experiment)
        _run_rounds(experiment)
    finally:
        engine.close()


def child_main(
    config: configuration.Config,
    output_string: str,
    channel: multiprocessing.connection.Connection,
) -> None:
    """Runs the engine with native output redirected and reports its outcome."""
    output = Path(output_string)
    with (output / "engine.log").open(
        "a", buffering=1, encoding="utf-8"
    ) as log:
        # The dedicated child owns these descriptors until it exits.
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
        try:
            _execute(config, output, channel)
            channel.send(models.Message(models.EventKind.DONE))
        except BaseException as exc:
            traceback.print_exc()
            channel.send(
                models.Message(
                    models.EventKind.ERROR,
                    {
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                )
            )
        finally:
            channel.close()
