"""Supervises the engine process and preserves completed experiment results.

A separate process enforces deadlines even when native query code is stuck.
Progress messages reset per-phase deadlines and flush observations to disk.
Final summaries include only complete successful repetition groups.
"""

import dataclasses
import datetime
import multiprocessing
import multiprocessing.connection
import multiprocessing.process
import time
import uuid
from pathlib import Path
from typing import cast

from parallel_bench import artifacts, experiment, metrics, models
from parallel_bench import config as configuration

# Public compatibility alias; the implementation now lives in timing.py.
from parallel_bench.timing import run_batch as run_batch


@dataclasses.dataclass
class SupervisorState:
    """Mutable process lifecycle and completed batches owned by the parent."""

    deadline: float
    status: models.RunStatus = models.RunStatus.RUNNING
    error: str | None = None
    phase: models.JsonObject = dataclasses.field(
        default_factory=lambda: {"name": "startup"}
    )
    batches: list[models.Batch] = dataclasses.field(default_factory=list)

    def fail(self, status: models.RunStatus, error: str) -> None:
        """Records one terminal failure reason."""
        self.status = status
        self.error = error

    def snapshot(self, exitcode: int | None) -> models.JsonObject:
        """Builds the final status document without serializing batch data."""
        return {
            "status": self.status,
            "error": self.error,
            "phase": self.phase,
            "engine_exitcode": exitcode,
        }


def stop_process(process: multiprocessing.process.BaseProcess) -> None:
    """Terminates a stuck child, escalating to kill after a bounded wait."""
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
    if process.is_alive():
        process.kill()
        process.join(timeout=2)


def _handle_message(
    message: models.Message,
    state: SupervisorState,
    config: configuration.Config,
    output: Path,
    log: artifacts.ArtifactLog,
) -> None:
    """Persists a message and applies its lifecycle transition."""
    log.event({"type": message.kind, "value": message.payload})
    if message.kind == models.EventKind.PHASE:
        state.phase = message.payload
        state.deadline = time.monotonic() + config.trial_timeout_s
        artifacts.write_json(output / "status.json", state.snapshot(None))
    elif message.kind == models.EventKind.METADATA:
        artifacts.write_json(output / "metadata.json", message.payload)
        if "sirius_effective_config" in message.payload:
            print(
                f"Sirius YAML: {message.payload['sirius_effective_config']}",
                flush=True,
            )
    elif message.kind == models.EventKind.BATCH:
        batch = cast(models.Batch, message.payload)
        state.batches.append(batch)
        log.batch(batch)
        state.phase = {"name": "between_trials"}
        state.deadline = time.monotonic() + config.trial_timeout_s
        if (
            not batch["warmup"]
            and batch["condition"] == models.Condition.CONCURRENT
        ):
            print(
                f"Measured pair {batch['repetition']}/{config.repetitions}",
                flush=True,
            )
    elif message.kind == models.EventKind.DONE:
        state.status = models.RunStatus.COMPLETE
    elif message.kind == models.EventKind.ERROR:
        state.fail(models.RunStatus.ERROR, message.payload["error"])


def _monitor(
    process: multiprocessing.process.BaseProcess,
    receiver: multiprocessing.connection.Connection,
    state: SupervisorState,
    config: configuration.Config,
    output: Path,
    log: artifacts.ArtifactLog,
) -> None:
    """Polls progress until completion, an external deadline, or a crash."""
    while state.status == models.RunStatus.RUNNING:
        remaining = state.deadline - time.monotonic()
        if remaining <= 0:
            state.fail(
                models.RunStatus.TIMEOUT,
                f"Deadline exceeded during {state.phase['name']}",
            )
            return
        if not receiver.poll(min(0.2, remaining)):
            if not process.is_alive():
                state.fail(
                    models.RunStatus.ERROR,
                    f"Engine process exited unexpectedly ({process.exitcode})",
                )
            continue
        try:
            message = receiver.recv()
        except EOFError:
            state.fail(
                models.RunStatus.ERROR,
                "Engine process closed its channel unexpectedly",
            )
            return
        _handle_message(message, state, config, output, log)


def _supervise(
    config: configuration.Config,
    output: Path,
    state: SupervisorState,
    log: artifacts.ArtifactLog,
) -> int | None:
    """Starts a fresh interpreter and guarantees bounded process cleanup."""
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=experiment.child_main, args=(config, str(output), sender)
    )
    try:
        process.start()
        sender.close()
        _monitor(process, receiver, state, config, output, log)
    except KeyboardInterrupt:
        state.fail(models.RunStatus.INTERRUPTED, "Interrupted by user")
    except (OSError, RuntimeError) as exc:
        state.fail(models.RunStatus.ERROR, str(exc))
    finally:
        if state.status == models.RunStatus.COMPLETE:
            process.join(timeout=2)
            if process.is_alive() or process.exitcode != 0:
                state.fail(
                    models.RunStatus.ERROR, "Engine did not exit cleanly"
                )
        stop_process(process)
        sender.close()
        receiver.close()
    return process.exitcode


def _print_summary(summary: models.JsonObject) -> None:
    """Prints each query's isolated and concurrent medians beside slowdown."""
    for slot in summary["slots"]:
        print(
            f"Slot {slot['slot']} ({slot['label']}): "
            f"L_i={slot['isolated_completion_s']['median']:.6f}s, "
            f"C_i={slot['concurrent_completion_s']['median']:.6f}s, "
            f"median paired slowdown={slot['paired_slowdown']['median']:.3f}x"
        )


def _finish(
    output: Path,
    state: SupervisorState,
    exitcode: int | None,
    log: artifacts.ArtifactLog,
) -> bool:
    """Writes final status, summary and explicit paired per-query latencies."""
    success = state.status == models.RunStatus.COMPLETE
    if not success:
        log.failure(state.phase, state.status, state.error)
    result = state.snapshot(exitcode)
    log.event({"type": "finished", **result})
    artifacts.write_json(output / "status.json", result)
    summary = metrics.summarize(state.batches)
    summary.update(status=state.status, error=state.error)
    artifacts.write_json(output / "summary.json", summary)
    artifacts.write_paired_latencies(
        output / "paired_latencies.csv", summary["paired_latencies"]
    )
    _print_summary(summary)
    if not success:
        print(f"Run {state.status}: {state.error}. Partial artifacts: {output}")
    return success


def run(config: configuration.Config) -> tuple[Path, bool]:
    """Runs one supervised experiment and returns its directory and success."""
    config.validate_config()
    if not Path(config.db).is_file():
        raise ValueError(
            f"Database does not exist: {config.db}; run prepare first"
        )
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y%m%dT%H%M%S.%fZ"
    )
    output = (
        Path(config.output_dir)
        / f"{stamp}-{config.label}-{uuid.uuid4().hex[:8]}"
    )
    output.mkdir(parents=True, exist_ok=False)
    artifacts.write_json(output / "config.json", config.to_dict())
    print(f"Artifacts: {output}", flush=True)
    if config.backend == models.BackendKind.SIRIUS:
        print(
            f"Effective YAML: {output / 'sirius-effective.yaml'}",
            flush=True,
        )
    state = SupervisorState(time.monotonic() + config.startup_timeout_s)
    log = artifacts.ArtifactLog(output)
    try:
        exitcode = _supervise(config, output, state, log)
        success = _finish(output, state, exitcode, log)
    finally:
        log.close()
    return output, success
