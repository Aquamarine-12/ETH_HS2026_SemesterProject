"""Runs isolated or concurrent query submissions behind a Python barrier.

Every worker creates its own connection to the same native database instance.
The barrier action captures one release timestamp before any execute call.
Result validation and serialization are deferred until all workers finish.
"""

import concurrent.futures
import dataclasses
import threading
import time

from parallel_bench import config as configuration
from parallel_bench import models


@dataclasses.dataclass
class ReleaseClock:
    """Holds the common barrier-release timestamp relative to a run epoch."""

    epoch_ns: int
    release_ns: int | None = None

    def stamp(self) -> None:
        """Records release while the barrier still holds all participants."""
        self.release_ns = time.perf_counter_ns() - self.epoch_ns


@dataclasses.dataclass
class TrialContext:
    """Shares synchronization primitives within one isolated or paired trial."""

    factory: models.ConnectionFactory
    queries: list[models.Query]
    clock: ReleaseClock
    barrier: threading.Barrier
    connection_lock: threading.Lock
    timeout_s: float


def _record_durations(sample: models.Sample, clock: ReleaseClock) -> None:
    """Adds durations only when their corresponding timestamps exist."""
    if clock.release_ns is None:
        return
    sample["release_ns"] = clock.release_ns
    if "finish_ns" in sample:
        sample["completion_s"] = (sample["finish_ns"] - clock.release_ns) / 1e9
        sample["execute_fetch_s"] = (
            sample["finish_ns"] - sample["start_ns"]
        ) / 1e9


def _close_connection(
    connection: models.QueryConnection | None, sample: models.Sample
) -> None:
    """Closes a worker connection and retains cleanup failures in its record."""
    if connection is None:
        return
    try:
        connection.close()
    except Exception as exc:
        sample["status"] = models.SampleStatus.ERROR
        sample["error"] = f"Connection cleanup failed: {exc}"


def _run_one(
    context: TrialContext, slot: int
) -> tuple[models.Sample, models.ResultRows | None]:
    """Times one complete execute/fetch call after the shared release."""
    sample: models.Sample = {
        "slot": slot,
        "label": context.queries[slot]["label"],
        "status": models.SampleStatus.ERROR,
        "thread_id": threading.get_native_id(),
    }
    connection = None
    rows = None
    try:
        # Only root.cursor() and connection setup are serialized, never queries.
        with context.connection_lock:
            connection = context.factory.connection()
        sample["arrival_ns"] = time.perf_counter_ns() - context.clock.epoch_ns
        context.barrier.wait(timeout=context.timeout_s)
        sample["start_ns"] = time.perf_counter_ns() - context.clock.epoch_ns
        rows = connection.execute(context.queries[slot]["sql"]).fetchall()
        sample["finish_ns"] = time.perf_counter_ns() - context.clock.epoch_ns
        sample["rows"] = len(rows)
        sample["status"] = models.SampleStatus.OK
    except Exception as exc:
        # If setup failed, release a peer that might still be waiting.
        context.barrier.abort()
        sample["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        _record_durations(sample, context.clock)
        _close_connection(connection, sample)
    return sample, rows


def run_batch(
    factory: models.ConnectionFactory,
    pool: concurrent.futures.ThreadPoolExecutor,
    queries: list[models.Query],
    slots: list[int],
    config: configuration.Config,
    epoch: int,
) -> list[tuple[models.Sample, models.ResultRows | None]]:
    """Executes one condition, using a one-party barrier for isolation."""
    clock = ReleaseClock(epoch)
    context = TrialContext(
        factory,
        queries,
        clock,
        threading.Barrier(len(slots), action=clock.stamp),
        threading.Lock(),
        config.barrier_timeout_s,
    )
    futures = [pool.submit(_run_one, context, slot) for slot in slots]
    return [future.result() for future in futures]
