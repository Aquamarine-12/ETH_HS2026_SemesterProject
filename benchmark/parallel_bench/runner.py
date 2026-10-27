"""Two caller threads in one engine process, supervised by a separate process."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import csv
import json
import multiprocessing
import os
from pathlib import Path
import random
import threading
from time import monotonic, perf_counter_ns
import traceback
import uuid

from .artifacts import hardware_info, write_json
from .backend import Backend, configure_environment, sha256_file
from .dataset import queries_for_run
from .metrics import summarize
from .validation import compare_results


CSV_FIELDS = ["repetition", "condition", "warmup", "slot", "label", "status", "error",
              "arrival_ns", "release_ns", "start_ns", "finish_ns", "completion_s",
              "execute_fetch_s", "rows", "thread_id"]


def run_batch(backend, pool, queries, slots, config, epoch):
    release = {}

    def stamp_release():
        release["ns"] = perf_counter_ns() - epoch

    barrier = threading.Barrier(len(slots), action=stamp_release)
    connection_lock = threading.Lock()

    def run_one(slot):
        record = {"slot": slot, "label": queries[slot]["label"], "status": "error",
                  "thread_id": threading.get_native_id()}
        con = None
        rows = None
        try:
            # Protect root.cursor() only; executions use independent connections.
            with connection_lock:
                con = backend.connection()
            record["arrival_ns"] = perf_counter_ns() - epoch
            barrier.wait(timeout=config.barrier_timeout_s)
            record["start_ns"] = perf_counter_ns() - epoch
            rows = con.execute(queries[slot]["sql"]).fetchall()
            record["finish_ns"] = perf_counter_ns() - epoch
            record["rows"] = len(rows)
            record["status"] = "ok"
        except Exception as exc:
            barrier.abort()
            record["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            if "ns" in release:
                record["release_ns"] = release["ns"]
            if "finish_ns" in record:
                record["completion_s"] = (record["finish_ns"] - release["ns"]) / 1e9
                record["execute_fetch_s"] = (record["finish_ns"] - record["start_ns"]) / 1e9
            if con is not None:
                try:
                    con.close()
                except Exception as exc:
                    record["status"] = "error"
                    record["error"] = f"Connection cleanup failed: {exc}"
        return record, rows

    futures = [pool.submit(run_one, slot) for slot in slots]
    # No validation/serialization while the other query is still being timed.
    return [future.result() for future in futures]


def child_main(config, output_string, channel):
    output = Path(output_string)
    # Capture Python AND native engine output without mixing it with result data.
    with (output / "engine.log").open("a", buffering=1) as log:
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
        backend = None
        try:
            configure_environment(config, output)
            backend = Backend(config)
            queries, dataset = queries_for_run(config, backend, output)
            metadata = {"schema_version": 1, "engine": backend.version,
                        "effective_settings": backend.settings, "dataset": dataset,
                        "queries": queries, "hardware": hardware_info(), "pid": os.getpid(),
                        "cache_policy": "warm runs; no cache eviction; references precede warmups",
                        "clock": "perf_counter_ns; timestamps relative to this engine process epoch",
                        "validation": "complete answers against untimed CPU references" if config.validate else "disabled",
                        "execution_path": "cpu" if config.backend == "duckdb" else (
                            "fallback_permitted" if config.sirius.enable_duckdb_fallback else
                            "gpu_requested_fallback_disabled"),
                        "gpu_overlap_verified": False,
                        "environment": {k: v for k, v in os.environ.items()
                                        if k.startswith("SIRIUS_") or k in {"CUDA_VISIBLE_DEVICES", "OMP_NUM_THREADS"}},
                        "runner_sources": {p.name: sha256_file(p) for p in Path(__file__).parent.glob("*.py")}}
            if config.backend == "sirius":
                metadata["sirius_extension_sha256"] = sha256_file(config.sirius.extension_path)
                metadata["sirius_config_sha256"] = sha256_file(output / "sirius-effective.yaml")
                # Check both options on an actual worker connection before measurements.
                con = backend.connection()
                try:
                    # Do not send a catalog table function through the GPU optimizer
                    # just to inspect settings with fallback disabled.
                    metadata["sirius_applied_session_settings"] = {
                        "gpu_execution": True,
                        "enable_duckdb_fallback": config.sirius.enable_duckdb_fallback,
                    }
                finally:
                    con.close()
            channel.send({"type": "metadata", "value": metadata})
            references = []
            for slot, query in enumerate(queries):
                if config.validate:
                    channel.send({"type": "phase", "name": f"reference_{slot}"})
                    references.append(backend.root.execute(query["sql"]).fetchall())
                if config.explain:
                    channel.send({"type": "phase", "name": f"explain_{slot}"})
                    con = backend.connection()
                    try:
                        plan = con.execute("EXPLAIN " + query["sql"]).fetchall()
                        (output / f"plan-slot-{slot}.txt").write_text("\n".join(str(row) for row in plan))
                    finally:
                        con.close()
            rng = random.Random(config.seed)
            epoch = perf_counter_ns()
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="query") as pool:
                for warmup, count in ((True, config.warmups), (False, config.repetitions)):
                    for repetition in range(1, count + 1):
                        conditions = ["isolated_0", "isolated_1", "concurrent"]
                        if not warmup:
                            rng.shuffle(conditions)
                        for condition in conditions:
                            slots = [0, 1] if condition == "concurrent" else [int(condition[-1])]
                            phase = {"type": "phase", "name": condition, "repetition": repetition,
                                     "warmup": warmup, "slots": slots}
                            channel.send(phase)
                            results = run_batch(backend, pool, queries, slots, config, epoch)
                            records = []
                            for record, rows in results:
                                if config.validate and record["status"] == "ok":
                                    try:
                                        compare_results(references[record["slot"]], rows,
                                                        config.relative_tolerance, config.absolute_tolerance)
                                    except ValueError as exc:
                                        record.update(status="validation_error", error=str(exc))
                                records.append(record)
                            batch = {"condition": condition, "repetition": repetition,
                                     "warmup": warmup, "records": records}
                            channel.send({"type": "batch", "value": batch})
                            if any(r["status"] != "ok" for r in records):
                                raise RuntimeError("Trial failed; see samples.csv and engine.log")
            backend.close()
            backend = None
            channel.send({"type": "done"})
        except BaseException as exc:
            traceback.print_exc()
            channel.send({"type": "error", "error": f"{type(exc).__name__}: {exc}"})
        finally:
            if backend is not None:
                backend.close()
            channel.close()


def stop_process(process):
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
    if process.is_alive():
        process.kill()
        process.join(timeout=2)


def run(config):
    if not Path(config.db).is_file():
        raise ValueError(f"Database does not exist: {config.db}; run prepare first")
    config.validate_config()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output = Path(config.output_dir) / f"{stamp}-{config.label}-{uuid.uuid4().hex[:8]}"
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "config.json", config.to_dict())
    print(f"Artifacts: {output}", flush=True)
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=child_main, args=(config, str(output), sender))
    batches = []
    status, error = "running", None
    phase = {"name": "startup"}
    deadline = monotonic() + config.startup_timeout_s
    with (output / "events.jsonl").open("w", buffering=1) as events, (output / "samples.csv").open("w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        csvfile.flush()
        process.start()
        sender.close()
        try:
            while status == "running":
                remaining = deadline - monotonic()
                if remaining <= 0:
                    status, error = "timeout", f"Deadline exceeded during {phase['name']}"
                    break
                if not receiver.poll(min(0.2, remaining)):
                    if not process.is_alive():
                        status, error = "error", f"Engine process exited unexpectedly ({process.exitcode})"
                    continue
                try:
                    event = receiver.recv()
                except EOFError:
                    status, error = "error", "Engine process closed its channel unexpectedly"
                    break
                events.write(json.dumps(event, allow_nan=False) + "\n")
                kind = event["type"]
                if kind == "phase":
                    phase = event
                    deadline = monotonic() + config.trial_timeout_s
                    write_json(output / "status.json", {"status": status, "phase": phase})
                elif kind == "metadata":
                    write_json(output / "metadata.json", event["value"])
                elif kind == "batch":
                    batch = event["value"]
                    batches.append(batch)
                    for record in batch["records"]:
                        writer.writerow({**batch, **record})
                    csvfile.flush()
                    if not batch["warmup"] and batch["condition"] == "concurrent":
                        print(f"Measured pair {batch['repetition']}/{config.repetitions}", flush=True)
                    # Bound inter-trial work and final connection teardown too.
                    phase = {"name": "between_trials"}
                    deadline = monotonic() + config.trial_timeout_s
                elif kind == "done":
                    status = "complete"
                elif kind == "error":
                    status, error = "error", event["error"]
        except KeyboardInterrupt:
            status, error = "interrupted", "Interrupted by user"
        finally:
            if status == "complete":
                process.join(timeout=2)
                if process.is_alive() or process.exitcode != 0:
                    status, error = "error", "Engine did not exit cleanly after completion"
            stop_process(process)
            receiver.close()
        if status != "complete" and "slots" in phase:
            # These have NO timing values: a killed process cannot supply reliable ones.
            for slot in phase["slots"]:
                writer.writerow({"repetition": phase["repetition"], "condition": phase["name"],
                                 "warmup": phase["warmup"], "slot": slot, "status": status, "error": error})
        result = {"status": status, "error": error, "phase": phase, "engine_exitcode": process.exitcode}
        events.write(json.dumps({"type": "finished", **result}) + "\n")
    write_json(output / "status.json", result)
    summary = {**summarize(batches), "status": status, "error": error}
    write_json(output / "summary.json", summary)
    for slot in summary["slots"]:
        print(f"Slot {slot['slot']}: isolated {slot['isolated_completion_s']['median']:.6f}s, "
              f"concurrent {slot['concurrent_completion_s']['median']:.6f}s, "
              f"median paired slowdown {slot['paired_slowdown']['median']:.3f}x")
    if status != "complete":
        print(f"Run {status}: {error}. Partial artifacts retained in {output}")
    return output, status == "complete"
