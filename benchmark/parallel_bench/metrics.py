"""Converts successful measured trials into latency and slowdown statistics.

Each query's concurrent completion C_i is paired with its isolated latency L_i
from the same repetition. Warmups and incomplete/failed groups are excluded.
This module does arithmetic only: it neither executes queries nor writes files.
"""

import statistics
from typing import TypeAlias

from parallel_bench import models

TrialGroup: TypeAlias = dict[models.Condition, models.Batch]


def distribution(values: list[float]) -> models.Distribution:
    """Describes a nonempty series without distribution assumptions."""
    return {
        "count": len(values),
        "median": statistics.median(values),
        "mean": statistics.mean(values),
        "min": min(values),
        "max": max(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
    }


def _complete_groups(batches: list[models.Batch]) -> dict[int, TrialGroup]:
    """Keeps only repetition groups with every required successful condition."""
    groups: dict[int, TrialGroup] = {}
    for batch in batches:
        if not batch["warmup"]:
            groups.setdefault(batch["repetition"], {})[batch["condition"]] = (
                batch
            )
    complete = {}
    for repetition, group in sorted(groups.items()):
        if set(group) != set(models.Condition):
            continue
        records = [
            record for batch in group.values() for record in batch["records"]
        ]
        if all(
            record["status"] == models.SampleStatus.OK for record in records
        ):
            complete[repetition] = group
    return complete


def _latency_pair(
    repetition: int, slot: int, group: TrialGroup
) -> models.PairedLatency:
    """Builds one query's explicit L_i, C_i and C_i/L_i record."""
    isolated_condition = (
        models.Condition.ISOLATED_0
        if slot == 0
        else models.Condition.ISOLATED_1
    )
    isolated = group[isolated_condition]["records"][0]
    concurrent = next(
        record
        for record in group[models.Condition.CONCURRENT]["records"]
        if record["slot"] == slot
    )
    latency = isolated["completion_s"]
    completion = concurrent["completion_s"]
    return {
        "repetition": repetition,
        "slot": slot,
        "label": concurrent.get("label", f"slot-{slot}"),
        "isolated_latency_s": latency,
        "concurrent_completion_s": completion,
        "slowdown": completion / latency,
    }


def _paired_trial(repetition: int, group: TrialGroup) -> models.JsonObject:
    """Adds pair-level metrics while retaining both queries' raw latencies."""
    queries = [_latency_pair(repetition, slot, group) for slot in range(2)]
    slowdowns = [query["slowdown"] for query in queries]
    records = group[models.Condition.CONCURRENT]["records"]
    return {
        "repetition": repetition,
        "queries": queries,
        "slowdowns": slowdowns,
        "aggregate_slowdown": sum(slowdowns),
        "mean_slowdown": statistics.mean(slowdowns),
        "pair_makespan_s": max(
            query["concurrent_completion_s"] for query in queries
        ),
        "submission_skew_s": abs(
            records[0]["start_ns"] - records[1]["start_ns"]
        )
        / 1e9,
    }


def _slot_summary(
    slot: int, rows: list[models.PairedLatency]
) -> models.JsonObject:
    """Summarizes both latencies and the paired slowdown."""
    selected = [row for row in rows if row["slot"] == slot]
    isolated = [row["isolated_latency_s"] for row in selected]
    concurrent = [row["concurrent_completion_s"] for row in selected]
    return {
        "slot": slot,
        "label": selected[0]["label"],
        # Preserve the original summary names for existing result consumers.
        "isolated_completion_s": distribution(isolated),
        "concurrent_completion_s": distribution(concurrent),
        "paired_slowdown": distribution([row["slowdown"] for row in selected]),
        "ratio_of_medians": statistics.median(concurrent)
        / statistics.median(isolated),
    }


def summarize(batches: list[models.Batch]) -> models.JsonObject:
    """Calculates per-query and aggregate metrics for complete repetitions."""
    groups = _complete_groups(batches)
    paired = [
        _paired_trial(repetition, group) for repetition, group in groups.items()
    ]
    rows = [query for trial in paired for query in trial["queries"]]
    summary: models.JsonObject = {
        "complete_repetitions": len(paired),
        "paired_trials": paired,
        "paired_latencies": rows,
        "slots": [],
    }
    if not paired:
        return summary
    summary["slots"] = [_slot_summary(slot, rows) for slot in range(2)]
    for key in (
        "aggregate_slowdown",
        "mean_slowdown",
        "pair_makespan_s",
        "submission_skew_s",
    ):
        summary[key] = distribution([trial[key] for trial in paired])
    return summary
