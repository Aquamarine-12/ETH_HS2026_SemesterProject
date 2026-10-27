"""Explicit completion-time and slowdown definitions."""

import statistics


def distribution(values):
    return {"count": len(values), "median": statistics.median(values),
            "mean": statistics.mean(values), "min": min(values), "max": max(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0}


def summarize(batches):
    measured = [b for b in batches if not b["warmup"]]
    repetitions = sorted({b["repetition"] for b in measured})
    paired = []
    for repetition in repetitions:
        group = {b["condition"]: b for b in measured if b["repetition"] == repetition}
        if set(group) != {"isolated_0", "isolated_1", "concurrent"}:
            continue
        if any(r["status"] != "ok" for b in group.values() for r in b["records"]):
            continue
        concurrent = {r["slot"]: r for r in group["concurrent"]["records"]}
        slowdowns = [concurrent[s]["completion_s"] / group[f"isolated_{s}"]["records"][0]["completion_s"]
                     for s in range(2)]
        paired.append({"repetition": repetition, "slowdowns": slowdowns,
                       "aggregate_slowdown": sum(slowdowns), "mean_slowdown": statistics.mean(slowdowns),
                       "pair_makespan_s": max(r["completion_s"] for r in concurrent.values()),
                       "submission_skew_s": abs(concurrent[0]["start_ns"] - concurrent[1]["start_ns"]) / 1e9})
    summary = {"complete_repetitions": len(paired), "paired_trials": paired, "slots": []}
    if not paired:
        return summary
    valid = {p["repetition"] for p in paired}
    for slot in range(2):
        isolated = [r["completion_s"] for b in measured if b["repetition"] in valid
                    and b["condition"] == f"isolated_{slot}" for r in b["records"]]
        concurrent = [r["completion_s"] for b in measured if b["repetition"] in valid
                      and b["condition"] == "concurrent" for r in b["records"] if r["slot"] == slot]
        summary["slots"].append({"slot": slot, "isolated_completion_s": distribution(isolated),
                                 "concurrent_completion_s": distribution(concurrent),
                                 "paired_slowdown": distribution([p["slowdowns"][slot] for p in paired]),
                                 "ratio_of_medians": statistics.median(concurrent) / statistics.median(isolated)})
    for key in ("aggregate_slowdown", "mean_slowdown", "pair_makespan_s", "submission_skew_s"):
        summary[key] = distribution([p[key] for p in paired])
    return summary
