"""Small, inspectable experiment artifacts (no Git commands or dependencies)."""

import json
import os
from pathlib import Path
import platform
import subprocess


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def hardware_info():
    result = {"platform": platform.platform(), "python": platform.python_version(),
              "logical_cpus": os.cpu_count(), "cpu_model": platform.processor()}
    if hasattr(os, "sched_getaffinity"):
        result["cpu_affinity"] = sorted(os.sched_getaffinity(0))
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                result["cpu_model"] = line.partition(":")[2].strip()
                break
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        result["host_mem_total"] = meminfo.read_text().splitlines()[0]
    try:
        gpu = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,uuid,memory.total,driver_version", "--format=csv"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        result["nvidia_smi"] = {"returncode": gpu.returncode, "stdout": gpu.stdout, "stderr": gpu.stderr}
    except (OSError, subprocess.TimeoutExpired) as exc:
        result["nvidia_smi"] = {"unavailable": str(exc)}
    return result
