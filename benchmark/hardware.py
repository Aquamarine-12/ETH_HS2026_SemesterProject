"""Reads CPU and NVIDIA GPU model names on the measurement node."""

from pathlib import Path
import subprocess


def cpu_model() -> str | None:
    """Reads the CPU model names reported by Linux."""
    models = []
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        key, _, value = line.partition(":")
        if key.strip() == "model name":
            models.append(value.strip())
    return ", ".join(dict.fromkeys(models)) or None


def gpu_model() -> str | None:
    """Reads NVIDIA GPU model names, or None when unavailable."""
    try:
        output = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            text=True, stderr=subprocess.DEVNULL,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    models = [line.strip() for line in output.splitlines() if line.strip()]
    return ", ".join(dict.fromkeys(models)) or None


def hardware_info() -> dict[str, str | None]:
    """Returns model names to store with a completed measurement."""
    return {"cpu_model": cpu_model(), "gpu_model": gpu_model()}


if __name__ == "__main__":
    info = hardware_info()
    print(f"CPU: {info['cpu_model']} | GPU: {info['gpu_model']}")
