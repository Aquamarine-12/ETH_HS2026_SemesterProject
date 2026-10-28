"""Prepares Sirius startup configuration before the engine is imported.

The same renderer serves the preview command and real GPU experiments.
Each GPU run stores its effective YAML next to its measurement artifacts.
Device selection preserves a single GPU already assigned by the scheduler.
"""

import os
from pathlib import Path

from parallel_bench import config as configuration
from parallel_bench import models


def _mapping(parent: models.JsonObject, key: str) -> models.JsonObject:
    """Gets a nested YAML mapping, rejecting scalars and lists."""
    value = parent.setdefault(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"Sirius YAML {key!r} must contain a mapping")
    return value


def render_yaml(config: configuration.Config) -> str:
    """Renders effective YAML without importing DuckDB or requiring a GPU."""
    try:
        import yaml  # Optional dependency: CPU experiments do not need it.
    except ImportError as exc:
        raise RuntimeError(
            "Install the Sirius extra: pip install -e '.[sirius]'"
        ) from exc
    runtime: models.JsonObject = {}
    if config.sirius.config_path:
        source = Path(config.sirius.config_path).read_text(encoding="utf-8")
        runtime = yaml.safe_load(source)
    if not isinstance(runtime, dict):
        raise ValueError("Sirius YAML must contain a mapping")
    sirius = _mapping(runtime, "sirius")
    # Admission is part of the experiment, not a tunable engine default.
    sirius["max_concurrent_queries"] = 2
    executor = sirius.get("executor", {})
    if not isinstance(executor, dict):
        raise ValueError("Sirius executor must contain a mapping")
    scan = executor.get("scan_manager", {})
    if not isinstance(scan, dict):
        raise ValueError("Sirius scan_manager must contain a mapping")
    if "max_concurrent_queries" in scan:
        scan["max_concurrent_queries"] = 2
    return yaml.safe_dump(runtime, sort_keys=False)


def write_config(config: configuration.Config, target: Path) -> Path:
    """Writes a new effective YAML file, refusing to overwrite another file."""
    content = render_yaml(config)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x", encoding="utf-8") as handle:
        handle.write(content)
    return target


def configure_environment(config: configuration.Config, output: Path) -> None:
    """Sets runtime configuration and one visible GPU before loading Sirius."""
    if config.backend != models.BackendKind.SIRIUS:
        return
    target = write_config(config, output / "sirius-effective.yaml")
    os.environ["SIRIUS_CONFIG_FILE"] = str(target)
    visible = config.sirius.gpu
    if visible is None:
        visible = os.environ.get("CUDA_VISIBLE_DEVICES", "0")
    if not visible.strip() or visible == "-1" or "," in visible:
        raise ValueError(
            "Select one GPU with sirius.gpu or CUDA_VISIBLE_DEVICES"
        )
    os.environ["CUDA_VISIBLE_DEVICES"] = visible
    if not Path(config.sirius.extension_path).is_file():
        raise ValueError(
            f"Sirius extension not found: {config.sirius.extension_path}"
        )
