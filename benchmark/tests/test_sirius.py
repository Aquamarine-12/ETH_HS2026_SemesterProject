"""Checks Sirius configuration without loading its native GPU extension.

These tests exercise the public YAML preview and actual startup renderer.
They verify admission overrides, preserved settings and scheduler GPU selection.
GPU execution itself is intentionally not simulated as successful validation.
"""

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from parallel_bench import cli, config, models, sirius

try:
    import yaml
except ImportError:
    yaml = None


@unittest.skipIf(yaml is None, "Install PyYAML for Sirius configuration tests")
class SiriusConfigTests(unittest.TestCase):
    """Covers configuration output and GPU selection on a CPU-only host."""

    def setUp(self) -> None:
        """Creates a private extension placeholder and source YAML."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        extension = self.directory / "sirius.duckdb_extension"
        extension.touch()
        source = self.directory / "input.yaml"
        source.write_text(
            "sirius:\n  max_concurrent_queries: 1\n  executor:\n"
            "    scan_manager:\n      max_concurrent_queries: 1\n"
            "      memory_prefetcher:\n        enable: true\n"
        )
        self.config = config.Config(
            backend=models.BackendKind.SIRIUS,
            expected_source_id="0123456789",
            sirius=config.SiriusConfig(
                extension_path=str(extension), config_path=str(source)
            ),
        )

    def test_effective_yaml_preserves_settings_and_changes_admission(
        self,
    ) -> None:
        """Overrides both admission spellings without discarding other YAML."""
        output = self.directory / "effective.yaml"
        sirius.write_config(self.config, output)
        runtime = yaml.safe_load(output.read_text())["sirius"]
        self.assertEqual(runtime["max_concurrent_queries"], 2)
        scan = runtime["executor"]["scan_manager"]
        self.assertEqual(scan["max_concurrent_queries"], 2)
        self.assertTrue(scan["memory_prefetcher"]["enable"])
        with self.assertRaises(FileExistsError):
            sirius.write_config(self.config, output)

    def test_scheduler_gpu_is_preserved(self) -> None:
        """Keeps a scheduler-assigned device instead of changing it to zero."""
        with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "GPU-test"}):
            sirius.configure_environment(self.config, self.directory)
            self.assertEqual(os.environ["CUDA_VISIBLE_DEVICES"], "GPU-test")
            path = Path(os.environ["SIRIUS_CONFIG_FILE"])
            self.assertTrue(path.is_file())
            self.assertEqual(path.name, "sirius-effective.yaml")

    def test_multiple_inherited_gpus_require_a_selection(self) -> None:
        """Rejects an ambiguous allocation for a single-GPU experiment."""
        with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "0,1"}):
            with self.assertRaisesRegex(ValueError, "Select one GPU"):
                sirius.configure_environment(self.config, self.directory)

    def test_cli_preview_needs_no_extension_binary(self) -> None:
        """Writes effective YAML without a GPU or installed Sirius extension."""
        source = self.directory / "preview.toml"
        source.write_text(
            '[benchmark]\nbackend="sirius"\n'
            'expected_source_id="0123456789"\n'
            '[sirius]\nextension_path="/missing/extension"\n'
        )
        output = self.directory / "preview.yaml"
        with contextlib.redirect_stdout(io.StringIO()):
            status = cli.main(
                [
                    "sirius-config",
                    "--config",
                    str(source),
                    "--output",
                    str(output),
                ]
            )
        self.assertEqual(status, 0)
        runtime = yaml.safe_load(output.read_text())
        self.assertEqual(runtime["sirius"]["max_concurrent_queries"], 2)


if __name__ == "__main__":
    unittest.main()
