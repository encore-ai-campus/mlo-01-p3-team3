from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from helpers import fixture
from product_pipeline.cli import main
from product_pipeline.files import atomic_json, read_json, rows
from product_pipeline.snapshot import prepare


class CliTests(unittest.TestCase):
    def test_plan_is_network_free_and_creates_no_state(self):
        with tempfile.TemporaryDirectory() as root:
            source, media = fixture(root, codes=("11",))
            sealed = Path(root) / "snapshot"
            prepare(source, sealed, media)
            paths_before = {p.relative_to(Path(root)) for p in Path(root).rglob("*")}
            output = io.StringIO()
            with patch("socket.socket", side_effect=AssertionError("network forbidden")), redirect_stdout(output):
                result = main(["plan", "--snapshot", str(sealed)])
            self.assertEqual(result, 0)
            self.assertEqual(json.loads(output.getvalue())["external_writes"], 0)
            self.assertEqual(paths_before, {p.relative_to(Path(root)) for p in Path(root).rglob("*")})

    def test_rehearsal_from_cli_resumes_and_production_mode_cannot_reuse_state(self):
        with tempfile.TemporaryDirectory() as root:
            source, media = fixture(root, codes=("11",))
            sealed, state = Path(root) / "snapshot", Path(root) / "state"
            prepare(source, sealed, media)
            args = ["rehearse", "--snapshot", str(sealed), "--state-dir", str(state)]
            for repeat in range(2):
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(main(args), 0)
                report = json.loads(output.getvalue().splitlines()[-1])
                self.assertEqual(report["events_sent"], 0 if repeat else 1)
            config = Path(root) / "config.json"
            atomic_json(config, {"environment_id": "rehearsal", "mongo_database": "rehearsal", "image_bucket": "rehearsal-images"})
            with patch("product_pipeline.cli._live", side_effect=AssertionError("must not connect")), redirect_stdout(io.StringIO()):
                self.assertEqual(main(["ingest", "--snapshot", str(sealed), "--state-dir", str(state), "--config", str(config)]), 2)

    def test_invalid_snapshot_prevents_live_constructor(self):
        with tempfile.TemporaryDirectory() as root:
            config = Path(root) / "config.json"
            atomic_json(config, {"environment_id": "test", "mongo_database": "test", "image_bucket": "images"})
            with patch("product_pipeline.cli._live") as live, redirect_stdout(io.StringIO()):
                self.assertEqual(main(["ingest", "--snapshot", str(Path(root)/"missing"), "--state-dir", str(Path(root)/"state"), "--config", str(config)]), 2)
                live.assert_not_called()

    def test_backend_exception_does_not_expose_secret(self):
        with tempfile.TemporaryDirectory() as root:
            config = Path(root) / "config.json"
            atomic_json(config, {"environment_id": "test", "mongo_database": "test", "image_bucket": "images"})
            output = io.StringIO()
            with patch("product_pipeline.cli._live", side_effect=RuntimeError("mongodb://user:SECRET@private")), redirect_stdout(output):
                self.assertEqual(main(["preflight", "--config", str(config)]), 2)
            self.assertNotIn("SECRET", output.getvalue())


if __name__ == "__main__":
    unittest.main()
