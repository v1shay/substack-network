import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import update_graph


class _DummyProcess:
    def __init__(self, returncode: int = 0) -> None:
        self.returncode = returncode

    def wait(self, timeout=None) -> int:  # noqa: ANN001
        return self.returncode


class TestUpdateGraph(unittest.TestCase):
    def test_uses_code_root_for_scripts_and_runtime_root_for_db_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runtime_root = Path(td)
            (runtime_root / "data").mkdir(parents=True, exist_ok=True)
            (runtime_root / "index.html").write_text("<html></html>", encoding="utf-8")

            popen_calls: list[dict] = []

            def _fake_popen(cmd, cwd=None, env=None, start_new_session=False, stdin=None, stdout=None, stderr=None):  # noqa: ANN001
                popen_calls.append(
                    {
                        "cmd": cmd,
                        "cwd": cwd,
                        "env": env,
                        "start_new_session": start_new_session,
                        "stdin": stdin,
                        "stdout": stdout,
                        "stderr": stderr,
                    }
                )
                return _DummyProcess(returncode=0)

            with patch.dict(os.environ, {"CARTOGRAPHER_ROOT": str(runtime_root)}, clear=False):
                with patch("scripts.update_graph.subprocess.run") as run_mock:
                    with patch("scripts.update_graph.subprocess.Popen", side_effect=_fake_popen):
                        with patch("scripts.update_graph.webbrowser.open") as open_mock:
                            run_mock.return_value = SimpleNamespace(returncode=0)
                            with patch("sys.argv", ["update_graph.py", "--no-open"]):
                                update_graph.main()

            open_mock.assert_not_called()

            self.assertGreaterEqual(len(popen_calls), 2)
            first_popen = popen_calls[0]
            self.assertEqual(Path(first_popen["cwd"]), update_graph.CODE_ROOT)
            self.assertEqual(
                Path(first_popen["env"]["CARTOGRAPHER_ROOT"]).resolve(),
                runtime_root.resolve(),
            )
            self.assertIn(
                str(update_graph.CODE_ROOT / "scripts" / "milestone01" / "crawl.py"),
                first_popen["cmd"],
            )
            self.assertTrue(first_popen["start_new_session"])
            self.assertEqual(subprocess.DEVNULL, first_popen["stdin"])
            self.assertEqual(subprocess.DEVNULL, first_popen["stdout"])
            self.assertEqual(subprocess.DEVNULL, first_popen["stderr"])

            second_popen = popen_calls[1]
            self.assertTrue(second_popen["start_new_session"])
            self.assertEqual(subprocess.DEVNULL, second_popen["stdin"])
            self.assertEqual(subprocess.DEVNULL, second_popen["stdout"])
            self.assertEqual(subprocess.DEVNULL, second_popen["stderr"])

            # All subprocess.run calls should execute from the code repo while targeting runtime root via env.
            for call in run_mock.call_args_list:
                self.assertEqual(Path(call.kwargs["cwd"]), update_graph.CODE_ROOT)
                self.assertEqual(
                    Path(call.kwargs["env"]["CARTOGRAPHER_ROOT"]).resolve(),
                    runtime_root.resolve(),
                )


if __name__ == "__main__":
    unittest.main()
