import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import gh_pages


def _completed(*, returncode: int = 0, stdout: str = "", stderr: str = ""):
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


class TestGHPages(unittest.TestCase):
    def test_dry_run_does_not_commit_or_push(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "data").mkdir(parents=True, exist_ok=True)
            (root / "data" / "substack_graph.html").write_text("<html></html>", encoding="utf-8")

            calls = []

            def fake_run(cmd, cwd, check=True):  # noqa: ANN001
                calls.append(cmd)
                if cmd[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
                    return _completed(stdout="true\n")
                if cmd[:4] == ["git", "rev-parse", "--abbrev-ref", "HEAD"]:
                    return _completed(stdout="main\n")
                if cmd[:3] == ["git", "status", "--porcelain"]:
                    return _completed(stdout=" M tracked.txt\n")
                if cmd[:4] == ["git", "rev-parse", "--verify", "pages"]:
                    return _completed(returncode=0, stdout="pages\n")
                self.fail(f"unexpected command: {cmd}")

            with patch.dict(os.environ, {"CARTOGRAPHER_ROOT": str(root)}, clear=False):
                with patch("scripts.gh_pages.run", side_effect=fake_run):
                    with patch("sys.argv", ["gh_pages.py", "--dry-run"]):
                        gh_pages.main()

            self.assertIn(["git", "status", "--porcelain"], calls)
            self.assertNotIn(["git", "add", "-A"], calls)
            self.assertFalse(any(cmd[:2] == ["git", "commit"] for cmd in calls))
            self.assertFalse(any(cmd[:2] == ["git", "push"] for cmd in calls))


if __name__ == "__main__":
    unittest.main()
