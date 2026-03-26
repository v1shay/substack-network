import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.comments import quality_gate


def _completed(*, returncode: int, stdout: str = "", stderr: str = ""):
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


class TestQualityGate(unittest.TestCase):
    @patch("scripts.comments.quality_gate.subprocess.run")
    def test_live_endpoint_test_strict_sets_env_and_fails_on_skip(self, mocked_run):
        mocked_run.return_value = _completed(returncode=0, stdout="OK (skipped=1)\n")

        ok = quality_gate._run_live_endpoint_test(require_no_skips=True, strict_live=True)

        self.assertFalse(ok)
        self.assertEqual(1, mocked_run.call_count)
        kwargs = mocked_run.call_args.kwargs
        self.assertEqual(quality_gate.REPO_ROOT, kwargs["cwd"])
        env = kwargs["env"]
        self.assertEqual("1", env["SUBSTACK_LIVE_TESTS"])
        self.assertEqual("1", env["SUBSTACK_STRICT_LIVE"])

    @patch("scripts.comments.quality_gate.subprocess.run")
    def test_live_endpoint_test_allows_skip_when_not_required(self, mocked_run):
        mocked_run.return_value = _completed(returncode=0, stdout="OK (skipped=1)\n")

        ok = quality_gate._run_live_endpoint_test(require_no_skips=False, strict_live=False)

        self.assertTrue(ok)

    @patch("scripts.comments.quality_gate.subprocess.run")
    def test_bounded_crawl_uses_repo_cwd_and_runtime_root(self, mocked_run):
        mocked_run.return_value = _completed(returncode=0)

        with tempfile.TemporaryDirectory() as tmpdir:
            ok = quality_gate._run_bounded_crawl(
                Path(tmpdir),
                "config/seeds.md",
                2,
                0.25,
                4,
                enable_comments=True,
                comment_post_limit=3,
                classify_commenters=True,
                classification_max_users=25,
                classification_workers=4,
            )

        self.assertTrue(ok)
        kwargs = mocked_run.call_args.kwargs
        self.assertEqual(quality_gate.REPO_ROOT, kwargs["cwd"])
        self.assertEqual(tmpdir, kwargs["env"]["CARTOGRAPHER_ROOT"])

        cmd = mocked_run.call_args.args[0]
        self.assertEqual(str(quality_gate.REPO_ROOT / "scripts/milestone01/crawl.py"), cmd[1])
        seeds_value = cmd[cmd.index("--seeds-file") + 1]
        self.assertEqual(str((quality_gate.REPO_ROOT / "config/seeds.md").resolve()), seeds_value)
        self.assertIn("--classify-commenters", cmd)

    @patch("scripts.comments.quality_gate.subprocess.run")
    def test_wrapper_checks_use_safe_modes(self, mocked_run):
        mocked_run.return_value = _completed(returncode=0)

        with tempfile.TemporaryDirectory() as tmpdir:
            ok = quality_gate._run_wrapper_checks(Path(tmpdir))

        self.assertTrue(ok)
        commands = [call.args[0] for call in mocked_run.call_args_list]
        self.assertEqual(
            [sys.executable, str(quality_gate.REPO_ROOT / "scripts" / "update_graph.py"), "--no-open"],
            commands[0],
        )
        self.assertEqual(
            [
                sys.executable,
                str(quality_gate.REPO_ROOT / "scripts" / "gh_pages.py"),
                "--dry-run",
                "--no-commit",
                "--no-sync",
            ],
            commands[1],
        )
        self.assertEqual(
            [
                sys.executable,
                str(quality_gate.REPO_ROOT / "scripts" / "milestone02" / "label_topics_llm.py"),
                "--check-config",
            ],
            commands[2],
        )

    @patch("scripts.comments.quality_gate._print_commit_step")
    @patch("scripts.comments.quality_gate._run_post_repair_deterministic_suite", return_value=True)
    @patch("scripts.comments.quality_gate._run_audit_and_repair_loop", return_value=True)
    @patch("scripts.comments.quality_gate._run_live_endpoint_test", return_value=False)
    @patch("scripts.comments.quality_gate._run_unittest_discovery", return_value=True)
    def test_main_live_failure_is_advisory_by_default(
        self,
        _mock_discovery,
        mock_live,
        mock_audit,
        _mock_post_suite,
        _mock_print_commit,
    ):
        with patch.object(sys, "argv", ["quality_gate.py", "--allow-live-skip"]):
            rc = quality_gate.main()

        self.assertEqual(0, rc)
        mock_live.assert_called_once_with(require_no_skips=False, strict_live=False)
        mock_audit.assert_called_once()

    @patch("scripts.comments.quality_gate._run_audit_and_repair_loop", return_value=True)
    @patch("scripts.comments.quality_gate._run_live_endpoint_test", return_value=False)
    @patch("scripts.comments.quality_gate._run_unittest_discovery", return_value=True)
    def test_main_strict_live_failure_hard_fails(
        self,
        _mock_discovery,
        mock_live,
        mock_audit,
    ):
        with patch.object(sys, "argv", ["quality_gate.py", "--strict-live"]):
            rc = quality_gate.main()

        self.assertEqual(1, rc)
        mock_live.assert_called_once_with(require_no_skips=True, strict_live=True)
        mock_audit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
