import json
import os
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.comments import readiness_audit
from scripts.db_runtime import ensure_schema


class TestReadinessAudit(unittest.TestCase):
    def _prepare_db(self, path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path)
        ensure_schema(conn)
        conn.execute(
            """
            INSERT INTO publications (id, substack_id, name, domain, description, first_seen)
            VALUES (1, 'pub-1', 'Publication', 'alpha', 'desc', '2026-01-01T00:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO posts (
                id, external_post_id, publication_substack_id, title, url, published_at, first_seen, last_seen
            ) VALUES (
                1, 'post-1', 'pub-1', 'Post', 'https://example.com/p/post-1',
                '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00'
            )
            """
        )
        return conn

    def test_anomaly_increases_only_reports_regressions(self):
        increases = readiness_audit.anomaly_increases(
            {"a": 1, "b": 5},
            {"a": 1, "b": 7, "c": 0},
        )
        self.assertEqual(
            {"b": {"baseline": 5, "current": 7, "delta": 2}},
            increases,
        )

    def test_stage_command_includes_comment_and_classification_flags(self):
        stage = readiness_audit.StageConfig(
            name="stage1",
            comment_post_limit=10,
            classification_max_users=100,
            classification_workers=4,
            max_publications=25,
            max_attempts=60,
            delay=0.25,
        )
        cmd = readiness_audit._stage_command(Path("/repo"), "config/seeds.md", stage)
        self.assertIn("--enable-comments", cmd)
        self.assertIn("--classify-commenters", cmd)
        self.assertIn("--comment-post-limit", cmd)
        self.assertIn("--classification-max-users", cmd)
        self.assertIn("--max-publications", cmd)
        self.assertIn("--max-attempts", cmd)

    def test_audit_delta_metadata_allows_explicit_edge_cases(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "audit.db")
            conn = self._prepare_db(db_path)
            conn.execute(
                """
                INSERT INTO users (
                    id, external_user_id, name, handle, profile_url,
                    publication_substack_id, publication_role, is_publication_owner, first_seen, last_seen
                ) VALUES (
                    1, 'user-1', 'User', 'user', NULL,
                    NULL, NULL, 0, '2026-01-02T00:00:00+00:00', '2026-01-02T00:00:00+00:00'
                )
                """
            )
            conn.execute(
                """
                INSERT INTO comments (
                    id, external_comment_id, post_id, user_id, parent_comment_id,
                    parent_external_comment_id, body, commented_at, raw_json, first_seen, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    1,
                    "comment-1",
                    1,
                    None,
                    None,
                    None,
                    "",
                    "2026-01-02T00:00:00+00:00",
                    json.dumps({"deleted": True, "user": None}),
                    "2026-01-02T00:00:00+00:00",
                    "2026-01-02T00:00:00+00:00",
                ),
            )
            conn.commit()
            conn.close()

            report = readiness_audit.audit_delta_comment_metadata(
                db_path,
                baseline_max_ids={"users": 0, "posts": 0, "comments": 0},
            )

            self.assertEqual([], report["hard_failures"])
            self.assertEqual(1, report["metrics"]["comments_missing_user_id_allowed_edge_case"])
            self.assertEqual(1, report["metrics"]["comments_missing_body_allowed_deleted"])

    def test_audit_delta_metadata_flags_missing_required_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "audit.db")
            conn = self._prepare_db(db_path)
            conn.execute(
                """
                INSERT INTO comments (
                    id, external_comment_id, post_id, user_id, parent_comment_id,
                    parent_external_comment_id, body, commented_at, raw_json, first_seen, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    1,
                    "comment-1",
                    1,
                    None,
                    None,
                    "parent-1",
                    "",
                    "",
                    json.dumps({}),
                    "2026-01-02T00:00:00+00:00",
                    "2026-01-02T00:00:00+00:00",
                ),
            )
            conn.commit()
            conn.close()

            report = readiness_audit.audit_delta_comment_metadata(
                db_path,
                baseline_max_ids={"users": 0, "posts": 0, "comments": 0},
            )

            self.assertIn("new comments missing valid commented_at", report["hard_failures"])
            self.assertIn("reply comments missing parent_comment_id", report["hard_failures"])
            self.assertIn(
                "new comments missing user_id without explicit unavailable-user payload markers",
                report["hard_failures"],
            )
            self.assertIn(
                "new comments missing raw text without deleted=true payload markers",
                report["hard_failures"],
            )

    def test_run_metadata_check_from_files_writes_report_and_exit_code(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "audit.db")
            conn = self._prepare_db(db_path)
            conn.execute(
                """
                INSERT INTO users (
                    id, external_user_id, name, handle, profile_url,
                    publication_substack_id, publication_role, is_publication_owner, first_seen, last_seen
                ) VALUES (
                    1, 'user-1', 'User', 'user', NULL,
                    NULL, NULL, 0, '2026-01-02T00:00:00+00:00', '2026-01-02T00:00:00+00:00'
                )
                """
            )
            conn.execute(
                """
                INSERT INTO comments (
                    id, external_comment_id, post_id, user_id, parent_comment_id,
                    parent_external_comment_id, body, commented_at, raw_json, first_seen, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    1,
                    "comment-1",
                    1,
                    1,
                    None,
                    None,
                    "hello",
                    "2026-01-02T00:00:00+00:00",
                    json.dumps({"user_id": 1}),
                    "2026-01-02T00:00:00+00:00",
                    "2026-01-02T00:00:00+00:00",
                ),
            )
            conn.commit()
            conn.close()

            baseline_path = Path(tmpdir) / "baseline.json"
            output_path = Path(tmpdir) / "report.json"
            baseline_path.write_text(
                json.dumps({"max_ids": {"users": 0, "posts": 0, "comments": 0}}),
                encoding="utf-8",
            )

            rc = readiness_audit.run_metadata_check_from_files(
                db_path=Path(db_path),
                baseline_snapshot_path=baseline_path,
                output_path=output_path,
            )
            self.assertEqual(0, rc)
            self.assertTrue(output_path.exists())

    def test_write_metadata_check_script_creates_executable_wrapper(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            script_path = Path(tmpdir) / "stage1-metadata-check.py"
            baseline_path = Path(tmpdir) / "baseline.json"
            output_path = Path(tmpdir) / "report.json"
            readiness_audit.write_metadata_check_script(
                repo_root=Path("/repo"),
                db_path=Path("/repo/cartographer.db"),
                baseline_snapshot_path=baseline_path,
                output_path=output_path,
                script_path=script_path,
            )
            body = script_path.read_text(encoding="utf-8")
            self.assertIn("run_metadata_check_from_files", body)
            self.assertIn(str(output_path), body)
            self.assertTrue(os.access(script_path, os.X_OK))

    def test_git_branch_failure_raises_audit_failure(self):
        with patch("scripts.comments.readiness_audit.subprocess.run") as mocked_run:
            mocked_run.return_value = subprocess.CompletedProcess(
                args=["git"],
                returncode=1,
                stdout="",
                stderr="boom",
            )
            with self.assertRaises(readiness_audit.AuditFailure):
                readiness_audit._git_branch(Path("/repo"))

    def test_evaluate_comment_error_budget_aggregates_logs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stage1_log = Path(tmpdir) / "stage1-crawl.log"
            stage2_log = Path(tmpdir) / "stage2-crawl.log"
            stage1_log.write_text(
                "\n".join(
                    [
                        "[comments][error] domain=alpha: boom",
                        "✅ Crawl Complete. Processed 25 publications.",
                    ]
                ),
                encoding="utf-8",
            )
            stage2_log.write_text(
                "\n".join(
                    [
                        "[comments][error] domain=beta: boom",
                        "[comments][error] domain=alpha: again",
                        "✅ Crawl Complete. Processed 100 publications.",
                    ]
                ),
                encoding="utf-8",
            )

            report = readiness_audit.evaluate_comment_error_budget(
                {"stage1": stage1_log, "stage2": stage2_log},
                max_rate=0.02,
            )

            self.assertEqual(125, report["total_crawled_publications"])
            self.assertEqual(2, report["comment_error_publications"])
            self.assertAlmostEqual(2 / 125, report["comment_error_rate"])
            self.assertTrue(report["passes"])

    def test_evaluate_comment_error_budget_fails_when_rate_exceeds_threshold(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stage1_log = Path(tmpdir) / "stage1-crawl.log"
            stage2_log = Path(tmpdir) / "stage2-crawl.log"
            stage1_log.write_text(
                "[comments][error] domain=alpha: boom\n✅ Crawl Complete. Processed 10 publications.\n",
                encoding="utf-8",
            )
            stage2_log.write_text(
                "[comments][error] domain=beta: boom\n✅ Crawl Complete. Processed 10 publications.\n",
                encoding="utf-8",
            )

            report = readiness_audit.evaluate_comment_error_budget(
                {"stage1": stage1_log, "stage2": stage2_log},
                max_rate=0.05,
            )

            self.assertFalse(report["passes"])

    def test_evaluate_comment_error_budget_parses_url_based_comment_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stage1_log = Path(tmpdir) / "stage1-crawl.log"
            stage2_log = Path(tmpdir) / "stage2-crawl.log"
            stage1_log.write_text(
                "\n".join(
                    [
                        "[comments][error] transient status code 429 for https://alpha.substack.com/api/v1/post/1/comments (attempt 1/3)",
                        "[comments][error] transient status code 429 for https://substack.com/api/v1/user/alpha/public_profile (attempt 1/2)",
                        "✅ Crawl Complete. Processed 10 publications.",
                    ]
                ),
                encoding="utf-8",
            )
            stage2_log.write_text(
                "\n".join(
                    [
                        "[comments][error] domain=beta: boom",
                        "[comments][error] transient status code 429 for https://gamma.substack.com/api/v1/posts/2/comments (attempt 1/3)",
                        "✅ Crawl Complete. Processed 20 publications.",
                    ]
                ),
                encoding="utf-8",
            )

            report = readiness_audit.evaluate_comment_error_budget(
                {"stage1": stage1_log, "stage2": stage2_log},
                max_rate=0.2,
            )

            self.assertEqual(30, report["total_crawled_publications"])
            self.assertEqual(3, report["comment_error_publications"])
            self.assertCountEqual(
                ["alpha.substack.com", "beta", "gamma.substack.com"],
                report["comment_error_domains"],
            )


if __name__ == "__main__":
    unittest.main()
