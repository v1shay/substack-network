import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.comments.comment_backfill import (
    run_backfill,
    seed_comment_publication_status,
    select_backfill_targets,
    summarize_backfill_state,
)
from scripts.db_runtime import ensure_schema


class TestCommentBackfill(unittest.TestCase):
    def test_backfill_processes_existing_publications_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = sqlite3.connect(Path(tmpdir) / "backfill.db")
            ensure_schema(conn)
            self._insert_publication(conn, "alpha", "101")
            self._insert_publication(conn, "beta", "102")
            conn.commit()

            stats = {
                "posts_seen": 1,
                "posts_created": 1,
                "posts_updated": 0,
                "users_seen": 2,
                "users_created": 2,
                "users_updated": 0,
                "comments_fetched": 3,
                "comments_unique": 3,
                "comments_created": 3,
                "comments_updated": 0,
            }

            with patch("scripts.comments.comment_backfill.process_comments", return_value=stats) as mocked:
                first = run_backfill(conn, limit=1, delay_seconds=0)
                second = run_backfill(conn, limit=10, delay_seconds=0)

            self.assertEqual((1, 1, 0), (first.selected, first.succeeded, first.failed))
            self.assertEqual((1, 1, 0), (second.selected, second.succeeded, second.failed))
            self.assertEqual(2, mocked.call_count)
            self.assertEqual({"succeeded": 2}, summarize_backfill_state(conn))

            rows = conn.execute(
                """
                SELECT domain, status, attempts, posts_seen, comments_created
                  FROM comment_publication_status
                 ORDER BY domain
                """
            ).fetchall()
            self.assertEqual(
                [
                    ("alpha", "succeeded", 1, 1, 3),
                    ("beta", "succeeded", 1, 1, 3),
                ],
                rows,
            )
            conn.close()

    def test_failed_publication_retries_until_max_attempts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = sqlite3.connect(Path(tmpdir) / "retry.db")
            ensure_schema(conn)
            self._insert_publication(conn, "alpha", "101")
            conn.commit()

            with patch("scripts.comments.comment_backfill.process_comments", side_effect=RuntimeError("boom")):
                result = run_backfill(conn, limit=10, delay_seconds=0, max_attempts=1)

            self.assertEqual((1, 0, 1), (result.selected, result.succeeded, result.failed))
            self.assertEqual([], select_backfill_targets(conn, limit=10, max_attempts=1))

            status = conn.execute(
                "SELECT status, attempts, last_error FROM comment_publication_status WHERE domain = 'alpha'"
            ).fetchone()
            self.assertEqual(("failed", 1, "boom"), status)
            conn.close()

    def test_seed_only_creates_pending_rows_without_fetching(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = sqlite3.connect(Path(tmpdir) / "seed.db")
            ensure_schema(conn)
            self._insert_publication(conn, "alpha", "101")
            self._insert_publication(conn, "beta", "102")
            conn.commit()

            count = seed_comment_publication_status(conn, limit=1)

            self.assertEqual(1, count)
            self.assertEqual({"pending": 1}, summarize_backfill_state(conn))
            conn.close()

    def _insert_publication(self, conn, domain, substack_id):
        conn.execute(
            """
            INSERT INTO publications (substack_id, name, domain, description, first_seen)
            VALUES (?, ?, ?, ?, '2026-01-01T00:00:00Z')
            """,
            (substack_id, domain.title(), domain, "desc"),
        )
