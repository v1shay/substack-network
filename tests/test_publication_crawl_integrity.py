import io
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts.milestone01.crawl import SubstackNetworkCrawler


class _FakeNewsletter:
    def __init__(self, url: str):
        self.url = url


class TestPublicationCrawlIntegrity(unittest.TestCase):
    def test_core_schema_and_output_are_stable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "integrity.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)

            # Core schema should remain present and unchanged.
            conn = sqlite3.connect(db_path)
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            conn.close()
            self.assertTrue({"publications", "recommendations", "queue"}.issubset(tables))

            # Max 0 publications is a stable, no-network control mode.
            start = time.time()
            output = io.StringIO()
            with redirect_stdout(output):
                crawler.crawl(max_publications=0, delay=0)
            elapsed = time.time() - start

            text = output.getvalue()
            self.assertIn("--- Starting Network Crawl (Goal: 0 publications) ---", text)
            self.assertIn("✅ Crawl Complete. Processed 0 publications.", text)
            self.assertLess(elapsed, 2.0)

    def test_max_attempts_stops_runaway_failure_churn(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "attempt-cap.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            crawler.add_to_queue("alpha", 0)
            crawler.add_to_queue("beta", 0)
            crawler.add_to_queue("gamma", 0)

            output = io.StringIO()
            with patch("scripts.milestone01.crawl.Newsletter", _FakeNewsletter):
                with patch.object(SubstackNetworkCrawler, "get_publication_info", return_value=None):
                    with redirect_stdout(output):
                        crawler.crawl(max_publications=10, max_attempts=2, delay=0)

            conn = sqlite3.connect(db_path)
            failed_count = conn.execute(
                "SELECT COUNT(*) FROM queue WHERE status='failed'"
            ).fetchone()[0]
            pending_count = conn.execute(
                "SELECT COUNT(*) FROM queue WHERE status='pending'"
            ).fetchone()[0]
            conn.close()

            self.assertEqual(failed_count, 2)
            self.assertEqual(pending_count, 1)
            self.assertIn(
                "--- Crawl Stopped (Attempt Cap Reached: 2) ---",
                output.getvalue(),
            )


if __name__ == "__main__":
    unittest.main()
