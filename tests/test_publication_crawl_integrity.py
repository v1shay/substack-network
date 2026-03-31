import io
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts.milestone01.crawl import (
    SubstackNetworkCrawler,
    extract_publication_info_from_post_metadata,
    resolve_publication_url,
)


class _FakeNewsletter:
    def __init__(self, url: str):
        self.url = url


class TestPublicationCrawlIntegrity(unittest.TestCase):
    def test_post_metadata_fallback_recovers_custom_domain_publication_fields(self) -> None:
        meta = {
            "publication_id": 159185,
            "description": "Start your day with pragmatic takes on politics and public policy.",
            "canonical_url": "https://www.slowboring.com/p/testing-trumps-influence",
            "publishedBylines": [
                {
                    "publicationUsers": [
                        {
                            "publication": {
                                "id": 159185,
                                "name": "Slow Boring",
                                "subdomain": "matthewyglesias",
                                "custom_domain": "www.slowboring.com",
                                "hero_text": "Start your day with pragmatic takes on politics and public policy.",
                            }
                        }
                    ]
                }
            ],
        }

        publication_info = extract_publication_info_from_post_metadata(meta)

        self.assertEqual(159185, publication_info["id"])
        self.assertEqual("Slow Boring", publication_info["name"])
        self.assertEqual(
            "Start your day with pragmatic takes on politics and public policy.",
            publication_info["hero_text"],
        )
        self.assertEqual("matthewyglesias", publication_info["subdomain"])
        self.assertEqual("www.slowboring.com", publication_info["custom_domain"])
        self.assertEqual(
            "https://www.slowboring.com",
            resolve_publication_url("slowboring.com", publication_info),
        )

    def test_publication_url_resolution_prefers_substack_subdomain_when_available(self) -> None:
        publication_info = {
            "id": 35345,
            "subdomain": "noahpinion",
            "custom_domain": None,
            "canonical_url": "https://www.noahpinion.blog/p/example",
        }

        self.assertEqual(
            "https://noahpinion.substack.com",
            resolve_publication_url("www.noahpinion.blog", publication_info),
        )

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

    def test_existing_publication_failure_preserves_crawled_queue_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "existing-publication.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            crawler.add_to_queue("alpha", 0)
            crawler.conn.execute(
                """
                INSERT INTO publications (substack_id, name, domain, description, first_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("pub-1", "Alpha", "alpha", "desc", "2026-01-01T00:00:00+00:00"),
            )
            crawler.conn.commit()

            output = io.StringIO()
            with patch("scripts.milestone01.crawl.Newsletter", _FakeNewsletter):
                with patch.object(SubstackNetworkCrawler, "get_publication_info", return_value=None):
                    with redirect_stdout(output):
                        crawler.crawl(max_publications=1, max_attempts=1, delay=0)

            conn = sqlite3.connect(db_path)
            status = conn.execute(
                "SELECT status FROM queue WHERE domain = ?",
                ("alpha",),
            ).fetchone()[0]
            conn.close()
            crawler.conn.close()

            self.assertEqual("crawled", status)
            self.assertIn("preserving queue status as crawled", output.getvalue())


if __name__ == "__main__":
    unittest.main()
