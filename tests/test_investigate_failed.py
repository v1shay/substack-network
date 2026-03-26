import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.milestone02 import investigate_failed


class TestInvestigateFailed(unittest.TestCase):
    def test_ensure_failed_csv_builds_from_queue(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "cartographer.db"
            csv_path = root / "data" / "failed_publications.csv"

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE queue (domain TEXT PRIMARY KEY, status TEXT DEFAULT 'pending', depth INTEGER DEFAULT 0)"
            )
            cur.execute(
                "INSERT INTO queue (domain, status, depth) VALUES (?, ?, ?)",
                ("platformer.news", "failed", 2),
            )
            cur.execute(
                "INSERT INTO queue (domain, status, depth) VALUES (?, ?, ?)",
                ("popular.info", "crawled", 0),
            )
            conn.commit()
            conn.close()

            self.assertTrue(investigate_failed.ensure_failed_csv(root, csv_path))
            self.assertTrue(csv_path.exists())

            with open(csv_path, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["domain"], "platformer.news")
            self.assertEqual(rows[0]["depth"], "2")
            self.assertEqual(rows[0]["url"], "https://platformer.news")

    def test_ensure_failed_csv_returns_false_without_db(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            csv_path = root / "data" / "failed_publications.csv"
            self.assertFalse(investigate_failed.ensure_failed_csv(root, csv_path))
            self.assertFalse(csv_path.exists())

    def test_classify_result_prefers_redirect_and_api_statuses(self) -> None:
        redirected = investigate_failed.classify_result(
            homepage={"status": "200", "final_url": "https://other.example", "error": ""},
            archive={"status": "404", "final_url": "https://example.substack.com/api/v1/archive", "error": ""},
            publication={"status": "404", "final_url": "https://example.substack.com/api/v1/publication", "error": ""},
            base_url="https://example.substack.com",
        )
        self.assertEqual("redirected_elsewhere", redirected)

        publication_ok = investigate_failed.classify_result(
            homepage={"status": "200", "final_url": "https://example.substack.com", "error": ""},
            archive={"status": "404", "final_url": "https://example.substack.com/api/v1/archive", "error": ""},
            publication={"status": "200", "final_url": "https://example.substack.com/api/v1/publication", "error": ""},
            base_url="https://example.substack.com",
        )
        self.assertEqual("publication_api_ok", publication_ok)

        archive_ok = investigate_failed.classify_result(
            homepage={"status": "200", "final_url": "https://example.substack.com", "error": ""},
            archive={"status": "200", "final_url": "https://example.substack.com/api/v1/archive", "error": ""},
            publication={"status": "404", "final_url": "https://example.substack.com/api/v1/publication", "error": ""},
            base_url="https://example.substack.com",
        )
        self.assertEqual("archive_ok", archive_ok)

    def test_build_record_probes_all_surfaces(self) -> None:
        with patch(
            "scripts.milestone02.investigate_failed.probe_endpoint",
            side_effect=[
                {"status": "200", "final_url": "https://example.substack.com", "error": ""},
                {"status": "500", "final_url": "https://example.substack.com/api/v1/archive?sort=new&offset=0&limit=1", "error": ""},
                {"status": "404", "final_url": "https://example.substack.com/api/v1/publication", "error": ""},
            ],
        ) as probe_mock:
            record = investigate_failed.build_record(
                "example.substack.com",
                3,
                "https://example.substack.com",
                headers={"User-Agent": "x"},
                timeout=5,
            )

        self.assertEqual("homepage_up", record["classification"])
        self.assertEqual("200", record["homepage_status"])
        self.assertEqual("500", record["archive_status"])
        self.assertEqual("404", record["publication_status"])
        self.assertEqual(3, probe_mock.call_count)


if __name__ == "__main__":
    unittest.main()
