import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class TestRecommendationRankOutputs(unittest.TestCase):
    def test_centrality_cli_uses_recommendation_rank_contract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "cartographer.db"
            csv_path = root / "recommendation_rank.csv"
            _write_rank_fixture(db_path)

            repo_root = Path(__file__).resolve().parents[1]
            result = subprocess.run(
                [
                    sys.executable,
                    str(repo_root / "scripts" / "milestone01" / "centrality.py"),
                    "--db",
                    str(db_path),
                    "-n",
                    "3",
                    "-o",
                    str(csv_path),
                ],
                cwd=repo_root,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual("", result.stderr)
            self.assertEqual(0, result.returncode)
            self.assertIn("Top 3 by RecommendationRank", result.stdout)
            self.assertIn("Algorithm: PageRank over directed publication recommendation edges.", result.stdout)
            self.assertIn("recommendation_rank", result.stdout)
            self.assertTrue(csv_path.exists())
            self.assertEqual(
                "rank,domain,name,recommendation_rank,in_degree,depth",
                csv_path.read_text(encoding="utf-8").splitlines()[0],
            )

    def test_distribution_json_declares_metric_and_algorithm(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "cartographer.db"
            _write_rank_fixture(db_path)

            repo_root = Path(__file__).resolve().parents[1]
            env = os.environ.copy()
            env["CARTOGRAPHER_ROOT"] = str(root)
            result = subprocess.run(
                [
                    sys.executable,
                    str(repo_root / "scripts" / "milestone02" / "pagerank_distribution.py"),
                    "--db",
                    str(db_path),
                    "--json",
                ],
                cwd=repo_root,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(0, result.returncode)
            output_path = root / "data" / "pagerank_distribution.json"
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual("RecommendationRank", payload["metric"])
            self.assertEqual(
                "PageRank over directed publication recommendation edges",
                payload["algorithm"],
            )
            self.assertEqual(3, payload["n_publications"])


def _write_rank_fixture(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE publications (
            id INTEGER PRIMARY KEY,
            substack_id TEXT UNIQUE,
            name TEXT,
            domain TEXT NOT NULL UNIQUE,
            description TEXT,
            first_seen TIMESTAMP
        );
        CREATE TABLE recommendations (
            id INTEGER PRIMARY KEY,
            source_domain TEXT NOT NULL,
            target_domain TEXT NOT NULL,
            UNIQUE(source_domain, target_domain)
        );
        CREATE TABLE queue (
            domain TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            depth INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO publications (name, domain) VALUES
            ('Alpha', 'alpha'),
            ('Beta', 'beta'),
            ('Gamma', 'gamma');
        INSERT INTO recommendations (source_domain, target_domain) VALUES
            ('alpha', 'beta'),
            ('alpha', 'gamma'),
            ('beta', 'gamma'),
            ('gamma', 'beta');
        INSERT INTO queue (domain, status, depth) VALUES
            ('alpha', 'crawled', 0),
            ('beta', 'crawled', 1),
            ('gamma', 'crawled', 1);
        """
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    unittest.main()
