import os
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.milestone02 import label_topics_llm


class TestLabelTopicsLLM(unittest.TestCase):
    def test_check_config_is_read_only(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "cartographer.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE publications (
                    id INTEGER PRIMARY KEY,
                    substack_id TEXT,
                    name TEXT,
                    domain TEXT NOT NULL,
                    description TEXT,
                    first_seen TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO publications (substack_id, name, domain, description, first_seen)
                VALUES ('1', 'Example', 'example.substack.com', 'desc', '2026-01-01T00:00:00Z')
                """
            )
            conn.commit()
            conn.close()

            fake_openai = types.SimpleNamespace()
            with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}, clear=False):
                with patch.dict("sys.modules", {"openai": fake_openai}):
                    with patch("sys.argv", ["label_topics_llm.py", "--db", str(db_path), "--check-config"]):
                        label_topics_llm.main()

            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='publication_topics'"
            ).fetchone()
            conn.close()
            self.assertIsNone(row)


if __name__ == "__main__":
    unittest.main()
