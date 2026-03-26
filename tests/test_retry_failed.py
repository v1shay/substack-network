import importlib
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


class TestRetryFailed(unittest.TestCase):
    def test_main_refuses_to_run_when_crawler_lock_is_held(self):
        fake_substack_api = types.SimpleNamespace(Newsletter=object)
        sys.modules.pop("scripts.milestone02.retry_failed", None)
        with patch.dict(sys.modules, {"substack_api": fake_substack_api}):
            retry_failed = importlib.import_module("scripts.milestone02.retry_failed")
            retry_failed = importlib.reload(retry_failed)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "cartographer.db"
            sqlite3.connect(db_path).close()
            (root / ".crawler.lock").write_text(str(os.getpid()), encoding="utf-8")

            with patch.dict(os.environ, {"CARTOGRAPHER_ROOT": str(root)}, clear=False):
                with patch("sys.argv", ["retry_failed.py", "--db", str(db_path)]):
                    with self.assertRaises(SystemExit) as exc:
                        retry_failed.main()

        self.assertEqual(1, exc.exception.code)


if __name__ == "__main__":
    unittest.main()
