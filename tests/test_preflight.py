import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.db_runtime import SCHEMA_VERSION, connect_db, ensure_schema
from scripts.utils.preflight import (
    BACKUP_ERROR,
    LOCK_ERROR,
    SCHEMA_ERROR,
    acquire_backfill_lock,
    release_backfill_lock,
    verify_backup_exists,
    verify_schema_ready,
)


class TestIngestionPreflight(unittest.TestCase):
    def test_backup_check_requires_readable_nonempty_backup(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "cartographer.db"
            db_path.write_bytes(b"db")

            with self.assertRaisesRegex(RuntimeError, BACKUP_ERROR):
                verify_backup_exists(db_path)

            backup_path = Path(td) / "cartographer.db.backup"
            backup_path.write_bytes(b"backup")

            self.assertEqual(backup_path.resolve(), verify_backup_exists(db_path))

    def test_backup_check_accepts_timestamped_backup(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "cartographer.db"
            db_path.write_bytes(b"db")
            backup_path = Path(td) / "cartographer.db.20260416T120000.backup"
            backup_path.write_bytes(b"backup")

            self.assertEqual(backup_path.resolve(), verify_backup_exists(db_path))

    def test_schema_ready_requires_completed_ingestion_schema(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "cartographer.db"
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE publications (id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE comments (id INTEGER PRIMARY KEY)")
            conn.commit()

            with self.assertRaisesRegex(RuntimeError, SCHEMA_ERROR):
                verify_schema_ready(conn)
            conn.close()

            ready_path = Path(td) / "ready-cartographer.db"
            migrated = connect_db(ready_path)
            ensure_schema(migrated)
            verify_schema_ready(migrated)
            migration_row = migrated.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?",
                (str(SCHEMA_VERSION),),
            ).fetchone()
            lock_row = migrated.execute(
                "SELECT is_locked, locked_at, owner_pid FROM backfill_lock WHERE id = 1"
            ).fetchone()

            self.assertEqual((1,), migration_row)
            self.assertEqual((0, None, None), lock_row)
            migrated.close()

    def test_lock_acquisition_is_exclusive_and_releasable(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "cartographer.db"
            conn = connect_db(db_path)
            ensure_schema(conn)

            acquire_backfill_lock(conn)
            row = conn.execute(
                "SELECT is_locked, owner_pid FROM backfill_lock WHERE id = 1"
            ).fetchone()
            self.assertEqual((1, str(os.getpid())), row)

            second = connect_db(db_path)
            with self.assertRaisesRegex(RuntimeError, LOCK_ERROR):
                acquire_backfill_lock(second)
            second.close()

            release_backfill_lock(conn)
            row = conn.execute(
                "SELECT is_locked, locked_at, owner_pid FROM backfill_lock WHERE id = 1"
            ).fetchone()
            self.assertEqual((0, None, None), row)
            conn.close()

    def test_lock_survives_abrupt_process_exit_without_release(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "cartographer.db"
            conn = connect_db(db_path)
            ensure_schema(conn)
            conn.close()

            repo_root = Path(__file__).resolve().parents[1]
            code = """
import os
import sys
from scripts.db_runtime import connect_db
from scripts.utils.preflight import acquire_backfill_lock

conn = connect_db(sys.argv[1])
acquire_backfill_lock(conn)
os._exit(9)
"""
            env = os.environ.copy()
            env["PYTHONPATH"] = str(repo_root)
            completed = subprocess.run(
                [sys.executable, "-c", code, str(db_path)],
                env=env,
                check=False,
            )
            self.assertEqual(9, completed.returncode)

            post = connect_db(db_path)
            row = post.execute(
                "SELECT is_locked, locked_at, owner_pid FROM backfill_lock WHERE id = 1"
            ).fetchone()
            self.assertEqual(1, row[0])
            self.assertIsNotNone(row[1])
            self.assertIsNotNone(row[2])
            post.close()

    def test_connect_db_applies_sqlite_runtime_safety_pragmas(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "cartographer.db"
            conn = connect_db(db_path)
            journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            busy_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
            synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]

            self.assertEqual("wal", journal_mode.lower())
            self.assertEqual(5000, busy_timeout)
            self.assertEqual(1, synchronous)
            conn.close()


if __name__ == "__main__":
    unittest.main()
