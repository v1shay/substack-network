#!/usr/bin/env python3
"""Safety preflight checks for production comment ingestion."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3

from scripts.db_runtime import SCHEMA_VERSION

BACKUP_ERROR = "Refusing to run ingestion without verified backup"
SCHEMA_ERROR = "Schema not ready for ingestion"
LOCK_ERROR = "Another backfill process is already running"

REQUIRED_INGESTION_TABLES = (
    "schema_version",
    "schema_migrations",
    "publications",
    "posts",
    "comments",
    "comment_ingestion_runs",
    "comment_publication_status",
    "backfill_lock",
)


def verify_backup_exists(db_path: str | Path) -> Path:
    """Verify that a readable, non-empty backup exists for db_path."""
    resolved = Path(db_path).expanduser().resolve()
    for candidate in _backup_candidates(resolved):
        if _is_readable_nonempty_file(candidate):
            return candidate
    raise RuntimeError(BACKUP_ERROR)


def verify_schema_ready(conn: sqlite3.Connection) -> None:
    """Verify the DB schema has completed the ingestion-safe migration."""
    tables = _existing_tables(conn)
    if any(table_name not in tables for table_name in REQUIRED_INGESTION_TABLES):
        raise RuntimeError(SCHEMA_ERROR)

    version_row = conn.execute(
        "SELECT version FROM schema_version WHERE singleton = 1"
    ).fetchone()
    if version_row is None or int(version_row[0]) != SCHEMA_VERSION:
        raise RuntimeError(SCHEMA_ERROR)

    migration_row = conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = ?",
        (str(SCHEMA_VERSION),),
    ).fetchone()
    if migration_row is None:
        raise RuntimeError(SCHEMA_ERROR)

    lock_row = conn.execute(
        "SELECT is_locked FROM backfill_lock WHERE id = 1"
    ).fetchone()
    if lock_row is None or int(lock_row[0]) not in (0, 1):
        raise RuntimeError(SCHEMA_ERROR)


def acquire_backfill_lock(conn: sqlite3.Connection) -> None:
    """Atomically acquire the global backfill lock for this process."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT is_locked FROM backfill_lock WHERE id = 1").fetchone()
        if row is None:
            raise RuntimeError(SCHEMA_ERROR)
        if int(row[0]) == 1:
            raise RuntimeError(LOCK_ERROR)
        conn.execute(
            """
            UPDATE backfill_lock
               SET is_locked = 1,
                   locked_at = ?,
                   owner_pid = ?
             WHERE id = 1
            """,
            (_now_iso(), str(os.getpid())),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def release_backfill_lock(conn: sqlite3.Connection) -> None:
    """Release the global backfill lock."""
    conn.execute(
        """
        UPDATE backfill_lock
           SET is_locked = 0,
               locked_at = NULL,
               owner_pid = NULL
         WHERE id = 1
        """
    )
    conn.commit()


def _backup_candidates(db_path: Path) -> list[Path]:
    parent = db_path.parent
    name = db_path.name
    candidates = [parent / f"{name}.backup"]
    patterns = (
        f"{name}.backup-*",
        f"{name}.backup.*",
        f"{name}.*.backup",
        f"{db_path.stem}-*.backup",
    )
    seen = {candidates[0]}
    for pattern in patterns:
        for candidate in sorted(parent.glob(pattern)):
            if candidate not in seen:
                candidates.append(candidate)
                seen.add(candidate)
    return candidates


def _is_readable_nonempty_file(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        if path.stat().st_size <= 0:
            return False
        with path.open("rb") as handle:
            handle.read(1)
    except OSError:
        return False
    return True


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]) for row in rows}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
