#!/usr/bin/env python3
"""Targeted DB repair operations for release-gate anomalies."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3
import sys

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.comments.db_helpers import resolve_comment_parent_links
from scripts.db_runtime import connect_db, ensure_schema, schema_is_current


def _repo_root() -> Path:
    return Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _changed_rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    before = conn.total_changes
    conn.execute(sql, params)
    return conn.total_changes - before


def repair_db(conn: sqlite3.Connection) -> dict[str, int]:
    """Apply deterministic repairs that correspond to release-gate anomaly counters."""
    fixes: dict[str, int] = {}
    stamp = _now_iso()
    fixes["schema_migrations_applied"] = 0 if schema_is_current(conn) else 1
    ensure_schema(conn)

    fixes["inserted_missing_publications_from_recommendation_sources"] = _changed_rows(
        conn,
        """
        INSERT OR IGNORE INTO publications (substack_id, name, domain, description, first_seen)
        SELECT
            NULL,
            NULL,
            r.source_domain,
            '[placeholder inserted by scripts/comments/db_repair.py]',
            ?
          FROM recommendations r
          LEFT JOIN publications p
            ON p.domain = r.source_domain
         WHERE p.domain IS NULL
           AND r.source_domain IS NOT NULL
           AND TRIM(r.source_domain) <> ''
        """,
        (stamp,),
    )

    fixes["inserted_missing_publications_from_crawled_queue"] = _changed_rows(
        conn,
        """
        INSERT OR IGNORE INTO publications (substack_id, name, domain, description, first_seen)
        SELECT
            NULL,
            NULL,
            q.domain,
            '[placeholder inserted by scripts/comments/db_repair.py]',
            ?
          FROM queue q
          LEFT JOIN publications p
            ON p.domain = q.domain
         WHERE q.status = 'crawled'
           AND p.domain IS NULL
           AND q.domain IS NOT NULL
           AND TRIM(q.domain) <> ''
        """,
        (stamp,),
    )

    fixes["queue_failed_promoted_to_crawled_with_existing_publication"] = _changed_rows(
        conn,
        """
        UPDATE queue
           SET status = 'crawled'
         WHERE status = 'failed'
           AND domain IN (SELECT domain FROM publications)
        """,
    )

    fixes["comments_parent_external_backfilled_from_parent_id"] = _changed_rows(
        conn,
        """
        UPDATE comments
           SET parent_external_comment_id = (
               SELECT parent.external_comment_id
                 FROM comments parent
                WHERE parent.id = comments.parent_comment_id
           ),
               last_seen = ?
         WHERE parent_comment_id IS NOT NULL
           AND (parent_external_comment_id IS NULL OR TRIM(parent_external_comment_id) = '')
           AND EXISTS (
               SELECT 1
                 FROM comments parent
                WHERE parent.id = comments.parent_comment_id
                  AND parent.external_comment_id IS NOT NULL
                  AND TRIM(parent.external_comment_id) <> ''
           )
        """,
        (stamp,),
    )

    fixes["comments_parent_comment_links_resolved_from_external_id"] = resolve_comment_parent_links(conn)

    conn.commit()
    return fixes


def run_repair(db_path: Path | str) -> dict[str, int]:
    conn = connect_db(str(db_path))
    try:
        return repair_db(conn)
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply deterministic DB repairs for comment-crawling release gates."
    )
    parser.add_argument(
        "--db",
        default=str(_repo_root() / "cartographer.db"),
        help="Path to SQLite database (default: repo-root cartographer.db).",
    )
    args = parser.parse_args()

    fixes = run_repair(Path(args.db))
    print("DB repair summary:")
    for key, value in fixes.items():
        print(f"  {key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
