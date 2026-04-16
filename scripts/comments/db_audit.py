#!/usr/bin/env python3
"""Integrity audit for cartographer DB release gates."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.db_runtime import SCHEMA_VERSION, connect_db, ensure_schema, expected_schema_columns


ANOMALY_SPECS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    (
        "publications_null_domain",
        ("publications",),
        """
        SELECT COUNT(*)
          FROM publications
         WHERE domain IS NULL OR TRIM(domain) = ''
        """,
    ),
    (
        "publications_duplicate_domain",
        ("publications",),
        """
        SELECT COUNT(*)
          FROM (
                SELECT domain, COUNT(*) AS c
                  FROM publications
                 GROUP BY domain
                HAVING c > 1
               )
        """,
    ),
    (
        "recommendations_null_endpoints",
        ("recommendations",),
        """
        SELECT COUNT(*)
          FROM recommendations
         WHERE source_domain IS NULL OR target_domain IS NULL
        """,
    ),
    (
        "recommendations_duplicate_edges",
        ("recommendations",),
        """
        SELECT COUNT(*)
          FROM (
                SELECT source_domain, target_domain, COUNT(*) AS c
                  FROM recommendations
                 GROUP BY source_domain, target_domain
                HAVING c > 1
               )
        """,
    ),
    (
        "queue_null_domain",
        ("queue",),
        """
        SELECT COUNT(*)
          FROM queue
         WHERE domain IS NULL OR TRIM(domain) = ''
        """,
    ),
    (
        "queue_invalid_status",
        ("queue",),
        """
        SELECT COUNT(*)
          FROM queue
         WHERE status NOT IN ('pending', 'crawled', 'failed') OR status IS NULL
        """,
    ),
    (
        "queue_duplicate_domain",
        ("queue",),
        """
        SELECT COUNT(*)
          FROM (
                SELECT domain, COUNT(*) AS c
                  FROM queue
                 GROUP BY domain
                HAVING c > 1
               )
        """,
    ),
    (
        "recommendations_orphan_source",
        ("recommendations", "publications"),
        """
        SELECT COUNT(*)
          FROM recommendations r
          LEFT JOIN publications p
            ON p.domain = r.source_domain
         WHERE p.domain IS NULL
        """,
    ),
    (
        "queue_crawled_without_publication",
        ("queue", "publications"),
        """
        SELECT COUNT(*)
          FROM queue q
          LEFT JOIN publications p
            ON p.domain = q.domain
         WHERE q.status = 'crawled'
           AND p.domain IS NULL
        """,
    ),
    (
        "queue_failed_with_publication",
        ("queue", "publications"),
        """
        SELECT COUNT(*)
          FROM queue q
          JOIN publications p
            ON p.domain = q.domain
         WHERE q.status = 'failed'
        """,
    ),
    (
        "users_duplicate_external_user_id",
        ("users",),
        """
        SELECT COUNT(*)
          FROM (
                SELECT external_user_id, COUNT(*) AS c
                  FROM users
                 WHERE external_user_id IS NOT NULL
                   AND TRIM(external_user_id) <> ''
                 GROUP BY external_user_id
                HAVING c > 1
               )
        """,
    ),
    (
        "posts_duplicate_external_post_id",
        ("posts",),
        """
        SELECT COUNT(*)
          FROM (
                SELECT external_post_id, COUNT(*) AS c
                  FROM posts
                 WHERE external_post_id IS NOT NULL
                   AND TRIM(external_post_id) <> ''
                 GROUP BY external_post_id
                HAVING c > 1
               )
        """,
    ),
    (
        "comments_duplicate_external_comment_id",
        ("comments",),
        """
        SELECT COUNT(*)
          FROM (
                SELECT external_comment_id, COUNT(*) AS c
                  FROM comments
                 WHERE external_comment_id IS NOT NULL
                   AND TRIM(external_comment_id) <> ''
                 GROUP BY external_comment_id
                HAVING c > 1
               )
        """,
    ),
    (
        "comments_orphan_post_id",
        ("comments", "posts"),
        """
        SELECT COUNT(*)
          FROM comments c
          LEFT JOIN posts p
            ON p.id = c.post_id
         WHERE c.post_id IS NOT NULL
           AND p.id IS NULL
        """,
    ),
    (
        "comments_orphan_user_id",
        ("comments", "users"),
        """
        SELECT COUNT(*)
          FROM comments c
          LEFT JOIN users u
            ON u.id = c.user_id
         WHERE c.user_id IS NOT NULL
           AND u.id IS NULL
        """,
    ),
    (
        "comments_orphan_parent_comment_id",
        ("comments",),
        """
        SELECT COUNT(*)
          FROM comments c
          LEFT JOIN comments parent
            ON parent.id = c.parent_comment_id
         WHERE c.parent_comment_id IS NOT NULL
           AND parent.id IS NULL
        """,
    ),
    (
        "comments_broken_parent_external_comment_id",
        ("comments",),
        """
        SELECT COUNT(*)
          FROM comments c
          LEFT JOIN comments parent
            ON parent.external_comment_id = c.parent_external_comment_id
         WHERE c.parent_external_comment_id IS NOT NULL
           AND TRIM(c.parent_external_comment_id) <> ''
           AND parent.id IS NULL
        """,
    ),
    (
        "users_invalid_first_seen",
        ("users",),
        """
        SELECT COUNT(*)
          FROM users
         WHERE first_seen IS NULL
            OR TRIM(first_seen) = ''
            OR first_seen NOT GLOB '????-??-??*'
        """,
    ),
    (
        "users_invalid_last_seen",
        ("users",),
        """
        SELECT COUNT(*)
          FROM users
         WHERE last_seen IS NULL
            OR TRIM(last_seen) = ''
            OR last_seen NOT GLOB '????-??-??*'
        """,
    ),
    (
        "posts_invalid_first_seen",
        ("posts",),
        """
        SELECT COUNT(*)
          FROM posts
         WHERE first_seen IS NULL
            OR TRIM(first_seen) = ''
            OR first_seen NOT GLOB '????-??-??*'
        """,
    ),
    (
        "posts_invalid_last_seen",
        ("posts",),
        """
        SELECT COUNT(*)
          FROM posts
         WHERE last_seen IS NULL
            OR TRIM(last_seen) = ''
            OR last_seen NOT GLOB '????-??-??*'
        """,
    ),
    (
        "posts_invalid_published_at",
        ("posts",),
        """
        SELECT COUNT(*)
          FROM posts
         WHERE published_at IS NOT NULL
           AND TRIM(published_at) <> ''
           AND published_at NOT GLOB '????-??-??*'
        """,
    ),
    (
        "comments_invalid_first_seen",
        ("comments",),
        """
        SELECT COUNT(*)
          FROM comments
         WHERE first_seen IS NULL
            OR TRIM(first_seen) = ''
            OR first_seen NOT GLOB '????-??-??*'
        """,
    ),
    (
        "comments_invalid_last_seen",
        ("comments",),
        """
        SELECT COUNT(*)
          FROM comments
         WHERE last_seen IS NULL
            OR TRIM(last_seen) = ''
            OR last_seen NOT GLOB '????-??-??*'
        """,
    ),
    (
        "comments_invalid_commented_at",
        ("comments",),
        """
        SELECT COUNT(*)
          FROM comments
         WHERE commented_at IS NOT NULL
           AND TRIM(commented_at) <> ''
           AND commented_at NOT GLOB '????-??-??*'
        """,
    ),
)

ANOMALY_QUERIES: dict[str, str] = {name: query for name, _tables, query in ANOMALY_SPECS}
ANOMALY_ORDER: tuple[str, ...] = (
    "schema_version_missing",
    "schema_version_mismatch",
    "schema_drift_tables",
    *tuple(name for name, _tables, _query in ANOMALY_SPECS),
)


def _repo_root() -> Path:
    return Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()


def ordered_anomaly_names() -> tuple[str, ...]:
    return ANOMALY_ORDER


def compute_anomaly_counts(conn: sqlite3.Connection) -> dict[str, int]:
    existing_tables = _existing_tables(conn)
    counts = _schema_anomalies(conn, existing_tables)
    cur = conn.cursor()
    for name, required_tables, query in ANOMALY_SPECS:
        if not set(required_tables).issubset(existing_tables):
            counts[name] = 0
            continue
        cur.execute(query)
        counts[name] = int(cur.fetchone()[0])
    return counts


def audit_db(db_path: Path | str, *, read_only: bool = False) -> dict[str, int]:
    if read_only:
        conn = sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)
        conn.execute("PRAGMA foreign_keys = ON")
    else:
        conn = connect_db(str(db_path))
    try:
        if not read_only:
            ensure_schema(conn)
        return compute_anomaly_counts(conn)
    finally:
        conn.close()


def summarize(counts: dict[str, int]) -> dict[str, Any]:
    total = sum(counts.values())
    return {
        "all_zero": total == 0,
        "total_anomalies": total,
        "counters": counts,
    }


def _schema_anomalies(conn: sqlite3.Connection, existing_tables: set[str]) -> dict[str, int]:
    counts = {
        "schema_version_missing": 0,
        "schema_version_mismatch": 0,
        "schema_drift_tables": 0,
    }
    expected_columns = expected_schema_columns()
    expected_tables = set(expected_columns)

    if "schema_version" not in existing_tables:
        counts["schema_version_missing"] = 1
    else:
        row = conn.execute(
            "SELECT version FROM schema_version WHERE singleton = 1"
        ).fetchone()
        if row is None:
            counts["schema_version_missing"] = 1
        elif int(row[0]) != SCHEMA_VERSION:
            counts["schema_version_mismatch"] = 1

    drift = 0
    for table_name in expected_tables:
        if table_name not in existing_tables:
            drift += 1
            continue
        if tuple(_column_names(conn, table_name)) != expected_columns[table_name]:
            drift += 1
    counts["schema_drift_tables"] = drift
    return counts


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {str(row[0]) for row in cur.fetchall()}


def _column_names(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [str(row[1]) for row in cur.fetchall()]


def _print_human(summary: dict[str, Any]) -> None:
    counters = summary["counters"]
    print("DB integrity audit:")
    for name in ordered_anomaly_names():
        print(f"  {name}: {counters.get(name, 0)}")
    print(f"all_zero: {summary['all_zero']}")
    print(f"total_anomalies: {summary['total_anomalies']}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit cartographer DB integrity for comment-crawling release gates."
    )
    parser.add_argument(
        "--db",
        default=str(_repo_root() / "cartographer.db"),
        help="Path to SQLite database (default: repo-root cartographer.db).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON output.",
    )
    parser.add_argument(
        "--read-only",
        action="store_true",
        help="Open the SQLite file in read-only mode and do not run schema migrations.",
    )
    parser.add_argument(
        "--fail-on-anomaly",
        action="store_true",
        help="Exit 1 if any anomaly counter is non-zero.",
    )
    args = parser.parse_args()

    summary = summarize(audit_db(Path(args.db), read_only=args.read_only))
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_human(summary)

    if args.fail_on_anomaly and not summary["all_zero"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
