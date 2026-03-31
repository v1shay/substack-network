#!/usr/bin/env python3
"""Run live comment ingestion and assert that SQLite contains usable graph data."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any

_CODE_ROOT = Path(__file__).resolve().parents[2]
if str(_CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(_CODE_ROOT))

from scripts.comments.comment_pipeline import process_comments
from scripts.comments.db_audit import audit_db
from scripts.db_runtime import connect_db, ensure_schema


def _runtime_root() -> Path:
    return Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()


def _table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    cursor = conn.cursor()
    counts: dict[str, int] = {}
    for table_name in ("users", "posts", "comments"):
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        counts[table_name] = int(cursor.fetchone()[0])
    return counts


def _query_scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    cursor = conn.cursor()
    cursor.execute(sql, params)
    return int(cursor.fetchone()[0])


def _sample_rows(conn: sqlite3.Connection) -> dict[str, list[tuple[Any, ...]]]:
    cursor = conn.cursor()
    samples: dict[str, list[tuple[Any, ...]]] = {}
    cursor.execute(
        """
        SELECT external_user_id, handle, publication_substack_id, publication_role, is_publication_owner
          FROM users
         ORDER BY id DESC
         LIMIT 5
        """
    )
    samples["users"] = cursor.fetchall()

    cursor.execute(
        """
        SELECT external_post_id, publication_substack_id, title, url, published_at
          FROM posts
         ORDER BY id DESC
         LIMIT 5
        """
    )
    samples["posts"] = cursor.fetchall()

    cursor.execute(
        """
        SELECT external_comment_id, post_id, user_id, parent_comment_id, substr(body, 1, 120), commented_at
          FROM comments
         ORDER BY id DESC
         LIMIT 5
        """
    )
    samples["comments"] = cursor.fetchall()
    return samples


def _validate_db(conn: sqlite3.Connection) -> dict[str, int]:
    checks = {
        "comments_with_body": _query_scalar(
            conn,
            """
            SELECT COUNT(*)
              FROM comments
             WHERE body IS NOT NULL
               AND TRIM(body) <> ''
            """,
        ),
        "reply_links": _query_scalar(
            conn,
            """
            SELECT COUNT(*)
              FROM comments
             WHERE parent_comment_id IS NOT NULL
            """,
        ),
        "posts_joined_to_publications": _query_scalar(
            conn,
            """
            SELECT COUNT(*)
              FROM posts p
              JOIN publications pub
                ON pub.substack_id = p.publication_substack_id
            """,
        ),
        "duplicate_users_by_external_id": _query_scalar(
            conn,
            """
            SELECT COUNT(*)
              FROM (
                    SELECT external_user_id
                      FROM users
                     WHERE external_user_id IS NOT NULL
                       AND TRIM(external_user_id) <> ''
                     GROUP BY external_user_id
                    HAVING COUNT(*) > 1
                   )
            """,
        ),
    }

    counts = _table_counts(conn)
    failures: list[str] = []
    if counts["users"] <= 0:
        failures.append("users table is empty")
    if counts["posts"] <= 0:
        failures.append("posts table is empty")
    if counts["comments"] <= 0:
        failures.append("comments table is empty")
    if checks["comments_with_body"] <= 0:
        failures.append("no comments with non-empty body")
    if checks["reply_links"] <= 0:
        failures.append("no linked replies (parent_comment_id)")
    if checks["posts_joined_to_publications"] != counts["posts"]:
        failures.append("one or more posts do not join to publications.substack_id")
    if checks["duplicate_users_by_external_id"] != 0:
        failures.append("duplicate users detected by external_user_id")

    if failures:
        raise RuntimeError("; ".join(failures))

    return checks


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run live Substack comment ingestion and verify SQLite state."
    )
    parser.add_argument(
        "publication_url",
        help="Publication URL or hostname with active comments, e.g. paulkrugman.substack.com",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Path to cartographer.db (default: CARTOGRAPHER_ROOT/cartographer.db or cwd/cartographer.db).",
    )
    parser.add_argument(
        "--post-limit",
        type=int,
        default=1,
        metavar="N",
        help="Maximum archive posts to inspect during validation (default: 1).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        metavar="SECS",
        help="Request timeout for archive/comment calls (default: 20.0).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        metavar="N",
        help="Retry attempts for archive/comment calls (default: 2).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    db_path = (
        Path(args.db).expanduser().resolve()
        if args.db is not None
        else _runtime_root() / "cartographer.db"
    )

    conn = connect_db(db_path)
    try:
        ensure_schema(conn)
        before = _table_counts(conn)
        stats = process_comments(
            args.publication_url,
            conn=conn,
            post_limit=args.post_limit,
            timeout=args.timeout,
            retries=args.retries,
        )
        if stats.get("posts_seen", 0) <= 0:
            raise RuntimeError("live ingestion returned zero posts")
        if stats.get("comments_unique", 0) <= 0:
            raise RuntimeError("live ingestion returned zero comments")

        after = _table_counts(conn)
        checks = _validate_db(conn)
        audit_counts = audit_db(db_path)
        anomalies = {name: value for name, value in audit_counts.items() if value}
        if anomalies:
            raise RuntimeError(f"db audit reported anomalies: {anomalies}")

        samples = _sample_rows(conn)
    finally:
        conn.close()

    print("Live ingestion validation passed:")
    print(f"  db={db_path}")
    print(f"  publication={args.publication_url}")
    print(
        "  pipeline_stats="
        f"posts_seen={stats.get('posts_seen', 0)} "
        f"posts_created={stats.get('posts_created', 0)} "
        f"posts_updated={stats.get('posts_updated', 0)} "
        f"users_seen={stats.get('users_seen', 0)} "
        f"users_created={stats.get('users_created', 0)} "
        f"users_updated={stats.get('users_updated', 0)} "
        f"comments_fetched={stats.get('comments_fetched', 0)} "
        f"comments_unique={stats.get('comments_unique', 0)} "
        f"comments_created={stats.get('comments_created', 0)} "
        f"comments_updated={stats.get('comments_updated', 0)}"
    )
    print(
        "  counts_before="
        f"users={before['users']} posts={before['posts']} comments={before['comments']}"
    )
    print(
        "  counts_after="
        f"users={after['users']} posts={after['posts']} comments={after['comments']}"
    )
    print(
        "  checks="
        f"comments_with_body={checks['comments_with_body']} "
        f"reply_links={checks['reply_links']} "
        f"posts_joined_to_publications={checks['posts_joined_to_publications']} "
        f"duplicate_users_by_external_id={checks['duplicate_users_by_external_id']}"
    )
    print(f"  sample_users={samples['users']}")
    print(f"  sample_posts={samples['posts']}")
    print(f"  sample_comments={samples['comments']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
