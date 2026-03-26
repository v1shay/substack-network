#!/usr/bin/env python3
"""Comment enrichment pipeline used standalone and from crawl integration."""

from __future__ import annotations

import argparse
import itertools
import logging
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any

_CODE_ROOT = Path(__file__).resolve().parents[2]
if str(_CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(_CODE_ROOT))

from scripts.db_runtime import connect_db, ensure_schema

if __package__ in (None, ""):
    from scripts.comments.comment_api import fetch_archive, fetch_post_comments
    from scripts.comments.db_helpers import (
        insert_comment_if_not_exists,
        insert_post_if_not_exists,
        insert_user_if_not_exists,
        resolve_comment_parent_links,
    )
    from scripts.comments.parsers import extract_comments_from_response, extract_posts_from_archive
    from scripts.comments.user_classifier import classify_users
else:
    from .comment_api import fetch_archive, fetch_post_comments
    from .db_helpers import (
        insert_comment_if_not_exists,
        insert_post_if_not_exists,
        insert_user_if_not_exists,
        resolve_comment_parent_links,
    )
    from .parsers import extract_comments_from_response, extract_posts_from_archive
    from .user_classifier import classify_users

LOG = logging.getLogger(__name__)


def discover_posts(
    publication_url: str,
    limit: int = 20,
    *,
    timeout: float = 15.0,
    retries: int = 3,
    session: Any | None = None,
) -> list[dict[str, Any]]:
    """Fetch and normalize posts for one publication."""
    archive_payload = fetch_archive(
        publication_url,
        timeout=timeout,
        retries=retries,
        session=session,
    )
    posts = extract_posts_from_archive(archive_payload)
    if limit >= 0:
        posts = posts[:limit]
    LOG.info("[comments] parsing success: posts=%s publication=%s", len(posts), publication_url)
    return posts


def fetch_comments_for_post(
    publication_url: str,
    post_id: str | int,
    *,
    timeout: float = 15.0,
    retries: int = 3,
    session: Any | None = None,
) -> list[dict[str, Any]]:
    """Fetch and normalize comments for one post."""
    comments_payload = fetch_post_comments(
        publication_url,
        post_id,
        timeout=timeout,
        retries=retries,
        session=session,
    )
    comments = extract_comments_from_response(comments_payload)
    LOG.info("[comments] parsing success: comments=%s post_id=%s", len(comments), post_id)
    return comments


def process_comments(
    publication_url: str,
    *,
    conn: sqlite3.Connection | None = None,
    post_limit: int = 20,
    timeout: float = 15.0,
    retries: int = 3,
    session: Any | None = None,
    classify_commenters: bool = False,
    classify_max_users: int = 75,
    classify_workers: int = 4,
    classification_timeout: float = 10.0,
    classification_retries: int = 2,
    classification_session: Any | None = None,
) -> dict[str, int]:
    """Run end-to-end comment enrichment for one publication."""
    own_conn = conn is None
    if own_conn:
        root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
        conn = connect_db(root / "cartographer.db")

    assert conn is not None
    ensure_schema(conn)
    stats = {
        "posts_seen": 0,
        "posts_created": 0,
        "posts_updated": 0,
        "users_seen": 0,
        "users_created": 0,
        "users_updated": 0,
        "comments_fetched": 0,
        "comments_unique": 0,
        "comments_created": 0,
        "comments_updated": 0,
        "classified_users": 0,
        "classified_owners": 0,
    }
    classified_user_ids: list[int] = []
    post_actions: dict[int, str] = {}
    user_actions: dict[int, str] = {}
    savepoint_name = f"comment_pipeline_{next(_SAVEPOINT_COUNTER)}"
    conn.execute(f"SAVEPOINT {savepoint_name}")

    try:
        posts = discover_posts(
            publication_url,
            limit=post_limit,
            timeout=timeout,
            retries=retries,
            session=session,
        )
        stats["posts_seen"] = len(posts)

        for post in posts:
            post_result = insert_post_if_not_exists(conn, post)
            local_post_id = post_result.row_id
            _record_write_action(post_actions, local_post_id, post_result.action)
            if local_post_id is None:
                continue

            external_post_id = post.get("external_post_id")
            if not external_post_id:
                continue

            normalized_comments = fetch_comments_for_post(
                publication_url,
                external_post_id,
                timeout=timeout,
                retries=retries,
                session=session,
            )
            stats["comments_fetched"] += len(normalized_comments)
            unique_comments = _dedupe_comments(normalized_comments)
            stats["comments_unique"] += len(unique_comments)

            inserted_comment_ids: list[int] = []
            for normalized_comment in unique_comments:
                user_result = insert_user_if_not_exists(conn, normalized_comment.get("user"))
                user_id = user_result.row_id
                if user_id is not None:
                    classified_user_ids.append(user_id)
                    _record_write_action(user_actions, user_id, user_result.action)

                comment_result = insert_comment_if_not_exists(
                    conn,
                    normalized_comment,
                    post_id=local_post_id,
                    user_id=user_id,
                    parent_comment_id=None,
                )
                if comment_result.row_id is not None:
                    inserted_comment_ids.append(comment_result.row_id)
                if comment_result.action == "created":
                    stats["comments_created"] += 1
                elif comment_result.action == "updated":
                    stats["comments_updated"] += 1

            resolve_comment_parent_links(conn, comment_ids=inserted_comment_ids)

        stats["posts_created"] = sum(1 for action in post_actions.values() if action == "created")
        stats["posts_updated"] = sum(1 for action in post_actions.values() if action == "updated")
        stats["users_seen"] = len(user_actions)
        stats["users_created"] = sum(1 for action in user_actions.values() if action == "created")
        stats["users_updated"] = sum(1 for action in user_actions.values() if action == "updated")

        if classify_commenters:
            classification_stats = classify_users(
                conn,
                user_ids=classified_user_ids,
                max_users=classify_max_users,
                workers=classify_workers,
                timeout=classification_timeout,
                retries=classification_retries,
                session=classification_session,
            )
            stats["classified_users"] = classification_stats.get("updated_users", 0)
            stats["classified_owners"] = classification_stats.get("owner_users", 0)
            LOG.info(
                "[comments] user classification success: publication=%s attempted_handles=%s resolved_profiles=%s updated_users=%s owner_users=%s",
                publication_url,
                classification_stats.get("attempted_handles", 0),
                classification_stats.get("resolved_profiles", 0),
                classification_stats.get("updated_users", 0),
                classification_stats.get("owner_users", 0),
            )

        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        if own_conn:
            conn.commit()
        LOG.info(
            "[comments] db insert success: publication=%s posts_seen=%s posts_created=%s posts_updated=%s users_seen=%s users_created=%s users_updated=%s comments_fetched=%s comments_unique=%s comments_created=%s comments_updated=%s",
            publication_url,
            stats["posts_seen"],
            stats["posts_created"],
            stats["posts_updated"],
            stats["users_seen"],
            stats["users_created"],
            stats["users_updated"],
            stats["comments_fetched"],
            stats["comments_unique"],
            stats["comments_created"],
            stats["comments_updated"],
        )
        return stats
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def _dedupe_comments(comments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique_comments: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for comment in comments:
        external_comment_id = comment.get("external_comment_id")
        if external_comment_id:
            key = f"id:{external_comment_id}"
        else:
            key = "fallback:" + "|".join(
                str(comment.get(field) or "")
                for field in ("post_external_id", "parent_external_comment_id", "body", "commented_at")
            )
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_comments.append(comment)
    return unique_comments


def _record_write_action(actions: dict[int, str], row_id: int | None, action: str) -> None:
    if row_id is None:
        return
    current = actions.get(row_id, "unchanged")
    precedence = {"created": 2, "updated": 1, "unchanged": 0}
    if precedence.get(action, 0) >= precedence.get(current, 0):
        actions[row_id] = action


_SAVEPOINT_COUNTER = itertools.count()


def _runtime_root() -> Path:
    return Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch archive posts and comments for one publication and persist them to cartographer.db."
    )
    parser.add_argument("publication_url", help="Publication URL or hostname, e.g. paulkrugman.substack.com")
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Path to cartographer.db (default: CARTOGRAPHER_ROOT/cartographer.db or cwd/cartographer.db).",
    )
    parser.add_argument(
        "--post-limit",
        type=int,
        default=20,
        metavar="N",
        help="Maximum number of archive posts to inspect (default: 20).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        metavar="SECS",
        help="Request timeout for archive/comment/profile calls (default: 15.0).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        metavar="N",
        help="Retry attempts for archive/comment requests (default: 3).",
    )
    parser.add_argument(
        "--classify-commenters",
        action="store_true",
        help="Classify commenters via Substack public_profile using the strict admin + hasPosts rule.",
    )
    parser.add_argument(
        "--classification-max-users",
        type=int,
        default=75,
        metavar="N",
        help="Maximum distinct commenter handles to classify (default: 75).",
    )
    parser.add_argument(
        "--classification-workers",
        type=int,
        default=4,
        metavar="N",
        help="Worker threads for profile lookups when classification is enabled (default: 4).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    conn: sqlite3.Connection | None = None
    try:
        if args.db is not None:
            conn = connect_db(Path(args.db).expanduser().resolve())

        stats = process_comments(
            args.publication_url,
            conn=conn,
            post_limit=args.post_limit,
            timeout=args.timeout,
            retries=args.retries,
            classify_commenters=args.classify_commenters,
            classify_max_users=args.classification_max_users,
            classify_workers=args.classification_workers,
        )
    except Exception as exc:
        print(f"[comments][error] {exc}", file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            conn.close()

    db_path = Path(args.db).expanduser().resolve() if args.db is not None else _runtime_root() / "cartographer.db"
    summary_parts = [
        "Comments pipeline complete:",
        f"db={db_path}",
        f"publication={args.publication_url}",
        f"posts_seen={stats.get('posts_seen', 0)}",
        f"posts_created={stats.get('posts_created', 0)}",
        f"users_seen={stats.get('users_seen', 0)}",
        f"comments_unique={stats.get('comments_unique', 0)}",
        f"comments_created={stats.get('comments_created', 0)}",
        f"classified_users={stats.get('classified_users', 0)}",
        f"classified_owners={stats.get('classified_owners', 0)}",
    ]
    print(" ".join(summary_parts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
