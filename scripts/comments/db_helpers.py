#!/usr/bin/env python3
"""Idempotent DB insert/update helpers for comment enrichment tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import sqlite3
from typing import Any

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class WriteResult:
    row_id: int | None
    action: str


def insert_user_if_not_exists(conn: sqlite3.Connection, user: dict[str, Any] | None) -> WriteResult:
    """Insert/update a user and return row id plus semantic action."""
    if not user:
        return WriteResult(None, "unchanged")

    external_user_id = _string_or_none(user.get("external_user_id"))
    name = _string_or_none(user.get("name"))
    handle = _string_or_none(user.get("handle"))
    profile_url = _string_or_none(user.get("profile_url"))
    publication_substack_id = _string_or_none(user.get("publication_substack_id"))
    publication_role = _string_or_none(user.get("publication_role"))
    is_publication_owner = int(bool(user.get("is_publication_owner")))
    now = _now_iso()
    cur = conn.cursor()

    existing = _find_optional_row(
        cur,
        [
            ("SELECT * FROM users WHERE external_user_id = ?", (external_user_id,)) if external_user_id is not None else None,
            ("SELECT * FROM users WHERE profile_url = ? LIMIT 1", (profile_url,)) if profile_url else None,
            ("SELECT * FROM users WHERE handle = ? LIMIT 1", (handle,)) if handle else None,
        ],
    )
    if existing is None:
        cur.execute(
            """
            INSERT INTO users (
                external_user_id, name, handle, profile_url,
                publication_substack_id, publication_role, is_publication_owner, first_seen, last_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                external_user_id,
                name,
                handle,
                profile_url,
                publication_substack_id,
                publication_role,
                is_publication_owner,
                now,
                now,
            ),
        )
        row_id = int(cur.lastrowid)
        LOG.info("[comments] db insert success: user action=created local_id=%s", row_id)
        return WriteResult(row_id, "created")

    row_id = int(existing["id"])
    changed = False
    if _prefer_new_value(existing["name"], name) != existing["name"]:
        changed = True
    if _prefer_new_value(existing["handle"], handle) != existing["handle"]:
        changed = True
    if _prefer_new_value(existing["profile_url"], profile_url) != existing["profile_url"]:
        changed = True
    if _prefer_new_value(existing["publication_substack_id"], publication_substack_id) != existing["publication_substack_id"]:
        changed = True
    if _prefer_new_value(existing["publication_role"], publication_role) != existing["publication_role"]:
        changed = True
    if is_publication_owner == 1 and int(existing["is_publication_owner"] or 0) != 1:
        changed = True

    cur.execute(
        """
        UPDATE users
           SET external_user_id = COALESCE(external_user_id, ?),
               name = COALESCE(?, name),
               handle = COALESCE(?, handle),
               profile_url = COALESCE(?, profile_url),
               publication_substack_id = COALESCE(?, publication_substack_id),
               publication_role = COALESCE(?, publication_role),
               is_publication_owner = CASE WHEN ? = 1 THEN 1 ELSE is_publication_owner END,
               last_seen = ?
         WHERE id = ?
        """,
        (
            external_user_id,
            name,
            handle,
            profile_url,
            publication_substack_id,
            publication_role,
            is_publication_owner,
            now,
            row_id,
        ),
    )
    action = "updated" if changed else "unchanged"
    LOG.info("[comments] db insert success: user action=%s local_id=%s", action, row_id)
    return WriteResult(row_id, action)


def insert_post_if_not_exists(conn: sqlite3.Connection, post: dict[str, Any] | None) -> WriteResult:
    """Insert/update a post and return row id plus semantic action."""
    if not post:
        return WriteResult(None, "unchanged")

    external_post_id = _string_or_none(post.get("external_post_id"))
    publication_substack_id = _string_or_none(post.get("publication_substack_id"))
    title = _string_or_none(post.get("title"))
    url = _string_or_none(post.get("url"))
    published_at = _string_or_none(post.get("published_at"))
    now = _now_iso()
    cur = conn.cursor()

    existing = _find_optional_row(
        cur,
        [
            ("SELECT * FROM posts WHERE external_post_id = ?", (external_post_id,)) if external_post_id is not None else None,
            ("SELECT * FROM posts WHERE url = ? LIMIT 1", (url,)) if url else None,
        ],
    )

    if existing is None:
        cur.execute(
            """
            INSERT INTO posts (
                external_post_id, publication_substack_id, title, url, published_at, first_seen, last_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (external_post_id, publication_substack_id, title, url, published_at, now, now),
        )
        row_id = int(cur.lastrowid)
        LOG.info("[comments] db insert success: post action=created local_id=%s", row_id)
        return WriteResult(row_id, "created")

    row_id = int(existing["id"])
    changed = False
    if _prefer_new_value(existing["publication_substack_id"], publication_substack_id) != existing["publication_substack_id"]:
        changed = True
    if _prefer_new_value(existing["title"], title) != existing["title"]:
        changed = True
    if _prefer_new_value(existing["url"], url) != existing["url"]:
        changed = True
    if _prefer_new_value(existing["published_at"], published_at) != existing["published_at"]:
        changed = True

    cur.execute(
        """
        UPDATE posts
           SET external_post_id = COALESCE(external_post_id, ?),
               publication_substack_id = COALESCE(?, publication_substack_id),
               title = COALESCE(?, title),
               url = COALESCE(?, url),
               published_at = COALESCE(?, published_at),
               last_seen = ?
         WHERE id = ?
        """,
        (external_post_id, publication_substack_id, title, url, published_at, now, row_id),
    )
    action = "updated" if changed else "unchanged"
    LOG.info("[comments] db insert success: post action=%s local_id=%s", action, row_id)
    return WriteResult(row_id, action)


def insert_comment_if_not_exists(
    conn: sqlite3.Connection,
    comment: dict[str, Any] | None,
    *,
    post_id: int | None = None,
    user_id: int | None = None,
    parent_comment_id: int | None = None,
) -> WriteResult:
    """Insert/update a comment and return row id plus semantic action."""
    if not comment:
        return WriteResult(None, "unchanged")

    external_comment_id = _string_or_none(comment.get("external_comment_id"))
    parent_external_comment_id = _string_or_none(comment.get("parent_external_comment_id"))
    body = _string_or_none(comment.get("body"))
    commented_at = _string_or_none(comment.get("commented_at"))
    raw_json = _string_or_none(comment.get("raw_json"))
    now = _now_iso()
    cur = conn.cursor()

    existing = _find_optional_row(
        cur,
        [
            (
                "SELECT * FROM comments WHERE external_comment_id = ?",
                (external_comment_id,),
            )
            if external_comment_id is not None
            else None,
            (
                """
                SELECT *
                  FROM comments
                 WHERE post_id IS ?
                   AND user_id IS ?
                   AND parent_external_comment_id IS ?
                   AND body IS ?
                   AND commented_at IS ?
                 LIMIT 1
                """,
                (post_id, user_id, parent_external_comment_id, body, commented_at),
            ),
        ],
    )

    if existing is None:
        cur.execute(
            """
            INSERT INTO comments (
                external_comment_id, post_id, user_id, parent_comment_id,
                parent_external_comment_id, body, commented_at, raw_json, first_seen, last_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                external_comment_id,
                post_id,
                user_id,
                parent_comment_id,
                parent_external_comment_id,
                body,
                commented_at,
                raw_json,
                now,
                now,
            ),
        )
        row_id = int(cur.lastrowid)
        LOG.info("[comments] db insert success: comment action=created local_id=%s", row_id)
        return WriteResult(row_id, "created")

    row_id = int(existing["id"])
    changed = False
    if _prefer_new_value(existing["post_id"], post_id) != existing["post_id"]:
        changed = True
    if _prefer_new_value(existing["user_id"], user_id) != existing["user_id"]:
        changed = True
    if _prefer_new_value(existing["parent_comment_id"], parent_comment_id) != existing["parent_comment_id"]:
        changed = True
    if _prefer_new_value(existing["parent_external_comment_id"], parent_external_comment_id) != existing["parent_external_comment_id"]:
        changed = True
    if _prefer_new_value(existing["body"], body) != existing["body"]:
        changed = True
    if _prefer_new_value(existing["commented_at"], commented_at) != existing["commented_at"]:
        changed = True
    if _prefer_new_value(existing["raw_json"], raw_json) != existing["raw_json"]:
        changed = True

    cur.execute(
        """
        UPDATE comments
           SET external_comment_id = COALESCE(external_comment_id, ?),
               post_id = COALESCE(?, post_id),
               user_id = COALESCE(?, user_id),
               parent_comment_id = COALESCE(?, parent_comment_id),
               parent_external_comment_id = COALESCE(?, parent_external_comment_id),
               body = COALESCE(?, body),
               commented_at = COALESCE(?, commented_at),
               raw_json = COALESCE(?, raw_json),
               last_seen = ?
         WHERE id = ?
        """,
        (
            external_comment_id,
            post_id,
            user_id,
            parent_comment_id,
            parent_external_comment_id,
            body,
            commented_at,
            raw_json,
            now,
            row_id,
        ),
    )
    action = "updated" if changed else "unchanged"
    LOG.info("[comments] db insert success: comment action=%s local_id=%s", action, row_id)
    return WriteResult(row_id, action)


def update_user_classification(
    conn: sqlite3.Connection,
    *,
    user_id: int | None,
    publication_substack_id: Any | None,
    publication_role: str | None,
    is_publication_owner: int | bool,
) -> bool:
    """Update publication linkage/owner flag for an existing user row."""
    if user_id is None:
        return False

    now = _now_iso()
    owner_flag = int(bool(is_publication_owner))
    cur = conn.cursor()
    existing = _fetch_optional_row(cur, "SELECT * FROM users WHERE id = ?", (user_id,))
    if existing is None:
        return False

    publication_substack_id_s = _string_or_none(publication_substack_id)
    publication_role_s = _string_or_none(publication_role)
    changed = False
    if _prefer_new_value(existing["publication_substack_id"], publication_substack_id_s) != existing["publication_substack_id"]:
        changed = True
    if _prefer_new_value(existing["publication_role"], publication_role_s) != existing["publication_role"]:
        changed = True
    if owner_flag == 1 and int(existing["is_publication_owner"] or 0) != 1:
        changed = True

    cur.execute(
        """
        UPDATE users
           SET publication_substack_id = COALESCE(?, publication_substack_id),
               publication_role = COALESCE(?, publication_role),
               is_publication_owner = CASE WHEN ? = 1 THEN 1 ELSE is_publication_owner END,
               last_seen = ?
         WHERE id = ?
        """,
        (publication_substack_id_s, publication_role_s, owner_flag, now, user_id),
    )
    if changed:
        LOG.info(
            "[comments] db insert success: user classification local_id=%s publication_substack_id=%s publication_role=%s is_owner=%s",
            user_id,
            publication_substack_id_s,
            publication_role_s,
            owner_flag,
        )
    return changed


def resolve_comment_parent_links(conn: sqlite3.Connection, *, comment_ids: list[int] | None = None) -> int:
    """Backfill parent_comment_id from parent_external_comment_id after insertion."""
    cur = conn.cursor()
    sql = """
        SELECT id, parent_comment_id, parent_external_comment_id
          FROM comments
         WHERE parent_external_comment_id IS NOT NULL
           AND TRIM(parent_external_comment_id) <> ''
    """
    params: tuple[Any, ...] = ()
    if comment_ids:
        placeholders = ",".join("?" for _ in comment_ids)
        sql += f" AND id IN ({placeholders})"
        params = tuple(comment_ids)
    cur.execute(sql, params)
    rows = cur.fetchall()
    if not rows:
        return 0

    parent_external_ids = sorted({row[2] for row in rows if row[2]})
    if not parent_external_ids:
        return 0
    placeholders = ",".join("?" for _ in parent_external_ids)
    cur.execute(
        f"SELECT external_comment_id, id FROM comments WHERE external_comment_id IN ({placeholders})",
        tuple(parent_external_ids),
    )
    parent_ids = {external_comment_id: int(row_id) for external_comment_id, row_id in cur.fetchall()}

    updates = 0
    for comment_id, parent_comment_id, parent_external_comment_id in rows:
        resolved_parent_id = parent_ids.get(parent_external_comment_id)
        if resolved_parent_id is None or resolved_parent_id == parent_comment_id:
            continue
        cur.execute(
            "UPDATE comments SET parent_comment_id = ?, last_seen = ? WHERE id = ?",
            (resolved_parent_id, _now_iso(), int(comment_id)),
        )
        updates += 1
    return updates


def _fetch_optional_row(
    cur: sqlite3.Cursor,
    sql: str | None,
    params: tuple[Any, ...],
) -> dict[str, Any] | None:
    if sql is None:
        return None
    cur.execute(sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    column_names = [description[0] for description in cur.description or ()]
    return dict(zip(column_names, row))


def _find_optional_row(
    cur: sqlite3.Cursor,
    candidates: list[tuple[str, tuple[Any, ...]] | None],
) -> dict[str, Any] | None:
    for candidate in candidates:
        if candidate is None:
            continue
        sql, params = candidate
        row = _fetch_optional_row(cur, sql, params)
        if row is not None:
            return row
    return None


def _prefer_new_value(existing: Any, candidate: Any) -> Any:
    return candidate if candidate not in (None, "") else existing


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)
