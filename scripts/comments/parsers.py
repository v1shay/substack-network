#!/usr/bin/env python3
"""Normalization/parsing helpers for comment pipeline payloads."""

from __future__ import annotations

import json
from typing import Any


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def extract_posts_from_archive(payload: Any) -> list[dict[str, Any]]:
    """Normalize archive response payload into post records."""
    items: list[Any]
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = []
        for key in ("posts", "items", "results"):
            if isinstance(payload.get(key), list):
                items = payload[key]
                break
    else:
        items = []

    posts: list[dict[str, Any]] = []
    for item in items:
        raw = _as_dict(item)
        if not raw:
            continue
        publication = _as_dict(raw.get("publication"))
        post = {
            "external_post_id": _string_or_none(raw.get("id") or raw.get("post_id")),
            "publication_substack_id": _string_or_none(raw.get("publication_id") or publication.get("id")),
            "title": raw.get("title"),
            "url": raw.get("canonical_url") or raw.get("url"),
            "published_at": raw.get("post_date") or raw.get("published_at") or raw.get("created_at"),
            "raw_json": json.dumps(raw, sort_keys=True, ensure_ascii=True),
        }
        posts.append(post)
    return posts


def extract_comments_from_response(payload: Any) -> list[dict[str, Any]]:
    """Flatten nested comment trees into a normalized comment list."""
    roots: list[Any]
    if isinstance(payload, list):
        roots = payload
    elif isinstance(payload, dict):
        roots = []
        for key in ("comments", "items", "results", "thread"):
            if isinstance(payload.get(key), list):
                roots = payload[key]
                break
    else:
        roots = []

    flat: list[dict[str, Any]] = []

    def walk(node: Any, parent_external_comment_id: str | None = None) -> None:
        normalized = normalize_comment(node, parent_external_comment_id=parent_external_comment_id)
        if not normalized:
            return
        flat.append(normalized)

        node_dict = _as_dict(node)
        next_parent = normalized.get("external_comment_id") or parent_external_comment_id
        for child_key in ("children", "replies", "child_comments"):
            children = node_dict.get(child_key)
            if isinstance(children, list):
                for child in children:
                    walk(child, parent_external_comment_id=next_parent)
                break

    for root in roots:
        walk(root, parent_external_comment_id=None)
    return flat


def normalize_user(comment: Any) -> dict[str, Any]:
    """Extract commenter identity from multiple possible payload shapes."""
    data = _as_dict(comment)
    user: dict[str, Any] = {}
    for key in ("user", "commenter", "author", "creator"):
        candidate = data.get(key)
        if isinstance(candidate, dict):
            user = candidate
            break

    external_user_id = _string_or_none(
        user.get("id")
        or user.get("user_id")
        or data.get("user_id")
    )

    return {
        "external_user_id": external_user_id,
        "name": (
            user.get("name")
            or user.get("full_name")
            or user.get("display_name")
            or data.get("name")
            or data.get("user_name")
        ),
        "handle": (
            user.get("handle")
            or user.get("username")
            or user.get("slug")
            or data.get("handle")
            or data.get("user_slug")
            or data.get("username")
            or data.get("slug")
        ),
        "profile_url": (
            user.get("profile_url")
            or user.get("url")
            or data.get("profile_url")
            or data.get("user_url")
        ),
        # Do not map the comment-thread publication_id to user publication_id.
        # That publication_id belongs to the commented post, not the commenter.
        "publication_substack_id": _string_or_none(user.get("publication_id")),
        "publication_role": _string_or_none(user.get("publication_role") or user.get("role")),
        "is_publication_owner": int(
            bool(user.get("is_publication_owner") or data.get("is_publication_owner"))
        ),
    }


def normalize_comment(comment: Any, *, parent_external_comment_id: str | None = None) -> dict[str, Any] | None:
    """Normalize one comment node."""
    data = _as_dict(comment)
    if not data:
        return None

    body = data.get("body") or data.get("body_text") or data.get("text")
    if isinstance(body, dict):
        body = body.get("text") or json.dumps(body, sort_keys=True, ensure_ascii=True)
    if body is not None and not isinstance(body, str):
        body = str(body)
    if data.get("deleted") and body is None:
        body = ""

    normalized = {
        "external_comment_id": _string_or_none(data.get("id") or data.get("comment_id")),
        "parent_external_comment_id": _string_or_none(parent_external_comment_id),
        "body": body,
        "commented_at": data.get("date") or data.get("created_at") or data.get("published_at"),
        "deleted": bool(data.get("deleted")),
        "post_external_id": _string_or_none(data.get("post_id")),
        "user": normalize_user(data),
        "raw_json": json.dumps(data, sort_keys=True, ensure_ascii=True),
    }
    return normalized


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)
