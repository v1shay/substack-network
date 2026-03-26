#!/usr/bin/env python3
"""Shared publication/recommendation persistence helpers for crawl flows."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import sqlite3
from typing import Any, Iterable


def normalize_domain(url: str) -> str:
    """Normalize a newsletter/custom-domain URL into the stored domain key."""
    clean = url.replace("https://", "").replace("http://", "")
    clean = clean.split("/")[0]
    clean = clean.split(":")[0]
    clean = clean.lower()

    if clean.endswith(".substack.com"):
        return clean.split(".")[0]
    if re.fullmatch(r"[a-z0-9-]+", clean):
        return clean
    return clean


def domain_to_publication_url(domain: str) -> str:
    if "." in domain and "substack.com" not in domain:
        return f"https://{domain}"
    clean_sub = domain.replace(".substack.com", "")
    return f"https://{clean_sub}.substack.com"


def upsert_publication(conn: sqlite3.Connection, *, domain: str, publication_info: dict[str, Any]) -> None:
    seen_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO publications (substack_id, name, domain, description, first_seen)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(domain) DO UPDATE SET
            substack_id = COALESCE(excluded.substack_id, publications.substack_id),
            name = COALESCE(excluded.name, publications.name),
            description = COALESCE(excluded.description, publications.description),
            first_seen = COALESCE(publications.first_seen, excluded.first_seen)
        """,
        (
            str(publication_info.get("id")) if publication_info.get("id") is not None else None,
            publication_info.get("name") or None,
            domain,
            publication_info.get("hero_text") or None,
            seen_at,
        ),
    )


def add_to_queue(conn: sqlite3.Connection, domain: str, depth: int) -> None:
    conn.execute(
        """
        INSERT INTO queue (domain, depth)
        VALUES (?, ?)
        ON CONFLICT(domain) DO UPDATE SET
            depth = MIN(queue.depth, excluded.depth)
        """,
        (domain, depth),
    )


def persist_recommendations(
    conn: sqlite3.Connection,
    *,
    source_domain: str,
    depth: int,
    recommendation_objects: Iterable[Any],
) -> int:
    inserted = 0
    for recommendation in recommendation_objects:
        recommendation_url = getattr(recommendation, "url", None)
        if not recommendation_url:
            continue
        recommendation_domain = normalize_domain(str(recommendation_url))
        if not recommendation_domain:
            continue
        before = conn.total_changes
        conn.execute(
            """
            INSERT OR IGNORE INTO recommendations (source_domain, target_domain)
            VALUES (?, ?)
            """,
            (source_domain, recommendation_domain),
        )
        add_to_queue(conn, recommendation_domain, depth + 1)
        inserted += conn.total_changes - before
    return inserted


def mark_queue_status(conn: sqlite3.Connection, *, domain: str, status: str) -> None:
    conn.execute("UPDATE queue SET status = ? WHERE domain = ?", (status, domain))
