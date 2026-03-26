#!/usr/bin/env python3
"""Classify commenter users as publication owners via Substack public profiles."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import random
import re
import sqlite3
import time
from typing import Any
from urllib.parse import quote, urlparse

import requests

from .db_helpers import update_user_classification

LOG = logging.getLogger(__name__)

_PROFILE_BASE = "https://substack.com/api/v1/user/{handle}/public_profile"
_PROFILE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
_DEFAULT_PROFILE_PACING_SECONDS = 0.02
_DEFAULT_PROFILE_JITTER_SECONDS = 0.03


def _sleep_with_jitter(base_seconds: float, jitter_seconds: float) -> None:
    base = max(float(base_seconds), 0.0)
    jitter = max(float(jitter_seconds), 0.0)
    delay = base
    if jitter > 0:
        delay += random.uniform(0.0, jitter)
    if delay > 0:
        time.sleep(delay)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _normalize_handle(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.startswith(("http://", "https://")):
        parsed = urlparse(text)
        host = parsed.netloc.lower()
        path = parsed.path.strip("/")
        if host == "substack.com" and path.startswith("@"):
            text = path[1:]
        elif host == "substack.com" and path:
            text = path.split("/", 1)[0]
        elif host.endswith(".substack.com"):
            text = host.split(".", 1)[0]

    text = text.strip().strip("/")
    if text.startswith("@"):
        text = text[1:]
    text = text.split("?", 1)[0].split("#", 1)[0]
    text = text.split("/", 1)[0]
    if not text:
        return None

    if not re.fullmatch(r"[A-Za-z0-9_.-]+", text):
        return None
    return text.lower()


def fetch_public_profile(
    handle: str,
    *,
    timeout: float = 10.0,
    retries: int = 2,
    backoff_seconds: float = 0.5,
    pacing_seconds: float = _DEFAULT_PROFILE_PACING_SECONDS,
    jitter_seconds: float = _DEFAULT_PROFILE_JITTER_SECONDS,
    session: requests.Session | Any | None = None,
) -> dict[str, Any] | None:
    """Fetch one user profile from Substack public_profile endpoint."""
    normalized = _normalize_handle(handle)
    if not normalized:
        return None

    endpoint = _PROFILE_BASE.format(handle=quote(normalized, safe=""))
    created_session = session is None
    sess = session or requests.Session()
    try:
        for attempt in range(1, max(retries, 1) + 1):
            _sleep_with_jitter(pacing_seconds, jitter_seconds)
            try:
                response = sess.get(
                    endpoint,
                    timeout=timeout,
                    allow_redirects=True,
                    headers=_PROFILE_HEADERS,
                )
                status_code = response.status_code

                if status_code == 404:
                    return None
                if status_code == 429 or status_code >= 500:
                    LOG.error(
                        "[comments][error] transient status code %s for %s (attempt %s/%s)",
                        status_code,
                        endpoint,
                        attempt,
                        retries,
                    )
                    if attempt < retries:
                        time.sleep(backoff_seconds * attempt)
                        continue
                    return None

                response.raise_for_status()
                try:
                    payload = response.json()
                except ValueError:
                    LOG.error("[comments][error] invalid JSON response for %s", endpoint)
                    return None
                if not isinstance(payload, dict):
                    LOG.error(
                        "[comments][error] unexpected JSON type %s for %s",
                        type(payload),
                        endpoint,
                    )
                    return None
                return payload
            except requests.RequestException as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                # 4xx except 429 are not transient for this workflow.
                if isinstance(status_code, int) and status_code < 500 and status_code != 429:
                    LOG.error(
                        "[comments][error] request failed for %s (attempt %s/%s): %s",
                        endpoint,
                        attempt,
                        retries,
                        exc,
                    )
                    return None
                LOG.error(
                    "[comments][error] request failed for %s (attempt %s/%s): %s",
                    endpoint,
                    attempt,
                    retries,
                    exc,
                )
                if attempt < retries:
                    time.sleep(backoff_seconds * attempt)
                    continue
                return None
    finally:
        if created_session:
            sess.close()
    return None


def classify_profile(profile: dict[str, Any] | None) -> dict[str, Any]:
    """Classify one profile using strict `admin + hasPosts` rule."""
    data = _as_dict(profile)
    if not data:
        return {
            "publication_substack_id": None,
            "publication_role": None,
            "is_publication_owner": 0,
        }

    has_posts = bool(data.get("hasPosts"))
    profile_user_id = data.get("id")
    profile_user_id_s = str(profile_user_id) if profile_user_id is not None else None

    primary_publication = _as_dict(data.get("primaryPublication"))
    publication_substack_id = primary_publication.get("id")
    publication_role: str | None = None

    owner_signal = False

    publication_users = data.get("publicationUsers")
    if isinstance(publication_users, list):
        for row_raw in publication_users:
            row = _as_dict(row_raw)
            if not row:
                continue

            row_user_id = row.get("user_id")
            row_user_id_s = str(row_user_id) if row_user_id is not None else None
            # Enforce strict ownership matching when profile user id is known.
            if profile_user_id_s is not None and row_user_id_s != profile_user_id_s:
                continue

            row_publication = _as_dict(row.get("publication"))
            row_publication_id = row.get("publication_id") or row_publication.get("id")
            if publication_substack_id is None and row_publication_id is not None:
                publication_substack_id = row_publication_id

            role = str(row.get("role") or "").lower()
            if publication_role is None and role:
                publication_role = role
            if role == "admin":
                owner_signal = True
                if row_publication_id is not None:
                    publication_substack_id = row_publication_id
                break

    return {
        "publication_substack_id": _string_or_none(publication_substack_id),
        "publication_role": publication_role,
        "is_publication_owner": int(has_posts and owner_signal),
    }


def classify_users(
    conn: sqlite3.Connection,
    *,
    user_ids: list[int],
    max_users: int = 75,
    workers: int = 4,
    timeout: float = 10.0,
    retries: int = 2,
    profile_pacing_seconds: float = _DEFAULT_PROFILE_PACING_SECONDS,
    profile_jitter_seconds: float = _DEFAULT_PROFILE_JITTER_SECONDS,
    session: requests.Session | Any | None = None,
) -> dict[str, int]:
    """Classify a bounded set of users in DB and persist classification fields."""
    stats = {
        "candidate_users": 0,
        "attempted_handles": 0,
        "resolved_profiles": 0,
        "updated_users": 0,
        "owner_users": 0,
    }
    if not user_ids:
        return stats

    ordered_user_ids: list[int] = []
    seen_ids: set[int] = set()
    for user_id in user_ids:
        if not isinstance(user_id, int):
            continue
        if user_id in seen_ids:
            continue
        seen_ids.add(user_id)
        ordered_user_ids.append(user_id)
    if not ordered_user_ids:
        return stats

    placeholders = ",".join("?" for _ in ordered_user_ids)
    cur = conn.cursor()
    cur.execute(
        f"SELECT id, handle, profile_url FROM users WHERE id IN ({placeholders})",
        ordered_user_ids,
    )
    rows_by_id: dict[int, tuple[Any, Any]] = {
        int(row_user_id): (handle, profile_url)
        for row_user_id, handle, profile_url in cur.fetchall()
    }

    handle_to_user_ids: dict[str, list[int]] = {}
    for row_user_id in ordered_user_ids:
        row = rows_by_id.get(row_user_id)
        if row is None:
            continue
        handle, profile_url = row
        normalized_handle = _normalize_handle(handle) or _normalize_handle(profile_url)
        if not normalized_handle:
            continue
        if normalized_handle not in handle_to_user_ids:
            handle_to_user_ids[normalized_handle] = []
        handle_to_user_ids[normalized_handle].append(int(row_user_id))
    stats["candidate_users"] = sum(len(ids) for ids in handle_to_user_ids.values())

    all_handles = list(handle_to_user_ids.keys())
    if max_users >= 0:
        all_handles = all_handles[:max_users]
    if not all_handles:
        return stats
    stats["attempted_handles"] = len(all_handles)

    profiles_by_handle: dict[str, dict[str, Any]] = {}
    # A caller-provided session in tests may not be thread-safe.
    should_parallelize = session is None and workers > 1
    if should_parallelize:
        with ThreadPoolExecutor(max_workers=max(workers, 1)) as executor:
            future_to_handle = {
                executor.submit(
                    fetch_public_profile,
                    handle,
                    timeout=timeout,
                    retries=retries,
                    pacing_seconds=profile_pacing_seconds,
                    jitter_seconds=profile_jitter_seconds,
                ): handle
                for handle in all_handles
            }
            for future in as_completed(future_to_handle):
                handle = future_to_handle[future]
                try:
                    profile = future.result()
                except Exception as exc:  # pragma: no cover - defensive fail-open guard
                    LOG.error("[comments][error] profile classification failed for %s: %s", handle, exc)
                    continue
                if profile:
                    profiles_by_handle[handle] = profile
    else:
        for handle in all_handles:
            profile = fetch_public_profile(
                handle,
                timeout=timeout,
                retries=retries,
                pacing_seconds=profile_pacing_seconds,
                jitter_seconds=profile_jitter_seconds,
                session=session,
            )
            if profile:
                profiles_by_handle[handle] = profile

    stats["resolved_profiles"] = len(profiles_by_handle)

    for handle, profile in profiles_by_handle.items():
        outcome = classify_profile(profile)
        publication_substack_id = outcome.get("publication_substack_id")
        publication_role = outcome.get("publication_role")
        owner_flag = int(bool(outcome.get("is_publication_owner")))
        for row_user_id in handle_to_user_ids.get(handle, []):
            updated = update_user_classification(
                conn,
                user_id=row_user_id,
                publication_substack_id=publication_substack_id,
                publication_role=publication_role,
                is_publication_owner=owner_flag,
            )
            if not updated:
                continue
            stats["updated_users"] += 1
            if owner_flag == 1:
                stats["owner_users"] += 1

    return stats


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)
