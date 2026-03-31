#!/usr/bin/env python3
"""HTTP helpers for Substack archive and comments APIs.

This module is independent from the publication crawl loop and can be used
manually or from tests.
"""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Any
from urllib.parse import urlparse

import requests

LOG = logging.getLogger(__name__)

_COMMENT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
_DEFAULT_COMMENT_PACING_SECONDS = 0.12
_DEFAULT_COMMENT_JITTER_SECONDS = 0.08


class CommentAPIFetchError(RuntimeError):
    """Raised when a comment/archive endpoint cannot be fetched successfully."""

    def __init__(self, url: str, detail: str) -> None:
        super().__init__(f"comment API request failed for {url}: {detail}")
        self.url = url
        self.detail = detail


def _normalize_publication_url(publication_url: str) -> str:
    value = (publication_url or "").strip()
    if not value:
        raise ValueError("publication_url is required")
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    parsed = urlparse(value)
    if not parsed.netloc:
        raise ValueError(f"invalid publication_url: {publication_url!r}")
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def _sleep_with_jitter(base_seconds: float, jitter_seconds: float) -> None:
    base = max(float(base_seconds), 0.0)
    jitter = max(float(jitter_seconds), 0.0)
    delay = base
    if jitter > 0:
        delay += random.uniform(0.0, jitter)
    if delay > 0:
        time.sleep(delay)


def _retry_delay_seconds(
    response: Any | None,
    *,
    attempt: int,
    backoff_seconds: float,
    jitter_seconds: float,
) -> float:
    fallback = max(float(backoff_seconds), 0.0) * max(int(attempt), 1)
    retry_after_raw = None
    if response is not None:
        headers = getattr(response, "headers", {}) or {}
        retry_after_raw = headers.get("Retry-After")

    retry_after_seconds = 0.0
    if retry_after_raw is not None:
        try:
            retry_after_seconds = max(float(retry_after_raw), 0.0)
        except (TypeError, ValueError):
            retry_after_seconds = 0.0

    base = max(fallback, retry_after_seconds)
    if jitter_seconds > 0:
        base += random.uniform(0.0, max(float(jitter_seconds), 0.0))
    return base


def _log_domain(url: str) -> str:
    return urlparse(url).netloc.strip().lower() or "unknown"


def _request_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 15.0,
    retries: int = 3,
    backoff_seconds: float = 0.5,
    pacing_seconds: float = _DEFAULT_COMMENT_PACING_SECONDS,
    jitter_seconds: float = _DEFAULT_COMMENT_JITTER_SECONDS,
    session: requests.Session | Any | None = None,
) -> Any | None:
    created_session = session is None
    sess = session or requests.Session()
    try:
        for attempt in range(1, max(retries, 1) + 1):
            _sleep_with_jitter(pacing_seconds, jitter_seconds)
            try:
                response = sess.get(
                    url,
                    params=params,
                    timeout=timeout,
                    allow_redirects=True,
                    headers=_COMMENT_HEADERS,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    LOG.error(
                        "[comments][error] domain=%s transient status code %s for %s (attempt %s/%s)",
                        _log_domain(url),
                        response.status_code,
                        url,
                        attempt,
                        retries,
                    )
                    if attempt < retries:
                        time.sleep(
                            _retry_delay_seconds(
                                response,
                                attempt=attempt,
                                backoff_seconds=backoff_seconds,
                                jitter_seconds=jitter_seconds,
                            )
                        )
                        continue
                    raise CommentAPIFetchError(url, f"transient status code {response.status_code}")

                response.raise_for_status()
                try:
                    payload = response.json()
                except ValueError:
                    LOG.error("[comments][error] domain=%s invalid JSON response for %s", _log_domain(url), url)
                    raise CommentAPIFetchError(url, "invalid JSON response")

                if not isinstance(payload, (dict, list)):
                    LOG.error(
                        "[comments][error] domain=%s unexpected JSON type %s for %s",
                        _log_domain(url),
                        type(payload),
                        url,
                    )
                    raise CommentAPIFetchError(url, f"unexpected JSON type {type(payload).__name__}")
                return payload
            except requests.RequestException as exc:
                LOG.error(
                    "[comments][error] domain=%s request failed for %s (attempt %s/%s): %s",
                    _log_domain(url),
                    url,
                    attempt,
                    retries,
                    exc,
                )
                if attempt < retries:
                    time.sleep(
                        _retry_delay_seconds(
                            getattr(exc, "response", None),
                            attempt=attempt,
                            backoff_seconds=backoff_seconds,
                            jitter_seconds=jitter_seconds,
                        )
                    )
                    continue
                raise CommentAPIFetchError(url, str(exc)) from exc
    finally:
        if created_session:
            sess.close()
    return None


def _items_from_archive_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("posts", "items", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _items_from_comments_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("comments", "items", "results", "thread"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def fetch_archive(
    publication_url: str,
    *,
    page_size: int = 20,
    max_pages: int = 5,
    start_offset: int = 0,
    timeout: float = 15.0,
    retries: int = 3,
    session: requests.Session | Any | None = None,
) -> list[dict[str, Any]]:
    """Fetch archive posts from `/api/v1/archive` with pagination."""
    base = _normalize_publication_url(publication_url)
    endpoint = f"{base}/api/v1/archive"
    offset = max(start_offset, 0)
    all_posts: list[dict[str, Any]] = []

    for _ in range(max(max_pages, 1)):
        params = {"sort": "new", "offset": offset, "limit": page_size}
        payload = _request_json(
            endpoint,
            params=params,
            timeout=timeout,
            retries=retries,
            session=session,
        )

        posts = _items_from_archive_payload(payload)
        LOG.info("[comments] archive fetched: %s posts (offset=%s)", len(posts), offset)
        if not posts:
            break
        all_posts.extend(posts)

        if isinstance(payload, dict) and payload.get("has_more") is False:
            break
        if len(posts) < page_size:
            break

        offset += len(posts)
    return all_posts


def fetch_post_comments(
    publication_url: str,
    post_id: str | int,
    *,
    page_size: int = 100,
    max_pages: int = 10,
    start_offset: int = 0,
    timeout: float = 15.0,
    retries: int = 3,
    session: requests.Session | Any | None = None,
) -> list[dict[str, Any]]:
    """Fetch post comments with pagination.

    Primary endpoint is `/api/v1/post/{post_id}/comments`.
    Compatibility fallback is `/api/v1/posts/{post_id}/comments`.
    """
    base = _normalize_publication_url(publication_url)
    primary_endpoint = f"{base}/api/v1/post/{post_id}/comments"
    compat_endpoint = f"{base}/api/v1/posts/{post_id}/comments"
    endpoint = primary_endpoint
    tried_compat = False
    offset = max(start_offset, 0)
    all_comments: list[dict[str, Any]] = []
    seen_payload_signatures: set[str] = set()
    seen_comment_keys: set[str] = set()

    for _ in range(max(max_pages, 1)):
        params = {"offset": offset, "limit": page_size, "sort": "new"}
        try:
            payload = _request_json(
                endpoint,
                params=params,
                timeout=timeout,
                retries=retries,
                session=session,
            )
        except CommentAPIFetchError:
            if tried_compat:
                raise
            tried_compat = True
            endpoint = compat_endpoint
            LOG.info(
                "[comments] switching to compatibility endpoint for post=%s endpoint=%s",
                post_id,
                endpoint,
            )
            payload = _request_json(
                endpoint,
                params=params,
                timeout=timeout,
                retries=retries,
                session=session,
            )

        comments = _items_from_comments_payload(payload)
        LOG.info("[comments] comments fetched: %s comments (post=%s offset=%s)", len(comments), post_id, offset)
        if not comments:
            break
        payload_signature = _payload_signature(comments)
        if payload_signature in seen_payload_signatures:
            LOG.info("[comments] repeated comments payload detected; stopping (post=%s offset=%s)", post_id, offset)
            break
        seen_payload_signatures.add(payload_signature)

        new_comments = []
        for comment in comments:
            key = _comment_dedupe_key(comment)
            if key in seen_comment_keys:
                continue
            seen_comment_keys.add(key)
            new_comments.append(comment)
        if not new_comments:
            LOG.info("[comments] page yielded no new comments; stopping (post=%s offset=%s)", post_id, offset)
            break
        all_comments.extend(new_comments)

        if isinstance(payload, dict):
            next_offset = payload.get("next_offset")
            if isinstance(next_offset, int) and next_offset > offset:
                offset = next_offset
                continue
            if payload.get("has_more") is False:
                break

        if len(comments) < page_size:
            break
        offset += len(comments)
    return all_comments


def _payload_signature(payload: list[dict[str, Any]]) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=True)


def _comment_dedupe_key(comment: dict[str, Any]) -> str:
    comment_id = comment.get("id") or comment.get("comment_id")
    if comment_id is not None:
        return f"id:{comment_id}"
    return "payload:" + json.dumps(comment, sort_keys=True, ensure_ascii=True)
