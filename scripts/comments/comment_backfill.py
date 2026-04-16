#!/usr/bin/env python3
"""Durable backfill runner for comment ingestion on already-crawled publications."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Any, Iterable

_CODE_ROOT = Path(__file__).resolve().parents[2]
if str(_CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(_CODE_ROOT))

from scripts.comments.comment_pipeline import process_comments
from scripts.crawl_persistence import domain_to_publication_url
from scripts.db_runtime import connect_db, ensure_schema


DEFAULT_POST_LIMIT = 3
DEFAULT_TARGET_LIMIT = 50
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_MAX_ATTEMPTS = 3
RETRYABLE_STATUSES = ("pending", "failed")


@dataclass(frozen=True)
class BackfillTarget:
    domain: str
    publication_substack_id: str | None
    status: str | None
    attempts: int


@dataclass(frozen=True)
class BackfillResult:
    run_id: int | None
    selected: int
    succeeded: int
    failed: int
    skipped: int


def runtime_root() -> Path:
    return Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()


def select_backfill_targets(
    conn: sqlite3.Connection,
    *,
    limit: int | None = DEFAULT_TARGET_LIMIT,
    domains: Iterable[str] | None = None,
    statuses: tuple[str, ...] = RETRYABLE_STATUSES,
    max_attempts: int | None = DEFAULT_MAX_ATTEMPTS,
) -> list[BackfillTarget]:
    """Select existing publications that need comment backfill work."""
    domain_list = [d.strip().lower() for d in domains or [] if d and d.strip()]
    params: list[Any] = []
    filters: list[str] = []

    if domain_list:
        placeholders = ", ".join("?" for _ in domain_list)
        filters.append(f"p.domain IN ({placeholders})")
        params.extend(domain_list)

    has_status = _table_exists(conn, "comment_publication_status")
    if has_status:
        status_placeholders = ", ".join("?" for _ in statuses)
        retry_filter = f"(s.domain IS NULL OR s.status IN ({status_placeholders}))"
        params.extend(statuses)
        if max_attempts is not None:
            retry_filter += " AND COALESCE(s.attempts, 0) < ?"
            params.append(max_attempts)
        filters.append(retry_filter)
        status_select = "s.status, COALESCE(s.attempts, 0) AS attempts"
        status_join = "LEFT JOIN comment_publication_status s ON s.domain = p.domain"
        order_by = "COALESCE(s.updated_at, p.first_seen, ''), p.domain"
    else:
        status_select = "NULL AS status, 0 AS attempts"
        status_join = ""
        order_by = "COALESCE(p.first_seen, ''), p.domain"

    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    limit_sql = "" if limit is None else "LIMIT ?"
    if limit is not None:
        params.append(limit)

    rows = conn.execute(
        f"""
        SELECT p.domain, p.substack_id, {status_select}
          FROM publications p
          {status_join}
          {where}
         ORDER BY {order_by}
         {limit_sql}
        """,
        params,
    ).fetchall()
    return [
        BackfillTarget(
            domain=str(row[0]),
            publication_substack_id=str(row[1]) if row[1] is not None else None,
            status=str(row[2]) if row[2] is not None else None,
            attempts=int(row[3] or 0),
        )
        for row in rows
    ]


def seed_comment_publication_status(
    conn: sqlite3.Connection,
    *,
    limit: int | None = DEFAULT_TARGET_LIMIT,
    domains: Iterable[str] | None = None,
) -> int:
    """Pre-create pending status rows for publications without running ingestion."""
    ensure_schema(conn)
    targets = select_backfill_targets(
        conn,
        limit=limit,
        domains=domains,
        statuses=("pending", "failed", "running"),
        max_attempts=None,
    )
    now = _now_iso()
    for target in targets:
        conn.execute(
            """
            INSERT INTO comment_publication_status (
                domain, publication_substack_id, status, attempts, updated_at
            ) VALUES (?, ?, 'pending', 0, ?)
            ON CONFLICT(domain) DO UPDATE SET
                publication_substack_id = COALESCE(excluded.publication_substack_id, comment_publication_status.publication_substack_id),
                updated_at = excluded.updated_at
            """,
            (target.domain, target.publication_substack_id, now),
        )
    conn.commit()
    return len(targets)


def run_backfill(
    conn: sqlite3.Connection,
    *,
    limit: int | None = DEFAULT_TARGET_LIMIT,
    domains: Iterable[str] | None = None,
    post_limit: int = DEFAULT_POST_LIMIT,
    timeout: float = 15.0,
    retries: int = 3,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    max_attempts: int | None = DEFAULT_MAX_ATTEMPTS,
    classify_commenters: bool = False,
    classify_max_users: int = 25,
    classify_workers: int = 4,
    session: Any | None = None,
    classification_session: Any | None = None,
    stop_on_error: bool = False,
    notes: str | None = None,
) -> BackfillResult:
    """Run a resumable comment backfill batch against existing publications."""
    ensure_schema(conn)
    targets = select_backfill_targets(
        conn,
        limit=limit,
        domains=domains,
        statuses=RETRYABLE_STATUSES,
        max_attempts=max_attempts,
    )
    run_id = _create_run(
        conn,
        mode="domain-list" if domains else "pilot",
        post_limit=post_limit,
        target_limit=limit,
        delay_seconds=delay_seconds,
        notes=notes,
    )
    conn.commit()

    succeeded = 0
    failed = 0
    skipped = 0
    run_error: str | None = None

    for index, target in enumerate(targets, start=1):
        _mark_publication_started(conn, target)
        conn.commit()

        try:
            stats = process_comments(
                domain_to_publication_url(target.domain),
                conn=conn,
                post_limit=post_limit,
                timeout=timeout,
                retries=retries,
                classify_commenters=classify_commenters,
                classify_max_users=classify_max_users,
                classify_workers=classify_workers,
                session=session,
                classification_session=classification_session,
            )
        except Exception as exc:
            failed += 1
            run_error = str(exc)
            _mark_publication_failed(conn, target, exc)
            conn.commit()
            print(f"[comments][backfill][error] domain={target.domain} error={exc}", file=sys.stderr)
            if stop_on_error:
                break
        else:
            succeeded += 1
            _mark_publication_succeeded(conn, target, stats)
            conn.commit()
            print(
                "[comments][backfill] "
                f"{index}/{len(targets)} domain={target.domain} "
                f"posts_seen={stats.get('posts_seen', 0)} "
                f"comments_created={stats.get('comments_created', 0)} "
                f"comments_unique={stats.get('comments_unique', 0)}"
            )

        if delay_seconds > 0 and index < len(targets):
            time.sleep(delay_seconds)

    if stop_on_error and failed:
        skipped = len(targets) - succeeded - failed
    status = "succeeded" if failed == 0 else ("failed" if stop_on_error else "completed_with_errors")
    _finish_run(conn, run_id, status=status, error=run_error if status == "failed" else None)
    conn.commit()
    return BackfillResult(run_id=run_id, selected=len(targets), succeeded=succeeded, failed=failed, skipped=skipped)


def summarize_backfill_state(conn: sqlite3.Connection) -> dict[str, int]:
    if not _table_exists(conn, "comment_publication_status"):
        return {}
    rows = conn.execute(
        """
        SELECT status, COUNT(*)
          FROM comment_publication_status
         GROUP BY status
         ORDER BY status
        """
    ).fetchall()
    return {str(status): int(count) for status, count in rows}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill comments for already-crawled publications with durable per-publication status."
    )
    parser.add_argument("--db", default=None, help="Path to cartographer.db (default: CARTOGRAPHER_ROOT/cartographer.db).")
    parser.add_argument("--limit", type=int, default=DEFAULT_TARGET_LIMIT, help="Max publications to process (default: 50).")
    parser.add_argument("--all", action="store_true", help="Remove the default target limit and process all eligible publications.")
    parser.add_argument("--domain", action="append", default=[], help="Only process a specific domain; can be repeated.")
    parser.add_argument("--domains-file", default=None, help="Optional file containing one domain or URL per line.")
    parser.add_argument("--post-limit", type=int, default=DEFAULT_POST_LIMIT, help="Archive posts per publication (default: 3).")
    parser.add_argument("--timeout", type=float, default=15.0, help="Request timeout in seconds (default: 15).")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retries per endpoint (default: 3).")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Delay between publications in seconds (default: 1).")
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS, help="Max attempts per publication before it is skipped (default: 3).")
    parser.add_argument("--classify-commenters", action="store_true", help="Also classify commenter handles via public_profile.")
    parser.add_argument("--classification-max-users", type=int, default=25, help="Max commenters to classify per publication.")
    parser.add_argument("--classification-workers", type=int, default=4, help="Profile lookup worker count.")
    parser.add_argument("--dry-run", action="store_true", help="List selected targets without writing or fetching comments.")
    parser.add_argument("--seed-only", action="store_true", help="Create pending status rows for selected targets, then exit.")
    parser.add_argument("--summary", action="store_true", help="Print status counts before doing any work.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop the batch on the first publication failure.")
    parser.add_argument("--notes", default=None, help="Optional run note stored in comment_ingestion_runs.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    db_path = Path(args.db).expanduser().resolve() if args.db else runtime_root() / "cartographer.db"
    limit = None if args.all else args.limit
    domains = [*args.domain, *_domains_from_file(args.domains_file)]

    if args.dry_run:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    else:
        conn = connect_db(db_path)

    try:
        if not args.dry_run:
            ensure_schema(conn)

        if args.summary:
            summary = summarize_backfill_state(conn)
            print(f"Backfill status summary: {summary if summary else 'no status rows yet'}")

        if args.dry_run:
            targets = select_backfill_targets(
                conn,
                limit=limit,
                domains=domains,
                statuses=RETRYABLE_STATUSES,
                max_attempts=args.max_attempts,
            )
            print(f"Dry run: selected {len(targets)} publication(s).")
            for target in targets[:20]:
                print(
                    f"  {target.domain} status={target.status or 'new'} "
                    f"attempts={target.attempts} publication_substack_id={target.publication_substack_id or ''}"
                )
            if len(targets) > 20:
                print(f"  ... and {len(targets) - 20} more")
            return 0

        if args.seed_only:
            count = seed_comment_publication_status(conn, limit=limit, domains=domains)
            print(f"Seeded {count} comment backfill status row(s).")
            return 0

        result = run_backfill(
            conn,
            limit=limit,
            domains=domains,
            post_limit=args.post_limit,
            timeout=args.timeout,
            retries=args.retries,
            delay_seconds=args.delay,
            max_attempts=args.max_attempts,
            classify_commenters=args.classify_commenters,
            classify_max_users=args.classification_max_users,
            classify_workers=args.classification_workers,
            stop_on_error=args.stop_on_error,
            notes=args.notes,
        )
    finally:
        conn.close()

    print(
        "Comment backfill complete: "
        f"run_id={result.run_id} selected={result.selected} "
        f"succeeded={result.succeeded} failed={result.failed} skipped={result.skipped}"
    )
    return 1 if result.failed and args.stop_on_error else 0


def _create_run(
    conn: sqlite3.Connection,
    *,
    mode: str,
    post_limit: int,
    target_limit: int | None,
    delay_seconds: float,
    notes: str | None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO comment_ingestion_runs (
            started_at, mode, status, post_limit, target_limit, delay_seconds, notes
        ) VALUES (?, ?, 'running', ?, ?, ?, ?)
        """,
        (_now_iso(), mode, post_limit, target_limit, delay_seconds, notes),
    )
    return int(cur.lastrowid)


def _finish_run(conn: sqlite3.Connection, run_id: int, *, status: str, error: str | None = None) -> None:
    conn.execute(
        """
        UPDATE comment_ingestion_runs
           SET finished_at = ?,
               status = ?,
               error = ?
         WHERE id = ?
        """,
        (_now_iso(), status, error, run_id),
    )


def _mark_publication_started(conn: sqlite3.Connection, target: BackfillTarget) -> None:
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO comment_publication_status (
            domain, publication_substack_id, status, attempts, last_attempt_at, updated_at
        ) VALUES (?, ?, 'running', 1, ?, ?)
        ON CONFLICT(domain) DO UPDATE SET
            publication_substack_id = COALESCE(excluded.publication_substack_id, comment_publication_status.publication_substack_id),
            status = 'running',
            attempts = comment_publication_status.attempts + 1,
            last_attempt_at = excluded.last_attempt_at,
            last_error = NULL,
            updated_at = excluded.updated_at
        """,
        (target.domain, target.publication_substack_id, now, now),
    )


def _mark_publication_succeeded(
    conn: sqlite3.Connection,
    target: BackfillTarget,
    stats: dict[str, int],
) -> None:
    now = _now_iso()
    conn.execute(
        """
        UPDATE comment_publication_status
           SET publication_substack_id = COALESCE(?, publication_substack_id),
               status = 'succeeded',
               last_success_at = ?,
               posts_seen = ?,
               posts_created = ?,
               posts_updated = ?,
               users_seen = ?,
               users_created = ?,
               users_updated = ?,
               comments_fetched = ?,
               comments_unique = ?,
               comments_created = ?,
               comments_updated = ?,
               last_error = NULL,
               updated_at = ?
         WHERE domain = ?
        """,
        (
            target.publication_substack_id,
            now,
            int(stats.get("posts_seen", 0)),
            int(stats.get("posts_created", 0)),
            int(stats.get("posts_updated", 0)),
            int(stats.get("users_seen", 0)),
            int(stats.get("users_created", 0)),
            int(stats.get("users_updated", 0)),
            int(stats.get("comments_fetched", 0)),
            int(stats.get("comments_unique", 0)),
            int(stats.get("comments_created", 0)),
            int(stats.get("comments_updated", 0)),
            now,
            target.domain,
        ),
    )


def _mark_publication_failed(conn: sqlite3.Connection, target: BackfillTarget, exc: Exception) -> None:
    now = _now_iso()
    conn.execute(
        """
        UPDATE comment_publication_status
           SET publication_substack_id = COALESCE(?, publication_substack_id),
               status = 'failed',
               last_error = ?,
               updated_at = ?
         WHERE domain = ?
        """,
        (target.publication_substack_id, str(exc), now, target.domain),
    )


def _domains_from_file(path: str | None) -> list[str]:
    if not path:
        return []
    domains: list[str] = []
    for raw in Path(path).expanduser().read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        value = line.split()[0]
        value = value.replace("https://", "").replace("http://", "").rstrip("/")
        domains.append(value.split("/", 1)[0].lower())
    return domains


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
