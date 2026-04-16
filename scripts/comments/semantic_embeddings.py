#!/usr/bin/env python3
"""Batch semantic embeddings for comments, posts, and publications."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Callable

_CODE_ROOT = Path(__file__).resolve().parents[2]
if str(_CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(_CODE_ROOT))

from scripts.db_runtime import connect_db, ensure_schema

DEFAULT_MODEL = "text-embedding-3-small"
DEFAULT_LIMIT = 100
SourceRow = tuple[int, str]
Embedder = Callable[[list[str], str], list[list[float]]]


@dataclass(frozen=True)
class EmbeddingCandidate:
    source_table: str
    source_id: int
    text: str
    source_hash: str


@dataclass(frozen=True)
class EmbeddingBatchResult:
    run_id: int | None
    processed: int
    embedded: int
    skipped: int


def runtime_root() -> Path:
    return Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()


def collect_embedding_candidates(
    conn: sqlite3.Connection,
    *,
    source_table: str,
    model: str,
    limit: int = DEFAULT_LIMIT,
) -> tuple[list[EmbeddingCandidate], int]:
    """Return rows that need embeddings plus the number skipped as empty/current."""
    rows = _source_rows(conn, source_table, limit=limit)
    has_embeddings = _table_exists(conn, "semantic_embeddings")
    candidates: list[EmbeddingCandidate] = []
    skipped = 0

    for source_id, text in rows:
        clean_text = " ".join((text or "").split())
        if not clean_text:
            skipped += 1
            continue
        source_hash = _hash_text(clean_text)
        if has_embeddings:
            existing = conn.execute(
                """
                SELECT source_hash
                  FROM semantic_embeddings
                 WHERE source_table = ?
                   AND source_id = ?
                   AND model = ?
                """,
                (source_table, source_id, model),
            ).fetchone()
            if existing is not None and existing[0] == source_hash:
                skipped += 1
                continue
        candidates.append(
            EmbeddingCandidate(
                source_table=source_table,
                source_id=source_id,
                text=clean_text,
                source_hash=source_hash,
            )
        )

    return candidates, skipped


def run_embedding_batch(
    conn: sqlite3.Connection,
    *,
    source_table: str = "comments",
    model: str = DEFAULT_MODEL,
    limit: int = DEFAULT_LIMIT,
    embedder: Embedder | None = None,
    dry_run: bool = False,
) -> EmbeddingBatchResult:
    if source_table not in {"comments", "posts", "publications"}:
        raise ValueError("source_table must be one of: comments, posts, publications")

    if not dry_run:
        ensure_schema(conn)

    candidates, skipped = collect_embedding_candidates(
        conn,
        source_table=source_table,
        model=model,
        limit=limit,
    )
    if dry_run:
        return EmbeddingBatchResult(run_id=None, processed=len(candidates) + skipped, embedded=0, skipped=skipped)

    run_id = _create_run(conn, source_table=source_table, model=model, target_limit=limit)
    conn.commit()

    embedded = 0
    error: str | None = None
    try:
        if candidates:
            vectors = (embedder or _openai_embedder)([candidate.text for candidate in candidates], model)
            if len(vectors) != len(candidates):
                raise RuntimeError("embedding provider returned a different number of vectors than inputs")
            for candidate, vector in zip(candidates, vectors):
                _upsert_embedding(conn, candidate, model=model, vector=vector)
                embedded += 1
        _finish_run(conn, run_id, status="succeeded", processed=len(candidates) + skipped, embedded=embedded, skipped=skipped)
        conn.commit()
    except Exception as exc:
        error = str(exc)
        _finish_run(conn, run_id, status="failed", processed=len(candidates) + skipped, embedded=embedded, skipped=skipped, error=error)
        conn.commit()
        raise

    return EmbeddingBatchResult(run_id=run_id, processed=len(candidates) + skipped, embedded=embedded, skipped=skipped)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate semantic embeddings for comments, posts, or publications.")
    parser.add_argument("--db", default=None, help="Path to cartographer.db (default: CARTOGRAPHER_ROOT/cartographer.db).")
    parser.add_argument("--source", choices=("comments", "posts", "publications"), default="comments")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--dry-run", action="store_true", help="Show how many rows need embeddings without writing or calling OpenAI.")
    parser.add_argument("--check-config", action="store_true", help="Validate OpenAI package/API key and source rows without API calls.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    db_path = Path(args.db).expanduser().resolve() if args.db else runtime_root() / "cartographer.db"

    read_only = args.dry_run or args.check_config
    conn = (
        sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        if read_only
        else connect_db(db_path)
    )
    try:
        if args.check_config:
            missing = []
            if not os.environ.get("OPENAI_API_KEY"):
                missing.append("OPENAI_API_KEY")
            try:
                import openai  # noqa: F401
            except ImportError:
                missing.append("openai package")
            candidates, skipped = collect_embedding_candidates(
                conn,
                source_table=args.source,
                model=args.model,
                limit=args.limit,
            )
            if missing:
                print("Missing config: " + ", ".join(missing), file=sys.stderr)
                return 1
            print(
                "Config OK: "
                f"db={db_path} source={args.source} model={args.model} "
                f"candidates={len(candidates)} skipped={skipped}"
            )
            return 0

        result = run_embedding_batch(
            conn,
            source_table=args.source,
            model=args.model,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    finally:
        conn.close()

    prefix = "Dry run:" if args.dry_run else "Embedding batch complete:"
    print(
        f"{prefix} run_id={result.run_id} source={args.source} model={args.model} "
        f"processed={result.processed} embedded={result.embedded} skipped={result.skipped}"
    )
    return 0


def _source_rows(conn: sqlite3.Connection, source_table: str, *, limit: int) -> list[SourceRow]:
    if source_table == "comments":
        sql = """
            SELECT id, body
              FROM comments
             ORDER BY id
             LIMIT ?
        """
    elif source_table == "posts":
        sql = """
            SELECT id,
                   TRIM(COALESCE(title, '') || ' ' || COALESCE(url, ''))
              FROM posts
             ORDER BY id
             LIMIT ?
        """
    elif source_table == "publications":
        sql = """
            SELECT id,
                   TRIM(COALESCE(name, '') || ' ' || domain || ' ' || COALESCE(description, ''))
              FROM publications
             ORDER BY id
             LIMIT ?
        """
    else:
        raise ValueError("source_table must be one of: comments, posts, publications")
    return [(int(row[0]), str(row[1] or "")) for row in conn.execute(sql, (limit,)).fetchall()]


def _openai_embedder(texts: list[str], model: str) -> list[list[float]]:
    try:
        import openai
    except ImportError as exc:
        raise RuntimeError("Install the openai package before running semantic embeddings.") from exc
    client = openai.OpenAI()
    response = client.embeddings.create(model=model, input=texts)
    return [list(item.embedding) for item in response.data]


def _create_run(conn: sqlite3.Connection, *, source_table: str, model: str, target_limit: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO semantic_embedding_runs (
            started_at, source_table, model, status, target_limit
        ) VALUES (?, ?, ?, 'running', ?)
        """,
        (_now_iso(), source_table, model, target_limit),
    )
    return int(cur.lastrowid)


def _finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    processed: int,
    embedded: int,
    skipped: int,
    error: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE semantic_embedding_runs
           SET finished_at = ?,
               status = ?,
               processed = ?,
               embedded = ?,
               skipped = ?,
               error = ?
         WHERE id = ?
        """,
        (_now_iso(), status, processed, embedded, skipped, error, run_id),
    )


def _upsert_embedding(
    conn: sqlite3.Connection,
    candidate: EmbeddingCandidate,
    *,
    model: str,
    vector: list[float],
) -> None:
    conn.execute(
        """
        INSERT INTO semantic_embeddings (
            source_table, source_id, source_hash, model, dimensions, embedding_json, embedded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_table, source_id, model) DO UPDATE SET
            source_hash = excluded.source_hash,
            dimensions = excluded.dimensions,
            embedding_json = excluded.embedding_json,
            embedded_at = excluded.embedded_at
        """,
        (
            candidate.source_table,
            candidate.source_id,
            candidate.source_hash,
            model,
            len(vector),
            json.dumps(vector, separators=(",", ":"), ensure_ascii=True),
            _now_iso(),
        ),
    )


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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
