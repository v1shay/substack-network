#!/usr/bin/env python3
"""Shared SQLite connection, schema creation, and legacy-schema migration."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3

SCHEMA_VERSION = 2
QUEUE_STATUSES = ("pending", "crawled", "failed")
CORE_TABLES = ("publications", "recommendations", "queue", "users", "posts", "comments")


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    if _is_current_schema(conn):
        return

    existing_tables = _existing_tables(conn)
    current_version = _read_schema_version(conn, existing_tables)
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        if not existing_tables & set(CORE_TABLES):
            _create_schema(conn)
            _set_schema_version(conn, SCHEMA_VERSION)
        elif current_version is None:
            _migrate_legacy_schema(conn)
        else:
            _upgrade_schema(conn, current_version)
        conn.commit()
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def _upgrade_schema(conn: sqlite3.Connection, current_version: int) -> None:
    if current_version > SCHEMA_VERSION:
        raise RuntimeError(
            f"Database schema version {current_version} is newer than this code supports ({SCHEMA_VERSION})."
        )

    if current_version < 2:
        # Version 2 adds operational state for large comment backfills and
        # semantic embedding batches. Core crawl/comment table columns are unchanged.
        _create_schema(conn)
        _set_schema_version(conn, SCHEMA_VERSION)
        return

    if current_version == SCHEMA_VERSION:
        # Heal missing sidecar tables/indexes for partially applied migrations.
        _create_schema(conn)
        _set_schema_version(conn, SCHEMA_VERSION)
        return


def ensure_sidecar_schema(conn: sqlite3.Connection) -> None:
    """Create v2 sidecar tables without migrating legacy core tables.

    Readiness tools use this only after they know the core schema is usable.
    Normal callers should prefer ensure_schema(...).
    """
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        _create_schema(conn)
        _set_schema_version(conn, SCHEMA_VERSION)
        conn.commit()
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def schema_is_current(conn: sqlite3.Connection) -> bool:
    return _is_current_schema(conn)


def expected_schema_columns() -> dict[str, tuple[str, ...]]:
    return {
        "schema_version": ("singleton", "version", "updated_at"),
        "publications": ("id", "substack_id", "name", "domain", "description", "first_seen"),
        "recommendations": ("id", "source_domain", "target_domain"),
        "queue": ("domain", "status", "depth"),
        "users": (
            "id",
            "external_user_id",
            "name",
            "handle",
            "profile_url",
            "publication_substack_id",
            "publication_role",
            "is_publication_owner",
            "first_seen",
            "last_seen",
        ),
        "posts": (
            "id",
            "external_post_id",
            "publication_substack_id",
            "title",
            "url",
            "published_at",
            "first_seen",
            "last_seen",
        ),
        "comments": (
            "id",
            "external_comment_id",
            "post_id",
            "user_id",
            "parent_comment_id",
            "parent_external_comment_id",
            "body",
            "commented_at",
            "raw_json",
            "first_seen",
            "last_seen",
        ),
        "comment_ingestion_runs": (
            "id",
            "started_at",
            "finished_at",
            "mode",
            "status",
            "post_limit",
            "target_limit",
            "delay_seconds",
            "error",
            "notes",
        ),
        "comment_publication_status": (
            "domain",
            "publication_substack_id",
            "status",
            "attempts",
            "last_attempt_at",
            "next_retry_at",
            "last_success_at",
            "posts_seen",
            "posts_created",
            "posts_updated",
            "users_seen",
            "users_created",
            "users_updated",
            "comments_fetched",
            "comments_unique",
            "comments_created",
            "comments_updated",
            "last_error",
            "updated_at",
        ),
        "semantic_embedding_runs": (
            "id",
            "started_at",
            "finished_at",
            "source_table",
            "model",
            "status",
            "target_limit",
            "processed",
            "embedded",
            "skipped",
            "error",
        ),
        "semantic_embeddings": (
            "id",
            "source_table",
            "source_id",
            "source_hash",
            "model",
            "dimensions",
            "embedding_json",
            "embedded_at",
        ),
    }


def _is_current_schema(conn: sqlite3.Connection) -> bool:
    if "schema_version" not in _existing_tables(conn):
        return False

    cur = conn.cursor()
    cur.execute("SELECT version FROM schema_version WHERE singleton = 1")
    row = cur.fetchone()
    if row is None or int(row[0]) != SCHEMA_VERSION:
        return False

    expected = expected_schema_columns()
    for table_name, columns in expected.items():
        if tuple(_column_names(conn, table_name)) != columns:
            return False
    return True


def _create_schema(conn: sqlite3.Connection) -> None:
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            version INTEGER NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS publications (
            id INTEGER PRIMARY KEY,
            substack_id TEXT UNIQUE,
            name TEXT,
            domain TEXT NOT NULL UNIQUE,
            description TEXT,
            first_seen TIMESTAMP
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS recommendations (
            id INTEGER PRIMARY KEY,
            source_domain TEXT NOT NULL,
            target_domain TEXT NOT NULL,
            UNIQUE(source_domain, target_domain)
        )
        """
    )

    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS queue (
            domain TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN {QUEUE_STATUSES}),
            depth INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            external_user_id TEXT UNIQUE,
            name TEXT,
            handle TEXT,
            profile_url TEXT,
            publication_substack_id TEXT,
            publication_role TEXT,
            is_publication_owner INTEGER NOT NULL DEFAULT 0 CHECK(is_publication_owner IN (0, 1)),
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY,
            external_post_id TEXT UNIQUE,
            publication_substack_id TEXT,
            title TEXT,
            url TEXT,
            published_at TIMESTAMP,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY,
            external_comment_id TEXT UNIQUE,
            post_id INTEGER,
            user_id INTEGER,
            parent_comment_id INTEGER,
            parent_external_comment_id TEXT,
            body TEXT,
            commented_at TIMESTAMP,
            raw_json TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE SET NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL,
            FOREIGN KEY(parent_comment_id) REFERENCES comments(id) ON DELETE SET NULL
        )
        """
    )

    c.execute("CREATE INDEX IF NOT EXISTS idx_users_publication_substack_id ON users(publication_substack_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_posts_publication_substack_id ON posts(publication_substack_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_comments_parent_comment_id ON comments(parent_comment_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_comments_parent_external_comment_id ON comments(parent_external_comment_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_comments_commented_at ON comments(commented_at)")

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS comment_ingestion_runs (
            id INTEGER PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            post_limit INTEGER NOT NULL,
            target_limit INTEGER,
            delay_seconds REAL NOT NULL DEFAULT 0,
            error TEXT,
            notes TEXT
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS comment_publication_status (
            domain TEXT PRIMARY KEY,
            publication_substack_id TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_attempt_at TIMESTAMP,
            next_retry_at TIMESTAMP,
            last_success_at TIMESTAMP,
            posts_seen INTEGER NOT NULL DEFAULT 0,
            posts_created INTEGER NOT NULL DEFAULT 0,
            posts_updated INTEGER NOT NULL DEFAULT 0,
            users_seen INTEGER NOT NULL DEFAULT 0,
            users_created INTEGER NOT NULL DEFAULT 0,
            users_updated INTEGER NOT NULL DEFAULT 0,
            comments_fetched INTEGER NOT NULL DEFAULT 0,
            comments_unique INTEGER NOT NULL DEFAULT 0,
            comments_created INTEGER NOT NULL DEFAULT 0,
            comments_updated INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS semantic_embedding_runs (
            id INTEGER PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            source_table TEXT NOT NULL,
            model TEXT NOT NULL,
            status TEXT NOT NULL,
            target_limit INTEGER,
            processed INTEGER NOT NULL DEFAULT 0,
            embedded INTEGER NOT NULL DEFAULT 0,
            skipped INTEGER NOT NULL DEFAULT 0,
            error TEXT
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS semantic_embeddings (
            id INTEGER PRIMARY KEY,
            source_table TEXT NOT NULL,
            source_id INTEGER NOT NULL,
            source_hash TEXT NOT NULL,
            model TEXT NOT NULL,
            dimensions INTEGER NOT NULL,
            embedding_json TEXT NOT NULL,
            embedded_at TIMESTAMP NOT NULL
        )
        """
    )

    c.execute("CREATE INDEX IF NOT EXISTS idx_comment_publication_status_status ON comment_publication_status(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_comment_publication_status_updated_at ON comment_publication_status(updated_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_semantic_embedding_runs_source ON semantic_embedding_runs(source_table, model, status)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_semantic_embeddings_unique_source_model ON semantic_embeddings(source_table, source_id, model)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_semantic_embeddings_hash ON semantic_embeddings(source_table, source_hash, model)")


def _migrate_legacy_schema(conn: sqlite3.Connection) -> None:
    c = conn.cursor()

    legacy = {
        name: name
        for name in CORE_TABLES
        if name in _existing_tables(conn)
    }
    for table_name in legacy.values():
        c.execute(f"ALTER TABLE {table_name} RENAME TO __legacy_{table_name}")

    _create_schema(conn)

    if "__legacy_publications" in _existing_tables(conn):
        c.execute(
            """
            INSERT INTO publications (id, substack_id, name, domain, description, first_seen)
            SELECT id, substack_id, name, domain, description, first_seen
              FROM __legacy_publications
            """
        )

    if "__legacy_recommendations" in _existing_tables(conn):
        c.execute(
            """
            INSERT INTO recommendations (id, source_domain, target_domain)
            SELECT id, source_domain, target_domain
              FROM __legacy_recommendations
             WHERE source_domain IS NOT NULL
               AND target_domain IS NOT NULL
            """
        )

    if "__legacy_queue" in _existing_tables(conn):
        c.execute(
            """
            INSERT INTO queue (domain, status, depth)
            SELECT domain,
                   CASE
                       WHEN status IN ('pending', 'crawled', 'failed') THEN status
                       ELSE 'pending'
                   END,
                   COALESCE(depth, 0)
              FROM __legacy_queue
             WHERE domain IS NOT NULL
               AND TRIM(domain) <> ''
            """
        )

    if "__legacy_users" in _existing_tables(conn):
        user_columns = set(_column_names(conn, "__legacy_users"))
        publication_column = "publication_substack_id" if "publication_substack_id" in user_columns else "publication_id"
        publication_role_expr = "publication_role" if "publication_role" in user_columns else "NULL"
        c.execute(
            f"""
            INSERT INTO users (
                id,
                external_user_id,
                name,
                handle,
                profile_url,
                publication_substack_id,
                publication_role,
                is_publication_owner,
                first_seen,
                last_seen
            )
            SELECT
                id,
                external_user_id,
                name,
                handle,
                profile_url,
                CAST({publication_column} AS TEXT),
                {publication_role_expr},
                COALESCE(is_publication_owner, 0),
                first_seen,
                last_seen
              FROM __legacy_users
            """
        )

    if "__legacy_posts" in _existing_tables(conn):
        post_columns = set(_column_names(conn, "__legacy_posts"))
        publication_column = "publication_substack_id" if "publication_substack_id" in post_columns else "publication_id"
        c.execute(
            f"""
            INSERT INTO posts (
                id,
                external_post_id,
                publication_substack_id,
                title,
                url,
                published_at,
                first_seen,
                last_seen
            )
            SELECT
                id,
                external_post_id,
                CAST({publication_column} AS TEXT),
                title,
                url,
                published_at,
                first_seen,
                last_seen
              FROM __legacy_posts
            """
        )

    if "__legacy_comments" in _existing_tables(conn):
        comment_columns = set(_column_names(conn, "__legacy_comments"))
        parent_external_expr = (
            "c.parent_external_comment_id"
            if "parent_external_comment_id" in comment_columns
            else "parent.external_comment_id"
        )
        c.execute(
            f"""
            INSERT INTO comments (
                id,
                external_comment_id,
                post_id,
                user_id,
                parent_comment_id,
                parent_external_comment_id,
                body,
                commented_at,
                raw_json,
                first_seen,
                last_seen
            )
            SELECT
                c.id,
                c.external_comment_id,
                c.post_id,
                c.user_id,
                c.parent_comment_id,
                {parent_external_expr},
                c.body,
                c.commented_at,
                c.raw_json,
                c.first_seen,
                c.last_seen
              FROM __legacy_comments c
              LEFT JOIN __legacy_comments parent
                ON parent.id = c.parent_comment_id
            """
        )

    for table_name in CORE_TABLES:
        if f"__legacy_{table_name}" in _existing_tables(conn):
            c.execute(f"DROP TABLE __legacy_{table_name}")

    _set_schema_version(conn, SCHEMA_VERSION)


def _read_schema_version(conn: sqlite3.Connection, existing_tables: set[str] | None = None) -> int | None:
    tables = existing_tables if existing_tables is not None else _existing_tables(conn)
    if "schema_version" not in tables:
        return None
    row = conn.execute("SELECT version FROM schema_version WHERE singleton = 1").fetchone()
    if row is None:
        return None
    return int(row[0])


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        """
        INSERT INTO schema_version (singleton, version, updated_at)
        VALUES (1, ?, ?)
        ON CONFLICT(singleton) DO UPDATE SET
            version = excluded.version,
            updated_at = excluded.updated_at
        """,
        (version, _now_iso()),
    )


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in cur.fetchall()}


def _column_names(conn: sqlite3.Connection, table_name: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
