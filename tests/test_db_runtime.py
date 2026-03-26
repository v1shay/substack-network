import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.comments.db_audit import audit_db
from scripts.db_runtime import connect_db, ensure_schema, expected_schema_columns, schema_is_current


class TestDBRuntime(unittest.TestCase):
    def test_legacy_schema_migrates_to_current_layout(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "legacy.db"
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE publications (
                    id INTEGER PRIMARY KEY,
                    substack_id TEXT UNIQUE,
                    name TEXT,
                    domain TEXT NOT NULL UNIQUE,
                    description TEXT,
                    first_seen TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE recommendations (
                    id INTEGER PRIMARY KEY,
                    source_domain TEXT NOT NULL,
                    target_domain TEXT NOT NULL,
                    UNIQUE(source_domain, target_domain)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE queue (
                    domain TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'pending',
                    depth INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    external_user_id TEXT UNIQUE,
                    name TEXT,
                    handle TEXT,
                    profile_url TEXT,
                    publication_id INTEGER,
                    is_publication_owner INTEGER NOT NULL DEFAULT 0,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE posts (
                    id INTEGER PRIMARY KEY,
                    external_post_id TEXT UNIQUE,
                    publication_id INTEGER,
                    title TEXT,
                    url TEXT,
                    published_at TIMESTAMP,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE comments (
                    id INTEGER PRIMARY KEY,
                    external_comment_id TEXT UNIQUE,
                    post_id INTEGER,
                    user_id INTEGER,
                    parent_comment_id INTEGER,
                    body TEXT,
                    commented_at TIMESTAMP,
                    raw_json TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                INSERT INTO publications (id, substack_id, name, domain, description, first_seen)
                VALUES (1, '11', 'Example', 'example.substack.com', 'desc', '2026-01-01T00:00:00Z')
                """
            )
            cur.execute(
                """
                INSERT INTO queue (domain, status, depth)
                VALUES ('example.substack.com', 'crawled', 0)
                """
            )
            cur.execute(
                """
                INSERT INTO users (id, external_user_id, name, handle, profile_url, publication_id, is_publication_owner, first_seen, last_seen)
                VALUES (1, 'user-1', 'User', 'user', NULL, 500, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            )
            cur.execute(
                """
                INSERT INTO posts (id, external_post_id, publication_id, title, url, published_at, first_seen, last_seen)
                VALUES (1, 'post-1', 500, 'Post', 'https://example.substack.com/p/post-1', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            )
            cur.execute(
                """
                INSERT INTO comments (id, external_comment_id, post_id, user_id, parent_comment_id, body, commented_at, raw_json, first_seen, last_seen)
                VALUES (1, 'parent', 1, 1, NULL, 'parent', '2026-01-01T00:00:00Z', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            )
            cur.execute(
                """
                INSERT INTO comments (id, external_comment_id, post_id, user_id, parent_comment_id, body, commented_at, raw_json, first_seen, last_seen)
                VALUES (2, 'child', 1, 1, 1, 'child', '2026-01-01T00:00:00Z', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            )
            conn.commit()
            conn.close()

            migrated = connect_db(db_path)
            ensure_schema(migrated)

            self.assertTrue(schema_is_current(migrated))
            self.assertEqual(1, migrated.execute("PRAGMA foreign_keys").fetchone()[0])

            for table_name, columns in expected_schema_columns().items():
                info = migrated.execute(f"PRAGMA table_info({table_name})").fetchall()
                self.assertEqual(columns, tuple(row[1] for row in info))

            row = migrated.execute(
                """
                SELECT publication_substack_id, publication_role, is_publication_owner
                  FROM users
                 WHERE id = 1
                """
            ).fetchone()
            self.assertEqual(("500", None, 1), row)

            row = migrated.execute(
                """
                SELECT publication_substack_id
                  FROM posts
                 WHERE id = 1
                """
            ).fetchone()
            self.assertEqual(("500",), row)

            row = migrated.execute(
                """
                SELECT parent_comment_id, parent_external_comment_id
                  FROM comments
                 WHERE id = 2
                """
            ).fetchone()
            self.assertEqual((1, "parent"), row)
            migrated.close()

    def test_audit_db_migrates_legacy_schema_before_counting(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "legacy-audit.db"
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE publications (
                    id INTEGER PRIMARY KEY,
                    substack_id TEXT UNIQUE,
                    name TEXT,
                    domain TEXT NOT NULL UNIQUE,
                    description TEXT,
                    first_seen TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE recommendations (
                    id INTEGER PRIMARY KEY,
                    source_domain TEXT NOT NULL,
                    target_domain TEXT NOT NULL,
                    UNIQUE(source_domain, target_domain)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE queue (
                    domain TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'pending',
                    depth INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    external_user_id TEXT UNIQUE,
                    name TEXT,
                    handle TEXT,
                    profile_url TEXT,
                    publication_id INTEGER,
                    is_publication_owner INTEGER NOT NULL DEFAULT 0,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE posts (
                    id INTEGER PRIMARY KEY,
                    external_post_id TEXT UNIQUE,
                    publication_id INTEGER,
                    title TEXT,
                    url TEXT,
                    published_at TIMESTAMP,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE comments (
                    id INTEGER PRIMARY KEY,
                    external_comment_id TEXT UNIQUE,
                    post_id INTEGER,
                    user_id INTEGER,
                    parent_comment_id INTEGER,
                    body TEXT,
                    commented_at TIMESTAMP,
                    raw_json TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()
            conn.close()

            counts = audit_db(db_path)
            self.assertEqual(0, counts["schema_version_missing"])
            self.assertEqual(0, counts["schema_drift_tables"])
            self.assertEqual(0, counts["comments_broken_parent_external_comment_id"])

            migrated = connect_db(db_path)
            self.assertTrue(schema_is_current(migrated))
            migrated.close()


if __name__ == "__main__":
    unittest.main()
