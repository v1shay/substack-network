import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import requests

from scripts.comments.comment_api import CommentAPIFetchError
from scripts.comments.comment_pipeline import main, process_comments
from scripts.milestone01.crawl import SubstackNetworkCrawler

REPO_ROOT = Path(__file__).resolve().parents[1]
COMMENT_PIPELINE_SCRIPT = REPO_ROOT / "scripts" / "comments" / "comment_pipeline.py"


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, route_payloads):
        self.route_payloads = {k: list(v) for k, v in route_payloads.items()}
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        queued = self.route_payloads.get(url, [])
        if not queued:
            return _FakeResponse(payload=[])
        next_item = queued.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return _FakeResponse(payload=next_item)

    def close(self):
        return None


class TestCommentPipeline(unittest.TestCase):
    def test_comment_pipeline_script_help_bootstraps(self):
        completed = subprocess.run(
            [sys.executable, str(COMMENT_PIPELINE_SCRIPT), "--help"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )

        self.assertEqual(0, completed.returncode)
        self.assertIn("publication_url", completed.stdout)
        self.assertIn("--post-limit", completed.stdout)

    def test_main_forwards_args_and_preserves_default_db_resolution(self):
        stats = {
            "posts_seen": 2,
            "posts_created": 2,
            "users_seen": 3,
            "comments_unique": 10,
            "comments_created": 10,
            "classified_users": 2,
            "classified_owners": 1,
        }

        with patch("scripts.comments.comment_pipeline.process_comments", return_value=stats) as process_mock:
            with patch("sys.stdout", new_callable=io.StringIO) as stdout:
                rc = main(
                    [
                        "https://example.substack.com",
                        "--post-limit",
                        "5",
                        "--timeout",
                        "12.5",
                        "--retries",
                        "4",
                        "--classify-commenters",
                        "--classification-max-users",
                        "8",
                        "--classification-workers",
                        "2",
                    ]
                )

        self.assertEqual(0, rc)
        process_mock.assert_called_once_with(
            "https://example.substack.com",
            conn=None,
            post_limit=5,
            timeout=12.5,
            retries=4,
            classify_commenters=True,
            classify_max_users=8,
            classify_workers=2,
        )
        self.assertIn("Comments pipeline complete:", stdout.getvalue())
        self.assertIn("publication=https://example.substack.com", stdout.getvalue())

    def test_main_uses_explicit_db_connection_and_closes_it(self):
        stats = {
            "posts_seen": 1,
            "posts_created": 1,
            "users_seen": 1,
            "comments_unique": 1,
            "comments_created": 1,
            "classified_users": 0,
            "classified_owners": 0,
        }
        fake_conn = unittest.mock.MagicMock()

        with patch("scripts.comments.comment_pipeline.connect_db", return_value=fake_conn) as connect_mock:
            with patch("scripts.comments.comment_pipeline.process_comments", return_value=stats) as process_mock:
                rc = main(
                    [
                        "example.substack.com",
                        "--db",
                        "/tmp/cartographer-comments.db",
                    ]
                )

        self.assertEqual(0, rc)
        connect_mock.assert_called_once()
        process_mock.assert_called_once_with(
            "example.substack.com",
            conn=fake_conn,
            post_limit=20,
            timeout=15.0,
            retries=3,
            classify_commenters=False,
            classify_max_users=75,
            classify_workers=4,
        )
        fake_conn.close.assert_called_once()

    def test_main_returns_nonzero_when_process_comments_raises_fetch_error(self):
        with patch(
            "scripts.comments.comment_pipeline.process_comments",
            side_effect=CommentAPIFetchError("https://example.substack.com/api/v1/archive", "boom"),
        ):
            with patch("sys.stderr", new_callable=io.StringIO) as stderr:
                rc = main(["example.substack.com"])

        self.assertEqual(1, rc)
        self.assertIn("[comments][error]", stderr.getvalue())

    def test_main_returns_zero_for_true_empty_success(self):
        stats = {
            "posts_seen": 0,
            "posts_created": 0,
            "users_seen": 0,
            "comments_unique": 0,
            "comments_created": 0,
            "classified_users": 0,
            "classified_owners": 0,
        }

        with patch("scripts.comments.comment_pipeline.process_comments", return_value=stats):
            with patch("sys.stdout", new_callable=io.StringIO) as stdout:
                rc = main(["example.substack.com"])

        self.assertEqual(0, rc)
        self.assertIn("posts_seen=0", stdout.getvalue())

    def test_process_comments_e2e_and_parent_linking(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "pipeline.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            conn = crawler.conn

            base = "https://example.substack.com"
            session = _FakeSession(
                {
                    f"{base}/api/v1/archive": [
                        {
                            "posts": [
                                {
                                    "id": "post-1",
                                    "title": "Post One",
                                    "canonical_url": f"{base}/p/post-1",
                                    "publication_id": 10,
                                    "post_date": "2025-01-01T00:00:00Z",
                                },
                                {
                                    "id": "post-2",
                                    "title": "Post Two",
                                    "canonical_url": f"{base}/p/post-2",
                                    "publication_id": 10,
                                    "post_date": "2025-01-02T00:00:00Z",
                                },
                            ],
                            "has_more": False,
                        }
                    ],
                    f"{base}/api/v1/post/post-1/comments": [
                        {
                            "comments": [
                                {
                                    "id": "parent-1",
                                    "body": "parent",
                                    "post_id": "post-1",
                                    "user": {"id": "u-parent", "name": "Parent", "handle": "writer"},
                                    "children": [
                                        {
                                            "id": "child-1",
                                            "body": "child",
                                            "post_id": "post-1",
                                            "user": {"id": "u-parent", "name": "Parent", "handle": "writer"},
                                        }
                                    ],
                                }
                            ],
                            "has_more": False,
                        }
                    ],
                    f"{base}/api/v1/post/post-2/comments": [
                        {"comments": [], "has_more": False}
                    ],
                }
            )

            stats = process_comments(base, conn=conn, post_limit=20, session=session)
            self.assertEqual(2, stats["posts_seen"])
            self.assertEqual(2, stats["posts_created"])
            self.assertEqual(0, stats["posts_updated"])
            self.assertEqual(1, stats["users_seen"])
            self.assertEqual(1, stats["users_created"])
            self.assertEqual(0, stats["users_updated"])
            self.assertEqual(2, stats["comments_fetched"])
            self.assertEqual(2, stats["comments_unique"])
            self.assertEqual(2, stats["comments_created"])
            self.assertEqual(0, stats["comments_updated"])

            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            self.assertEqual(1, cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM posts")
            self.assertEqual(2, cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM comments")
            self.assertEqual(2, cur.fetchone()[0])

            cur.execute(
                "SELECT id FROM comments WHERE external_comment_id = ?",
                ("parent-1",),
            )
            parent_id = cur.fetchone()[0]
            cur.execute(
                "SELECT parent_comment_id FROM comments WHERE external_comment_id = ?",
                ("child-1",),
            )
            self.assertEqual(parent_id, cur.fetchone()[0])

            session_again = _FakeSession(
                {
                    f"{base}/api/v1/archive": [
                        {
                            "posts": [
                                {"id": "post-1", "canonical_url": f"{base}/p/post-1"},
                                {"id": "post-2", "canonical_url": f"{base}/p/post-2"},
                            ],
                            "has_more": False,
                        }
                    ],
                    f"{base}/api/v1/post/post-1/comments": [
                        {
                            "comments": [
                                {
                                    "id": "parent-1",
                                    "body": "parent",
                                    "post_id": "post-1",
                                    "user": {"id": "u-parent", "name": "Parent", "handle": "writer"},
                                    "children": [
                                        {
                                            "id": "child-1",
                                            "body": "child",
                                            "post_id": "post-1",
                                            "user": {"id": "u-parent", "name": "Parent", "handle": "writer"},
                                        }
                                    ],
                                }
                            ],
                            "has_more": False,
                        }
                    ],
                    f"{base}/api/v1/post/post-2/comments": [{"comments": [], "has_more": False}],
                }
            )
            stats_again = process_comments(base, conn=conn, post_limit=20, session=session_again)
            self.assertEqual(2, stats_again["posts_seen"])
            self.assertEqual(0, stats_again["posts_created"])
            self.assertEqual(0, stats_again["posts_updated"])
            self.assertEqual(1, stats_again["users_seen"])
            self.assertEqual(0, stats_again["users_created"])
            self.assertEqual(0, stats_again["users_updated"])
            self.assertEqual(2, stats_again["comments_fetched"])
            self.assertEqual(2, stats_again["comments_unique"])
            self.assertEqual(0, stats_again["comments_created"])
            self.assertEqual(0, stats_again["comments_updated"])
            conn.close()

    def test_process_comments_raises_and_rolls_back_when_comment_fetch_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "pipeline_fail_open.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            conn = crawler.conn

            base = "https://example.substack.com"
            session = _FakeSession(
                {
                    f"{base}/api/v1/archive": [
                        {
                            "posts": [
                                {
                                    "id": "post-1",
                                    "title": "Post One",
                                    "canonical_url": f"{base}/p/post-1",
                                    "publication_id": 10,
                                }
                            ],
                            "has_more": False,
                        }
                    ],
                    f"{base}/api/v1/post/post-1/comments": [requests.Timeout("timeout-like failure")],
                    f"{base}/api/v1/posts/post-1/comments": [requests.Timeout("timeout-like failure-compat")],
                }
            )

            with self.assertRaises(CommentAPIFetchError):
                process_comments(base, conn=conn, post_limit=20, retries=1, session=session)

            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM posts")
            self.assertEqual(0, cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM comments")
            self.assertEqual(0, cur.fetchone()[0])
            conn.close()

    def test_process_comments_savepoint_rolls_back_on_insert_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "pipeline_rollback.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            conn = crawler.conn

            base = "https://example.substack.com"
            session = _FakeSession(
                {
                    f"{base}/api/v1/archive": [
                        {
                            "posts": [
                                {
                                    "id": "post-1",
                                    "title": "Post One",
                                    "canonical_url": f"{base}/p/post-1",
                                    "publication_id": 10,
                                }
                            ],
                            "has_more": False,
                        }
                    ],
                    f"{base}/api/v1/post/post-1/comments": [
                        {
                            "comments": [
                                {
                                    "id": "comment-1",
                                    "body": "hello",
                                    "post_id": "post-1",
                                    "user": {"id": "u1", "name": "User One", "handle": "userone"},
                                }
                            ],
                            "has_more": False,
                        }
                    ],
                }
            )

            with patch(
                "scripts.comments.comment_pipeline.insert_comment_if_not_exists",
                side_effect=RuntimeError("boom"),
            ):
                with self.assertRaisesRegex(RuntimeError, "boom"):
                    process_comments(base, conn=conn, post_limit=20, session=session)

            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM posts")
            self.assertEqual(0, cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM users")
            self.assertEqual(0, cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM comments")
            self.assertEqual(0, cur.fetchone()[0])
            conn.close()

    def test_process_comments_with_user_classification_enabled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "pipeline_classification.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            conn = crawler.conn

            base = "https://example.substack.com"
            comment_session = _FakeSession(
                {
                    f"{base}/api/v1/archive": [
                        {
                            "posts": [
                                {
                                    "id": "post-1",
                                    "title": "Post One",
                                    "canonical_url": f"{base}/p/post-1",
                                    "publication_id": 10,
                                }
                            ],
                            "has_more": False,
                        }
                    ],
                    f"{base}/api/v1/post/post-1/comments": [
                        {
                            "comments": [
                                {
                                    "id": "comment-1",
                                    "body": "writer comment",
                                    "post_id": "post-1",
                                    "user_id": 101,
                                    "name": "Writer",
                                    "handle": "writerhandle",
                                }
                            ],
                            "has_more": False,
                        }
                    ],
                }
            )
            profile_session = _FakeSession(
                {
                    "https://substack.com/api/v1/user/writerhandle/public_profile": [
                        {
                            "id": 101,
                            "hasPosts": True,
                            "primaryPublication": {"id": 501, "author_id": 101},
                            "publicationUsers": [
                                {
                                    "user_id": 101,
                                    "publication_id": 501,
                                    "role": "admin",
                                    "publication": {"id": 501},
                                }
                            ],
                        }
                    ]
                }
            )

            stats = process_comments(
                base,
                conn=conn,
                post_limit=20,
                session=comment_session,
                classify_commenters=True,
                classify_max_users=75,
                classify_workers=1,
                classification_timeout=1.0,
                classification_retries=1,
                classification_session=profile_session,
            )
            self.assertEqual(1, stats["posts_seen"])
            self.assertEqual(1, stats["users_seen"])
            self.assertEqual(1, stats["comments_created"])
            self.assertEqual(1, stats["classified_users"])
            self.assertEqual(1, stats["classified_owners"])

            cur = conn.cursor()
            cur.execute(
                "SELECT handle, publication_substack_id, publication_role, is_publication_owner FROM users WHERE external_user_id = ?",
                ("101",),
            )
            row = cur.fetchone()
            self.assertEqual(("writerhandle", "501", "admin", 1), row)
            conn.close()


if __name__ == "__main__":
    unittest.main()
