import os
import tempfile
import unittest

import requests

from scripts.comments.user_classifier import classify_profile, classify_users
from scripts.milestone01.crawl import SubstackNetworkCrawler


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, route_payloads):
        self.route_payloads = {k: list(v) for k, v in route_payloads.items()}

    def get(self, url, **_kwargs):
        queued = self.route_payloads.get(url, [])
        if not queued:
            return _FakeResponse(status_code=404, payload={})
        next_item = queued.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        if isinstance(next_item, tuple):
            status_code, payload = next_item
            return _FakeResponse(status_code=status_code, payload=payload)
        return _FakeResponse(payload=next_item)

    def close(self):
        return None


class TestUserClassifier(unittest.TestCase):
    def test_classify_profile_admin_and_has_posts_is_owner(self):
        profile = {
            "id": 10,
            "hasPosts": True,
            "primaryPublication": {"id": 101, "author_id": 10},
            "publicationUsers": [
                {"user_id": 10, "publication_id": 101, "role": "admin"},
            ],
        }
        outcome = classify_profile(profile)
        self.assertEqual("101", outcome["publication_substack_id"])
        self.assertEqual("admin", outcome["publication_role"])
        self.assertEqual(1, outcome["is_publication_owner"])

    def test_classify_profile_publication_without_posts_is_not_owner(self):
        profile = {
            "id": 11,
            "hasPosts": False,
            "primaryPublication": {"id": 202, "author_id": 11},
            "publicationUsers": [
                {"user_id": 11, "publication_id": 202, "role": "admin"},
            ],
        }
        outcome = classify_profile(profile)
        self.assertEqual("202", outcome["publication_substack_id"])
        self.assertEqual("admin", outcome["publication_role"])
        self.assertEqual(0, outcome["is_publication_owner"])

    def test_classify_profile_requires_admin_role_not_primary_author_only(self):
        profile = {
            "id": 15,
            "hasPosts": True,
            "primaryPublication": {"id": 303, "author_id": 15},
            "publicationUsers": [
                {"user_id": 15, "publication_id": 303, "role": "contributor"},
            ],
        }
        outcome = classify_profile(profile)
        self.assertEqual("303", outcome["publication_substack_id"])
        self.assertEqual("contributor", outcome["publication_role"])
        self.assertEqual(0, outcome["is_publication_owner"])

    def test_classify_users_updates_db_and_fail_open_on_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "classify-users.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            conn = crawler.conn
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO users (
                    external_user_id, name, handle, profile_url,
                    publication_substack_id, publication_role, is_publication_owner
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("u-owner", "Owner", "ownerhandle", None, None, None, 0),
            )
            owner_row_id = cur.lastrowid
            cur.execute(
                """
                INSERT INTO users (
                    external_user_id, name, handle, profile_url,
                    publication_substack_id, publication_role, is_publication_owner
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("u-shell", "Shell", "shellhandle", None, None, None, 0),
            )
            shell_row_id = cur.lastrowid
            cur.execute(
                """
                INSERT INTO users (
                    external_user_id, name, handle, profile_url,
                    publication_substack_id, publication_role, is_publication_owner
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("u-bad", "Bad", "brokenhandle", None, None, None, 0),
            )
            broken_row_id = cur.lastrowid
            conn.commit()

            session = _FakeSession(
                {
                    "https://substack.com/api/v1/user/ownerhandle/public_profile": [
                        {
                            "id": 1001,
                            "hasPosts": True,
                            "primaryPublication": {"id": 5001, "author_id": 1001},
                            "publicationUsers": [
                                {
                                    "user_id": 1001,
                                    "publication_id": 5001,
                                    "role": "admin",
                                    "publication": {"id": 5001},
                                }
                            ],
                        }
                    ],
                    "https://substack.com/api/v1/user/shellhandle/public_profile": [
                        {
                            "id": 1002,
                            "hasPosts": False,
                            "primaryPublication": {"id": 5002, "author_id": 1002},
                            "publicationUsers": [
                                {
                                    "user_id": 1002,
                                    "publication_id": 5002,
                                    "role": "admin",
                                    "publication": {"id": 5002},
                                }
                            ],
                        }
                    ],
                    "https://substack.com/api/v1/user/brokenhandle/public_profile": [
                        requests.Timeout("timeout-like failure")
                    ],
                }
            )

            stats = classify_users(
                conn,
                user_ids=[owner_row_id, shell_row_id, broken_row_id],
                max_users=75,
                workers=1,
                timeout=1.0,
                retries=1,
                session=session,
            )

            self.assertEqual(3, stats["candidate_users"])
            self.assertEqual(3, stats["attempted_handles"])
            self.assertEqual(2, stats["resolved_profiles"])
            self.assertEqual(2, stats["updated_users"])
            self.assertEqual(1, stats["owner_users"])

            cur.execute(
                "SELECT publication_substack_id, publication_role, is_publication_owner FROM users WHERE id = ?",
                (owner_row_id,),
            )
            self.assertEqual(("5001", "admin", 1), cur.fetchone())

            cur.execute(
                "SELECT publication_substack_id, publication_role, is_publication_owner FROM users WHERE id = ?",
                (shell_row_id,),
            )
            self.assertEqual(("5002", "admin", 0), cur.fetchone())

            cur.execute(
                "SELECT publication_substack_id, publication_role, is_publication_owner FROM users WHERE id = ?",
                (broken_row_id,),
            )
            self.assertEqual((None, None, 0), cur.fetchone())

    def test_classify_users_honors_input_order_for_max_users_cap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "classify-order.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            conn = crawler.conn
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO users (
                    external_user_id, name, handle, profile_url,
                    publication_substack_id, publication_role, is_publication_owner
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("u-owner", "Owner", "ownerhandle", None, None, None, 0),
            )
            owner_row_id = cur.lastrowid
            cur.execute(
                """
                INSERT INTO users (
                    external_user_id, name, handle, profile_url,
                    publication_substack_id, publication_role, is_publication_owner
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("u-shell", "Shell", "shellhandle", None, None, None, 0),
            )
            shell_row_id = cur.lastrowid
            conn.commit()

            session = _FakeSession(
                {
                    "https://substack.com/api/v1/user/ownerhandle/public_profile": [
                        {
                            "id": 1001,
                            "hasPosts": True,
                            "primaryPublication": {"id": 5001, "author_id": 1001},
                            "publicationUsers": [
                                {"user_id": 1001, "publication_id": 5001, "role": "admin"}
                            ],
                        }
                    ],
                    "https://substack.com/api/v1/user/shellhandle/public_profile": [
                        {
                            "id": 1002,
                            "hasPosts": False,
                            "primaryPublication": {"id": 5002, "author_id": 1002},
                            "publicationUsers": [
                                {"user_id": 1002, "publication_id": 5002, "role": "admin"}
                            ],
                        }
                    ],
                }
            )

            stats = classify_users(
                conn,
                user_ids=[shell_row_id, owner_row_id],
                max_users=1,
                workers=1,
                timeout=1.0,
                retries=1,
                session=session,
            )

            self.assertEqual(1, stats["attempted_handles"])

            cur.execute(
                "SELECT publication_substack_id, is_publication_owner FROM users WHERE id = ?",
                (shell_row_id,),
            )
            self.assertEqual(("5002", 0), cur.fetchone())

            cur.execute(
                "SELECT publication_substack_id, is_publication_owner FROM users WHERE id = ?",
                (owner_row_id,),
            )
            self.assertEqual((None, 0), cur.fetchone())


if __name__ == "__main__":
    unittest.main()
