import os
import tempfile
import unittest

from scripts.comments.db_helpers import (
    insert_comment_if_not_exists,
    insert_post_if_not_exists,
    insert_user_if_not_exists,
    resolve_comment_parent_links,
)
from scripts.milestone01.crawl import SubstackNetworkCrawler


class TestCommentDBIntegrity(unittest.TestCase):
    def test_idempotent_inserts_return_semantic_actions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "comments.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            conn = crawler.conn

            user = {
                "external_user_id": "u1",
                "name": "User One",
                "handle": "userone",
                "profile_url": "https://example.substack.com/people/u1",
                "publication_substack_id": None,
                "publication_role": None,
                "is_publication_owner": 0,
            }
            u1 = insert_user_if_not_exists(conn, user)
            u2 = insert_user_if_not_exists(conn, user)
            self.assertEqual("created", u1.action)
            self.assertEqual("unchanged", u2.action)
            self.assertEqual(u1.row_id, u2.row_id)

            post = {
                "external_post_id": "p1",
                "publication_substack_id": None,
                "title": "Post",
                "url": "https://example.substack.com/p/post",
                "published_at": "2025-01-01T00:00:00Z",
            }
            p1 = insert_post_if_not_exists(conn, post)
            p2 = insert_post_if_not_exists(conn, post)
            self.assertEqual("created", p1.action)
            self.assertEqual("unchanged", p2.action)
            self.assertEqual(p1.row_id, p2.row_id)

            comment = {
                "external_comment_id": "c1",
                "body": "Hello",
                "commented_at": "2025-01-02T00:00:00Z",
                "raw_json": "{}",
            }
            c1 = insert_comment_if_not_exists(conn, comment, post_id=p1.row_id, user_id=u1.row_id)
            c2 = insert_comment_if_not_exists(conn, comment, post_id=p1.row_id, user_id=u1.row_id)
            self.assertEqual("created", c1.action)
            self.assertEqual("unchanged", c2.action)
            self.assertEqual(c1.row_id, c2.row_id)

            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            self.assertEqual(1, cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM posts")
            self.assertEqual(1, cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM comments")
            self.assertEqual(1, cur.fetchone()[0])
            conn.close()

    def test_parent_child_comment_linking_from_external_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "comments.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            conn = crawler.conn

            post_id = insert_post_if_not_exists(
                conn,
                {
                    "external_post_id": "p2",
                    "publication_substack_id": None,
                    "title": "Post 2",
                    "url": "https://example.substack.com/p/post-2",
                    "published_at": None,
                },
            ).row_id
            user_id = insert_user_if_not_exists(
                conn,
                {
                    "external_user_id": "u2",
                    "name": "User Two",
                    "handle": "usertwo",
                    "profile_url": None,
                    "publication_substack_id": None,
                    "publication_role": None,
                    "is_publication_owner": 0,
                },
            ).row_id

            child_id = insert_comment_if_not_exists(
                conn,
                {
                    "external_comment_id": "child",
                    "parent_external_comment_id": "parent",
                    "body": "child",
                    "commented_at": None,
                    "raw_json": "{}",
                },
                post_id=post_id,
                user_id=user_id,
            ).row_id
            parent_id = insert_comment_if_not_exists(
                conn,
                {
                    "external_comment_id": "parent",
                    "body": "parent",
                    "commented_at": None,
                    "raw_json": "{}",
                },
                post_id=post_id,
                user_id=user_id,
            ).row_id

            self.assertEqual(1, resolve_comment_parent_links(conn))

            cur = conn.cursor()
            cur.execute("SELECT parent_comment_id FROM comments WHERE id = ?", (child_id,))
            self.assertEqual(parent_id, cur.fetchone()[0])
            conn.close()


if __name__ == "__main__":
    unittest.main()
