import unittest

from scripts.comments.parsers import (
    extract_comments_from_response,
    extract_posts_from_archive,
    normalize_user,
)


class TestCommentParsing(unittest.TestCase):
    def test_extract_posts_from_archive_uses_publication_substack_id(self):
        payload = {
            "posts": [
                {
                    "id": 101,
                    "title": "A",
                    "canonical_url": "https://example.substack.com/p/a",
                    "publication_id": 7,
                    "post_date": "2025-01-01T00:00:00Z",
                }
            ]
        }
        posts = extract_posts_from_archive(payload)
        self.assertEqual(1, len(posts))
        self.assertEqual("101", posts[0]["external_post_id"])
        self.assertEqual("7", posts[0]["publication_substack_id"])

    def test_extract_comments_from_nested_tree(self):
        payload = {
            "comments": [
                {
                    "id": "p1",
                    "body": "parent",
                    "user": {"id": "u1", "name": "Parent"},
                    "children": [
                        {
                            "id": "c1",
                            "body": "child",
                            "user": {"id": "u2", "name": "Child"},
                            "children": [{"id": "g1", "deleted": True}],
                        }
                    ],
                }
            ]
        }
        comments = extract_comments_from_response(payload)
        self.assertEqual(3, len(comments))

        child = next(c for c in comments if c["external_comment_id"] == "c1")
        self.assertEqual("p1", child["parent_external_comment_id"])

        grandchild = next(c for c in comments if c["external_comment_id"] == "g1")
        self.assertEqual("c1", grandchild["parent_external_comment_id"])
        self.assertEqual("", grandchild["body"])

    def test_normalize_user_missing_fields(self):
        user = normalize_user({"body": "hello"})
        self.assertIsNone(user["external_user_id"])
        self.assertEqual(0, user["is_publication_owner"])

    def test_normalize_user_keeps_user_publication_link_only(self):
        user = normalize_user(
            {
                "user_id": 123,
                "name": "Top Level Name",
                "handle": "TopLevelHandle",
                "user_slug": "top-level-slug",
                "publication_id": 999,
            }
        )
        self.assertEqual("123", user["external_user_id"])
        self.assertEqual("Top Level Name", user["name"])
        self.assertEqual("TopLevelHandle", user["handle"])
        # Thread-level publication_id belongs to the commented post, not the commenter.
        self.assertIsNone(user["publication_substack_id"])


if __name__ == "__main__":
    unittest.main()
