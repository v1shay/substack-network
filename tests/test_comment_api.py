import unittest

import requests

from scripts.comments.comment_api import CommentAPIFetchError, fetch_post_comments


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, json_error=False):
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self):
        if self._json_error:
            raise ValueError("bad json")
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        current = self.responses.pop(0)
        if isinstance(current, Exception):
            raise current
        return current

    def close(self):
        return None


class TestCommentAPI(unittest.TestCase):
    def test_comment_pagination(self):
        session = _FakeSession(
            [
                _FakeResponse(payload={"comments": [{"id": "c1"}], "has_more": True, "next_offset": 1}),
                _FakeResponse(payload={"comments": [{"id": "c2"}], "has_more": False}),
            ]
        )
        comments = fetch_post_comments(
            "example.substack.com",
            post_id="p1",
            max_pages=5,
            session=session,
        )
        self.assertEqual(["c1", "c2"], [c["id"] for c in comments])
        self.assertTrue(session.calls[0][1]["allow_redirects"])
        self.assertTrue(session.calls[0][0].endswith("/api/v1/post/p1/comments"))

    def test_comment_endpoint_compatibility_fallback(self):
        session = _FakeSession(
            [
                _FakeResponse(status_code=404),
                _FakeResponse(payload={"comments": [{"id": "legacy-c1"}], "has_more": False}),
            ]
        )
        comments = fetch_post_comments(
            "example.substack.com",
            post_id="p1",
            retries=1,
            session=session,
        )
        self.assertEqual(["legacy-c1"], [c["id"] for c in comments])
        self.assertTrue(session.calls[0][0].endswith("/api/v1/post/p1/comments"))
        self.assertTrue(session.calls[1][0].endswith("/api/v1/posts/p1/comments"))

    def test_repeated_comment_pages_stop_fetch_and_dedupe(self):
        repeated_payload = {
            "comments": [{"id": "c1"}, {"id": "c2"}],
            "has_more": True,
            "next_offset": 2,
        }
        session = _FakeSession(
            [
                _FakeResponse(payload=repeated_payload),
                _FakeResponse(payload=repeated_payload),
            ]
        )
        comments = fetch_post_comments("https://example.substack.com", "p1", session=session)
        self.assertEqual(["c1", "c2"], [c["id"] for c in comments])
        self.assertEqual(2, len(session.calls))

    def test_duplicate_comments_on_next_page_stop_when_no_new_items(self):
        session = _FakeSession(
            [
                _FakeResponse(payload={"comments": [{"id": "c1"}], "has_more": True, "next_offset": 1}),
                _FakeResponse(payload={"comments": [{"id": "c1"}], "has_more": True, "next_offset": 2}),
            ]
        )
        comments = fetch_post_comments("https://example.substack.com", "p1", session=session)
        self.assertEqual(["c1"], [c["id"] for c in comments])
        self.assertEqual(2, len(session.calls))

    def test_deleted_and_nested_payload_is_returned_without_crash(self):
        payload = {
            "comments": [
                {
                    "id": "parent",
                    "deleted": True,
                    "children": [{"id": "child", "body": "nested"}],
                }
            ],
            "has_more": False,
        }
        session = _FakeSession([_FakeResponse(payload=payload)])
        comments = fetch_post_comments("https://example.substack.com", "p1", session=session)
        self.assertEqual(1, len(comments))
        self.assertTrue(comments[0]["deleted"])

    def test_terminal_comment_fetch_failure_raises_fetch_error(self):
        session = _FakeSession(
            [
                requests.Timeout("boom"),
                requests.Timeout("still-boom"),
            ]
        )
        with self.assertRaises(CommentAPIFetchError):
            fetch_post_comments("https://example.substack.com", "p1", retries=1, session=session)

    def test_malformed_response_raises_fetch_error(self):
        session = _FakeSession(
            [
                _FakeResponse(json_error=True),
                _FakeResponse(json_error=True),
            ]
        )
        with self.assertRaises(CommentAPIFetchError):
            fetch_post_comments("https://example.substack.com", "p1", session=session)


if __name__ == "__main__":
    unittest.main()
