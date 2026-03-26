import unittest
import requests

from scripts.comments.comment_api import CommentAPIFetchError, fetch_archive


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


class TestArchiveAPI(unittest.TestCase):
    def test_archive_pagination(self):
        session = _FakeSession(
            [
                _FakeResponse(payload=[{"id": "1"}, {"id": "2"}]),
                _FakeResponse(payload=[{"id": "3"}]),
            ]
        )
        posts = fetch_archive(
            "example.substack.com",
            page_size=2,
            max_pages=5,
            session=session,
        )
        self.assertEqual(3, len(posts))
        self.assertTrue(session.calls[0][1]["allow_redirects"])
        self.assertTrue(session.calls[0][0].endswith("/api/v1/archive"))

    def test_archive_empty_payload_is_successful(self):
        session = _FakeSession([_FakeResponse(payload=[])])
        posts = fetch_archive("https://example.substack.com", session=session)
        self.assertEqual([], posts)

    def test_archive_terminal_timeout_raises_fetch_error(self):
        session = _FakeSession([requests.Timeout("boom"), _FakeResponse(payload=[])])
        with self.assertRaises(CommentAPIFetchError):
            fetch_archive(
                "https://example.substack.com",
                retries=1,
                session=session,
            )

    def test_archive_malformed_json_raises_fetch_error(self):
        session = _FakeSession([_FakeResponse(json_error=True)])
        with self.assertRaises(CommentAPIFetchError):
            fetch_archive("https://example.substack.com", session=session)


if __name__ == "__main__":
    unittest.main()
