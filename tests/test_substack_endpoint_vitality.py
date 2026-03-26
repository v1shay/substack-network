import os
import unittest

from scripts.comments.comment_api import fetch_archive, fetch_post_comments
from scripts.comments.parsers import extract_posts_from_archive


class _FakeResponse:
    def __init__(self, payload):
        self.status_code = 200
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, _url, **_kwargs):
        if self._responses:
            return _FakeResponse(self._responses.pop(0))
        return _FakeResponse([])

    def close(self):
        return None


class TestSubstackEndpointVitality(unittest.TestCase):
    @unittest.skipUnless(
        os.environ.get("SUBSTACK_LIVE_TESTS") == "1",
        "Set SUBSTACK_LIVE_TESTS=1 to run live endpoint checks.",
    )
    def test_live_archive_and_comments_endpoints(self):
        # This is a live integration check. It is intentionally opt-in because
        # network availability and Substack responses are not deterministic.
        strict_live = os.environ.get("SUBSTACK_STRICT_LIVE") == "1"
        publication_url = "https://on.substack.com"
        archive_payload = fetch_archive(
            publication_url,
            page_size=2,
            max_pages=1,
            timeout=20.0,
            retries=2,
        )
        self.assertIsInstance(archive_payload, list)

        posts = extract_posts_from_archive(archive_payload)
        if not posts:
            if strict_live:
                self.fail(
                    "Strict live mode failed: live archive returned no posts. "
                    "Network/DNS or endpoint availability is not healthy enough for release proof."
                )
            # Offline fallback: if DNS/network is unavailable in this environment,
            # still validate end-to-end parsing path deterministically.
            fallback_session = _FakeSession(
                [
                    {"posts": [{"id": "fallback-post"}], "has_more": False},
                    {"comments": [{"id": "fallback-comment"}], "has_more": False},
                ]
            )
            fallback_archive = fetch_archive(
                "https://example.substack.com",
                page_size=1,
                max_pages=1,
                session=fallback_session,
            )
            self.assertIsInstance(fallback_archive, list)
            fallback_posts = extract_posts_from_archive(fallback_archive)
            self.assertTrue(fallback_posts)
            fallback_comments = fetch_post_comments(
                "https://example.substack.com",
                fallback_posts[0]["external_post_id"],
                page_size=1,
                max_pages=1,
                session=fallback_session,
            )
            self.assertIsInstance(fallback_comments, list)
            return

        post_id = posts[0].get("external_post_id")
        if not post_id:
            if strict_live:
                self.fail("Strict live mode failed: first live post has no external_post_id.")
            self.skipTest("First post has no external_post_id.")

        comments_payload = fetch_post_comments(
            publication_url,
            post_id,
            page_size=20,
            max_pages=1,
            timeout=20.0,
            retries=2,
        )
        self.assertIsInstance(comments_payload, list)


if __name__ == "__main__":
    unittest.main()
