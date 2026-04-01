import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts.milestone01.crawl import SubstackNetworkCrawler


class _FakeNewsletter:
    def __init__(self, url: str):
        self.url = url

    def get_recommendations(self):
        return []


class TestCrawlCommentIntegration(unittest.TestCase):
    def _run_single_publication_crawl(
        self,
        *,
        enable_comments: bool,
        classify_commenters: bool = False,
        classification_max_users: int = 10,
        classification_workers: int = 1,
        process_comments_side_effect=None,
        process_comments_return=None,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "crawl-comments.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            expected_conn = crawler.conn
            crawler.add_to_queue("alpha", 0)

            output = io.StringIO()
            with patch("scripts.milestone01.crawl.Newsletter", _FakeNewsletter):
                with patch.object(
                    SubstackNetworkCrawler,
                    "get_publication_info",
                    return_value={"id": "pub-1", "name": "Alpha", "hero_text": "desc"},
                ):
                    with patch(
                        "scripts.comments.comment_pipeline.process_comments",
                        side_effect=process_comments_side_effect,
                        return_value=process_comments_return,
                    ) as mocked_process_comments:
                        with redirect_stdout(output):
                            crawler.crawl(
                                max_publications=1,
                                delay=0,
                                enable_comments=enable_comments,
                                comment_post_limit=7,
                                comment_timeout=4.5,
                                comment_retries=2,
                                classify_commenters=classify_commenters,
                                classification_max_users=classification_max_users,
                                classification_workers=classification_workers,
                            )

            conn = sqlite3.connect(db_path)
            status = conn.execute(
                "SELECT status FROM queue WHERE domain = ?",
                ("alpha",),
            ).fetchone()[0]
            conn.close()
            expected_conn.close()

            return mocked_process_comments, output.getvalue(), status, expected_conn

    def test_comments_run_when_enabled_and_stats_are_logged(self):
        mocked_process_comments, output, status, conn = self._run_single_publication_crawl(
            enable_comments=True,
            process_comments_return={
                "posts_seen": 1,
                "posts_created": 1,
                "posts_updated": 0,
                "users_seen": 2,
                "users_created": 2,
                "users_updated": 0,
                "comments_fetched": 3,
                "comments_unique": 3,
                "comments_created": 3,
                "comments_updated": 0,
                "classified_users": 0,
                "classified_owners": 0,
            },
        )

        self.assertEqual("crawled", status)
        mocked_process_comments.assert_called_once()
        args, kwargs = mocked_process_comments.call_args
        self.assertEqual("https://alpha.substack.com", args[0])
        self.assertIs(kwargs["conn"], conn)
        self.assertEqual(7, kwargs["post_limit"])
        self.assertEqual(4.5, kwargs["timeout"])
        self.assertEqual(2, kwargs["retries"])
        self.assertFalse(kwargs["classify_commenters"])
        self.assertEqual(10, kwargs["classify_max_users"])
        self.assertEqual(1, kwargs["classify_workers"])
        self.assertIn("[comments] domain=alpha posts_seen=1", output)
        self.assertIn("users_seen=2", output)
        self.assertIn("comments_created=3", output)

    def test_comments_skipped_when_disabled(self):
        mocked_process_comments, _output, status, _conn = self._run_single_publication_crawl(
            enable_comments=False,
            process_comments_return={"posts_seen": 9},
        )

        self.assertEqual("crawled", status)
        mocked_process_comments.assert_not_called()

    def test_comment_exceptions_do_not_mark_publication_failed(self):
        _mocked_process_comments, output, status, _conn = self._run_single_publication_crawl(
            enable_comments=True,
            process_comments_side_effect=RuntimeError("boom"),
        )

        self.assertEqual("crawled", status)
        self.assertIn("[comments][error] domain=alpha: boom", output)

    def test_classification_flags_are_forwarded(self):
        mocked_process_comments, _output, status, _conn = self._run_single_publication_crawl(
            enable_comments=True,
            classify_commenters=True,
            classification_max_users=33,
            classification_workers=2,
            process_comments_return={
                "posts_seen": 1,
                "posts_created": 1,
                "posts_updated": 0,
                "users_seen": 1,
                "users_created": 1,
                "users_updated": 0,
                "comments_fetched": 1,
                "comments_unique": 1,
                "comments_created": 1,
                "comments_updated": 0,
                "classified_users": 1,
                "classified_owners": 0,
            },
        )

        self.assertEqual("crawled", status)
        mocked_process_comments.assert_called_once()
        kwargs = mocked_process_comments.call_args.kwargs
        self.assertTrue(kwargs["classify_commenters"])
        self.assertEqual(33, kwargs["classify_max_users"])
        self.assertEqual(2, kwargs["classify_workers"])


if __name__ == "__main__":
    unittest.main()
