import os
import tempfile
import unittest

from scripts.comments.db_audit import compute_anomaly_counts
from scripts.comments.db_repair import repair_db
from scripts.milestone01.crawl import SubstackNetworkCrawler


class TestCommentDBRepair(unittest.TestCase):
    def test_repair_resolves_deterministic_anomalies(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "repair.db")
            crawler = SubstackNetworkCrawler(db_name=db_path)
            conn = crawler.conn
            cur = conn.cursor()

            cur.execute(
                "INSERT INTO queue (domain, status, depth) VALUES (?, ?, ?)",
                ("missing-crawled.substack.com", "crawled", 1),
            )
            cur.execute(
                "INSERT INTO queue (domain, status, depth) VALUES (?, ?, ?)",
                ("known.substack.com", "failed", 1),
            )
            cur.execute(
                """
                INSERT INTO publications (substack_id, name, domain, description, first_seen)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("1", "Known", "known.substack.com", None, "2026-01-01T00:00:00Z"),
            )
            cur.execute(
                "INSERT INTO recommendations (source_domain, target_domain) VALUES (?, ?)",
                ("orphan-source.substack.com", "target.substack.com"),
            )

            cur.execute(
                """
                INSERT INTO posts (
                    external_post_id, publication_substack_id, title, url,
                    published_at, first_seen, last_seen
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "post-1",
                    "500",
                    "Post",
                    "https://example.substack.com/p/post-1",
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:00:00Z",
                ),
            )
            post_id = cur.lastrowid
            cur.execute(
                """
                INSERT INTO users (
                    external_user_id, name, handle, profile_url,
                    publication_substack_id, publication_role, is_publication_owner, first_seen, last_seen
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "user-1",
                    "User",
                    "user",
                    None,
                    None,
                    None,
                    0,
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:00:00Z",
                ),
            )
            user_id = cur.lastrowid
            cur.execute(
                """
                INSERT INTO comments (
                    external_comment_id, post_id, user_id, parent_comment_id,
                    parent_external_comment_id, body, commented_at, raw_json, first_seen, last_seen
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "parent",
                    post_id,
                    user_id,
                    None,
                    None,
                    "parent",
                    "2026-01-01T00:00:00Z",
                    "{}",
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:00:00Z",
                ),
            )
            parent_id = cur.lastrowid
            cur.execute(
                """
                INSERT INTO comments (
                    external_comment_id, post_id, user_id, parent_comment_id,
                    parent_external_comment_id, body, commented_at, raw_json, first_seen, last_seen
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "child-local",
                    post_id,
                    user_id,
                    parent_id,
                    None,
                    "child-local",
                    "2026-01-01T00:00:00Z",
                    "{}",
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:00:00Z",
                ),
            )
            cur.execute(
                """
                INSERT INTO comments (
                    external_comment_id, post_id, user_id, parent_comment_id,
                    parent_external_comment_id, body, commented_at, raw_json, first_seen, last_seen
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "child-external",
                    post_id,
                    user_id,
                    None,
                    "parent",
                    "child-external",
                    "2026-01-01T00:00:00Z",
                    "{}",
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:00:00Z",
                ),
            )
            child_external_id = cur.lastrowid
            conn.commit()

            before = compute_anomaly_counts(conn)
            self.assertEqual(0, before["schema_version_missing"])
            self.assertEqual(0, before["schema_drift_tables"])
            self.assertEqual(1, before["recommendations_orphan_source"])
            self.assertEqual(1, before["queue_crawled_without_publication"])
            self.assertEqual(1, before["queue_failed_with_publication"])

            fixes = repair_db(conn)
            self.assertEqual(0, fixes["schema_migrations_applied"])
            self.assertGreaterEqual(
                fixes["inserted_missing_publications_from_recommendation_sources"],
                1,
            )
            self.assertGreaterEqual(
                fixes["inserted_missing_publications_from_crawled_queue"],
                1,
            )
            self.assertGreaterEqual(
                fixes["queue_failed_promoted_to_crawled_with_existing_publication"],
                1,
            )
            self.assertGreaterEqual(fixes["comments_parent_external_backfilled_from_parent_id"], 1)
            self.assertGreaterEqual(
                fixes["comments_parent_comment_links_resolved_from_external_id"],
                1,
            )

            after = compute_anomaly_counts(conn)
            self.assertEqual(0, after["recommendations_orphan_source"])
            self.assertEqual(0, after["queue_crawled_without_publication"])
            self.assertEqual(0, after["queue_failed_with_publication"])

            cur.execute(
                "SELECT parent_external_comment_id FROM comments WHERE external_comment_id = ?",
                ("child-local",),
            )
            self.assertEqual("parent", cur.fetchone()[0])
            cur.execute(
                "SELECT parent_comment_id FROM comments WHERE id = ?",
                (child_external_id,),
            )
            self.assertEqual(parent_id, cur.fetchone()[0])
            conn.close()


if __name__ == "__main__":
    unittest.main()
