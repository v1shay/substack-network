import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.comments.semantic_embeddings import collect_embedding_candidates, run_embedding_batch
from scripts.db_runtime import ensure_schema


class TestSemanticEmbeddings(unittest.TestCase):
    def test_embedding_batch_dedupes_by_source_hash_and_model(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = sqlite3.connect(Path(tmpdir) / "semantic.db")
            ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO comments (external_comment_id, body, commented_at, first_seen, last_seen)
                VALUES ('c1', 'A useful comment', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO comments (external_comment_id, body, commented_at, first_seen, last_seen)
                VALUES ('c2', '', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            )
            conn.commit()

            result = run_embedding_batch(
                conn,
                source_table="comments",
                model="test-model",
                limit=10,
                embedder=lambda texts, model: [[1.0, 2.0] for _ in texts],
            )
            self.assertEqual((2, 1, 1), (result.processed, result.embedded, result.skipped))

            repeat = run_embedding_batch(
                conn,
                source_table="comments",
                model="test-model",
                limit=10,
                embedder=lambda texts, model: [[3.0, 4.0] for _ in texts],
            )
            self.assertEqual((2, 0, 2), (repeat.processed, repeat.embedded, repeat.skipped))
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM semantic_embeddings").fetchone()[0])

            other_model = run_embedding_batch(
                conn,
                source_table="comments",
                model="other-model",
                limit=10,
                embedder=lambda texts, model: [[5.0, 6.0] for _ in texts],
            )
            self.assertEqual((2, 1, 1), (other_model.processed, other_model.embedded, other_model.skipped))
            self.assertEqual(2, conn.execute("SELECT COUNT(*) FROM semantic_embeddings").fetchone()[0])
            conn.close()

    def test_changed_text_is_reembedded_for_same_model(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = sqlite3.connect(Path(tmpdir) / "semantic-update.db")
            ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO comments (external_comment_id, body, commented_at, first_seen, last_seen)
                VALUES ('c1', 'before', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            )
            conn.commit()

            run_embedding_batch(
                conn,
                source_table="comments",
                model="test-model",
                limit=10,
                embedder=lambda texts, model: [[1.0]],
            )
            before_hash = conn.execute("SELECT source_hash FROM semantic_embeddings").fetchone()[0]
            conn.execute("UPDATE comments SET body = 'after' WHERE external_comment_id = 'c1'")
            conn.commit()

            candidates, skipped = collect_embedding_candidates(
                conn,
                source_table="comments",
                model="test-model",
                limit=10,
            )
            self.assertEqual((1, 0), (len(candidates), skipped))

            run_embedding_batch(
                conn,
                source_table="comments",
                model="test-model",
                limit=10,
                embedder=lambda texts, model: [[2.0]],
            )
            after_hash = conn.execute("SELECT source_hash FROM semantic_embeddings").fetchone()[0]
            self.assertNotEqual(before_hash, after_hash)
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM semantic_embeddings").fetchone()[0])
            conn.close()

    def test_embedding_provider_count_mismatch_records_failed_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = sqlite3.connect(Path(tmpdir) / "semantic-fail.db")
            ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO comments (external_comment_id, body, commented_at, first_seen, last_seen)
                VALUES ('c1', 'text', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            )
            conn.commit()

            with self.assertRaises(RuntimeError):
                run_embedding_batch(
                    conn,
                    source_table="comments",
                    model="test-model",
                    limit=10,
                    embedder=lambda texts, model: [],
                )

            run_row = conn.execute(
                "SELECT status, processed, embedded, skipped FROM semantic_embedding_runs"
            ).fetchone()
            self.assertEqual(("failed", 1, 0, 0), run_row)
            conn.close()
