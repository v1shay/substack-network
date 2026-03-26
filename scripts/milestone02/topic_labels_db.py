"""
Shared DB schema for topic labels. Used by label_topics_llm.py and label_topics_cluster.py.
"""

import sqlite3


def ensure_publication_topics_table(conn: sqlite3.Connection) -> None:
    """Create publication_topics if it doesn't exist; add topic_auto if table existed without it."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS publication_topics (
            domain TEXT PRIMARY KEY,
            topic_llm TEXT,
            topic_auto TEXT,
            topic_cluster TEXT,
            topic_api TEXT
        )
    """)
    for col in ("topic_auto", "topic_api"):
        try:
            conn.execute(f"ALTER TABLE publication_topics ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
