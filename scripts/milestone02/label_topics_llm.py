#!/usr/bin/env python3
"""
Assign topic labels using an LLM and a fixed list of labels you choose.

Reads publications (domain, name, description) from cartographer.db, calls an
OpenAI-compatible API with a short prompt, and writes the chosen label to
publication_topics.topic_llm.

Requires: OPENAI_API_KEY (or similar) and a client lib (openai). Install: pip install openai

Usage (from repo root):
    python scripts/milestone02/label_topics_llm.py
    python scripts/milestone02/label_topics_llm.py --labels "tech,politics,opinion,culture,other"
    python scripts/milestone02/label_topics_llm.py --dry-run  # print only, no API or DB writes
"""

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.db_runtime import connect_db

# Add topic_labels_db to path when running as script
sys.path.insert(0, str(Path(__file__).resolve().parent))
from topic_labels_db import ensure_publication_topics_table

DEFAULT_LABELS = "tech, politics, opinion, culture wars, business, culture, other"


def main() -> None:
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    parser = argparse.ArgumentParser(description="Label publications by topic using an LLM and a fixed label list.")
    parser.add_argument("--db", type=str, default=None, help="Path to cartographer.db")
    parser.add_argument(
        "--labels",
        type=str,
        default=DEFAULT_LABELS,
        help=f"Comma-separated list of allowed labels (default: {DEFAULT_LABELS!r})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print what would be sent; no API or DB writes")
    parser.add_argument("--check-config", action="store_true", help="Validate DB, labels, API key, and OpenAI client import without making API calls")
    parser.add_argument("--limit", type=int, default=0, metavar="N", help="Process only N publications (0 = all)")
    args = parser.parse_args()
    if args.db:
        db_path = Path(args.db).resolve()

    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    labels_list = [s.strip() for s in args.labels.split(",") if s.strip()]
    if not labels_list:
        print("At least one label required.", file=sys.stderr)
        sys.exit(1)

    if args.check_config:
        rows = _load_publications(db_path, limit=args.limit, writable=False)
        missing = []
        if not os.environ.get("OPENAI_API_KEY"):
            missing.append("OPENAI_API_KEY")
        try:
            import openai  # noqa: F401
        except ImportError:
            missing.append("openai package")
        if missing:
            print("Missing config: " + ", ".join(missing), file=sys.stderr)
            sys.exit(1)
        print(f"Config OK: DB={db_path} rows={len(rows)} labels={labels_list}")
        return

    conn = connect_db(db_path)
    ensure_publication_topics_table(conn)
    cur = conn.cursor()
    cur.execute("SELECT domain, name, description FROM publications ORDER BY domain")
    rows = cur.fetchall()
    if args.limit:
        rows = rows[: args.limit]

    if args.dry_run:
        print(f"Dry run: would process {len(rows)} publications with labels: {labels_list}")
        for domain, name, desc in rows[:5]:
            print(f"  {domain!r}  name={name!r}  desc={desc[:50] if desc else None!r}...")
        if len(rows) > 5:
            print(f"  ... and {len(rows) - 5} more")
        conn.close()
        return

    try:
        import openai
    except ImportError:
        print("pip install openai", file=sys.stderr)
        sys.exit(1)

    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY is required to run label_topics_llm.py (or use --dry-run).", file=sys.stderr)
        sys.exit(1)

    try:
        client = openai.OpenAI()
    except Exception as exc:
        print(f"Could not initialize OpenAI client: {exc}", file=sys.stderr)
        sys.exit(1)
    labels_str = ", ".join(labels_list)
    updated = 0
    for i, (domain, name, desc) in enumerate(rows):
        name = (name or "").strip() or domain
        desc = (desc or "").strip() or ""
        text = f"Publication: {name}\nDomain: {domain}\nDescription: {desc}" if desc else f"Publication: {name}\nDomain: {domain}"

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": f"""Assign exactly one topic from this list: {labels_str}.

{text}

Reply with only the single topic word or phrase from the list, nothing else.""",
                }
            ],
            max_tokens=20,
        )
        choice = response.choices[0]
        topic = (choice.message.content or "").strip()
        if not topic:
            topic = labels_list[-1]  # fallback to last (e.g. "other")
        # Normalize: pick first matching label (case-insensitive)
        for L in labels_list:
            if L.lower() in topic.lower() or topic.lower() in L.lower():
                topic = L
                break

        cur.execute(
            """INSERT INTO publication_topics (domain, topic_llm, topic_cluster) VALUES (?, ?, NULL)
               ON CONFLICT(domain) DO UPDATE SET topic_llm = excluded.topic_llm""",
            (domain, topic),
        )
        updated += 1
        if (i + 1) % 10 == 0:
            conn.commit()
            print(f"  {i + 1}/{len(rows)} …", flush=True)
        time.sleep(0.2)  # rate limit

    conn.commit()
    conn.close()
    print(f"Done. Updated topic_llm for {updated} publications.")


def _load_publications(db_path: Path, *, limit: int, writable: bool) -> list[tuple[str, str | None, str | None]]:
    if writable:
        conn = connect_db(db_path)
    else:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT domain, name, description FROM publications ORDER BY domain")
        rows = cur.fetchall()
        if limit:
            return rows[:limit]
        return rows
    finally:
        conn.close()


if __name__ == "__main__":
    main()
