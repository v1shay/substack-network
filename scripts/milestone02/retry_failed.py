#!/usr/bin/env python3
"""
Retry failed publications: fetch each queue row with status='failed' again.
If the fetch succeeds this time, add the publication and its recommendations to the DB,
set queue status to 'crawled', and record the domain in the unfailed table.

The resulting graph (nodes + edges) is the same as if those publications had not
failed in the first place: we use the existing queue depth so new recommendations
are enqueued at depth+1 as they would have been. Order of crawling differs; the
set of publications and recommendation edges does not.

Run from repo root (do not run while the main crawler is running):

    python scripts/milestone02/retry_failed.py
    python scripts/milestone02/retry_failed.py --max 50 --delay 1.5

Unfailed domains are recorded in the unfailed table (domain, unfailed_at).
Query them: SELECT * FROM unfailed ORDER BY unfailed_at;
"""

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Run from repo root; make milestone01 importable
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS = _REPO_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from crawl_persistence import (
    domain_to_publication_url,
    mark_queue_status,
    persist_recommendations,
    upsert_publication,
)
from milestone01.crawl import SubstackNetworkCrawler
from substack_api import Newsletter


def _crawler_lock_held(root: Path) -> bool:
    lock_path = root / ".crawler.lock"
    if not lock_path.exists():
        return False
    try:
        pid = int(lock_path.read_text().strip())
    except (OSError, ValueError):
        lock_path.unlink(missing_ok=True)
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        lock_path.unlink(missing_ok=True)
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Retry failed publications; record unfailed in unfailed table."
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Path to cartographer.db (default: repo root / cartographer.db)",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Max number of failed domains to retry this run (default: all)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds between retries (default: 1.0)",
    )
    args = parser.parse_args()

    root = Path(os.environ.get("CARTOGRAPHER_ROOT", _REPO_ROOT)).resolve()
    db_path = root / "cartographer.db" if args.db is None else Path(args.db).resolve()

    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    if _crawler_lock_held(root):
        print("Crawler lock is held; do not run retry_failed.py while the main crawler is running.", file=sys.stderr)
        sys.exit(1)

    crawler = SubstackNetworkCrawler(str(db_path))
    c = crawler.conn.cursor()

    # Create unfailed table if missing (retry_failed.py owns this table)
    c.execute(
        """CREATE TABLE IF NOT EXISTS unfailed (
            domain TEXT PRIMARY KEY,
            unfailed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    crawler.conn.commit()

    c.execute(
        "SELECT domain, depth FROM queue WHERE status = 'failed' ORDER BY depth, domain"
    )
    rows = c.fetchall()
    if not rows:
        print("No failed publications in queue.")
        return

    to_retry = rows if args.max is None else rows[: args.max]
    print(f"Retrying {len(to_retry)} failed publication(s) (of {len(rows)} total failed).")

    unfailed_count = 0
    for current_domain, current_depth in to_retry:
        url = domain_to_publication_url(current_domain)

        print(f"\n  Retrying: {current_domain} (depth {current_depth})")

        try:
            newsletter = Newsletter(url)
            pub_info = crawler.get_publication_info(newsletter)

            if not pub_info:
                print("     ❌ Could not fetch publication info (still failed).")
                continue

            upsert_publication(
                crawler.conn,
                domain=current_domain,
                publication_info=pub_info,
            )

            recommendations = newsletter.get_recommendations()
            print(f"     🔗 Found {len(recommendations)} recommendations.")

            persist_recommendations(
                crawler.conn,
                source_domain=current_domain,
                depth=current_depth,
                recommendation_objects=recommendations,
            )
            mark_queue_status(crawler.conn, domain=current_domain, status="crawled")
            c.execute(
                "INSERT OR REPLACE INTO unfailed (domain, unfailed_at) VALUES (?, ?)",
                (current_domain, datetime.now().isoformat()),
            )
            crawler.conn.commit()
            unfailed_count += 1
            print("     ✅ Unfailed.")

        except Exception as e:
            print(f"     ❌ Error: {e}")
            crawler.conn.rollback()

        if args.delay > 0:
            time.sleep(args.delay)

    print(f"\nDone. Unfailed {unfailed_count} of {len(to_retry)} retried.")


if __name__ == "__main__":
    main()
