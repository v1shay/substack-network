#!/usr/bin/env python3
"""
Milestone 1: Basic Network Crawler

Crawls the Substack network using the substack_api library.

Usage:
    # Default seed (heuristic)
    python milestone01.py

    # Seeds from config (run from repo root)
    python scripts/milestone01/crawl.py --seeds-file config/seeds.md

    # Optional: cap this run
    python scripts/milestone01/crawl.py --seeds-file config/seeds.md --max-publications 100

    # Faster crawl (less delay between publications; may hit rate limits)
    python scripts/milestone01/crawl.py --seeds-file config/seeds.md --delay 0.25

    # Activate venv first: source setup.sh or source activate.sh
"""

import argparse
import atexit
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional
from urllib.parse import urlparse

import requests

# Ensure repo modules are importable even when CARTOGRAPHER_ROOT points to
# a different runtime DB directory.
_CODE_ROOT = Path(__file__).resolve().parents[2]
if str(_CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(_CODE_ROOT))

from scripts.crawl_persistence import (
    add_to_queue as persist_queue_domain,
    domain_to_publication_url,
    mark_queue_status,
    normalize_domain as shared_normalize_domain,
    persist_recommendations,
    upsert_publication,
)
from scripts.db_runtime import connect_db, ensure_schema

# Lockfile so only one crawler instance runs per DB directory
_LOCKFILE: Path | None = None


def _repo_root() -> Path:
    """Repo root: CARTOGRAPHER_ROOT env var or cwd."""
    return Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()


def _acquire_crawler_lock() -> bool:
    """Create a lockfile with our PID. Return True if we got the lock, False if another crawler is running."""
    global _LOCKFILE
    lock_path = _repo_root() / ".crawler.lock"
    if lock_path.exists():
        try:
            pid = int(lock_path.read_text().strip())
        except (ValueError, OSError):
            lock_path.unlink(missing_ok=True)
        else:
            # Check if that process is still running
            try:
                os.kill(pid, 0)
            except OSError:
                # Process is dead (stale lock)
                lock_path.unlink(missing_ok=True)
            else:
                return False
    lock_path.write_text(str(os.getpid()), encoding="utf-8")
    _LOCKFILE = lock_path
    atexit.register(_release_crawler_lock)
    return True


def _release_crawler_lock() -> None:
    global _LOCKFILE
    if _LOCKFILE is not None and _LOCKFILE.exists():
        _LOCKFILE.unlink(missing_ok=True)
        _LOCKFILE = None


# Monitor-style output (same colors as crawl_monitor.py)
_MON_BOLD = "\033[1m"
_MON_GREEN = "\033[92m"
_MON_CYAN = "\033[96m"
_MON_RESET = "\033[0m"


def _mon_c(s: str, color: str) -> str:
    return f"{color}{s}{_MON_RESET}" if sys.stdout.isatty() else s

# Store datetime values as ISO strings for SQLite compatibility.
sqlite3.register_adapter(datetime, lambda v: v.isoformat() if v else None)

_NEWSLETTER_IMPORT_ERROR: Exception | None = None


def _print_substack_api_import_help() -> None:
    venv_python = _CODE_ROOT / ".venv" / "bin" / "python"
    print("❌ Error: Could not import substack_api")
    print(f"   Current Python: {sys.executable}")
    if _NEWSLETTER_IMPORT_ERROR is not None:
        print(f"   Import error: {_NEWSLETTER_IMPORT_ERROR}")
    if "anaconda" in sys.executable.lower() or "conda" in sys.executable.lower():
        print("\n   ⚠️  Detected Anaconda/Conda Python instead of venv.")
        print("   Try one of these:")
        print(f"   1. Run directly with venv Python:")
        print(f"      {venv_python} scripts/milestone01/crawl.py")
        print("   2. Or activate venv properly:")
        print("      source .venv/bin/activate")
        print("      python scripts/milestone01/crawl.py")
    else:
        print("   Make sure the virtual environment is activated:")
        print("   source .venv/bin/activate")


try:
    from substack_api import Newsletter
except ImportError as exc:
    Newsletter = None  # type: ignore[assignment]
    _NEWSLETTER_IMPORT_ERROR = exc


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first_publication_from_post_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    publication = _as_dict(meta.get("publication"))
    if publication:
        return publication

    bylines = meta.get("publishedBylines")
    if not isinstance(bylines, list):
        return {}

    for byline_raw in bylines:
        byline = _as_dict(byline_raw)
        publication_users = byline.get("publicationUsers")
        if not isinstance(publication_users, list):
            continue
        for publication_user_raw in publication_users:
            publication_user = _as_dict(publication_user_raw)
            publication = _as_dict(publication_user.get("publication"))
            if publication:
                return publication
    return {}


def extract_publication_info_from_post_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    publication = _first_publication_from_post_metadata(meta)
    return {
        "id": meta.get("publication_id") or publication.get("id"),
        "name": publication.get("name") or meta.get("publication_name"),
        "hero_text": publication.get("hero_text") or meta.get("description") or "",
        "subdomain": publication.get("subdomain"),
        "custom_domain": publication.get("custom_domain"),
        "canonical_url": meta.get("canonical_url") or meta.get("url"),
    }


def resolve_publication_url(current_domain: str, publication_info: dict[str, Any] | None) -> str:
    if publication_info:
        custom_domain = _publication_host(publication_info.get("custom_domain"))
        if custom_domain:
            return f"https://{custom_domain}"

        subdomain = _publication_subdomain(publication_info.get("subdomain"))
        if subdomain:
            return f"https://{subdomain}.substack.com"

        canonical_host = _publication_host(publication_info.get("canonical_url"))
        if canonical_host:
            return f"https://{canonical_host}"
    return domain_to_publication_url(current_domain)


def _publication_host(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.startswith(("http://", "https://")):
        parsed = urlparse(text)
        host = parsed.netloc.strip().lower()
        return host or None
    host = text.split("/", 1)[0].strip().lower()
    return host or None


def _publication_subdomain(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    return text.split(".", 1)[0]


class SubstackNetworkCrawler:
    """Network crawler using substack_api library."""

    def __init__(self, db_name="cartographer.db"):
        """Initialize crawler with database connection.
        DB path is relative to current working directory (same as view_db default)."""
        self.conn = connect_db(db_name)
        self.create_schema()

    def create_schema(self):
        """Create database tables used by the crawler.

        Core crawl tables are publications/recommendations/queue.
        Comment-enrichment tables (users/posts/comments) are created here
        so future stages can write to them without schema migrations.
        Writes to comment tables are optional and guarded by --enable-comments.
        """
        ensure_schema(self.conn)
        self.conn.commit()

    def normalize_domain(self, url: str) -> str:
        """Normalize domain from Newsletter URL."""
        return shared_normalize_domain(url)

    def load_seeds_from_file(self, path: str) -> List[str]:
        """
        Load seed domains from a file. Supports the same format as substack-pAIa
        config/newsletters.md: one URL per line (first https? URL on the line).
        Returns list of normalized domains (substack subdomain or custom domain).
        """
        path = Path(path).expanduser().resolve()
        if not path.exists():
            print(f"   [!] Seeds file not found: {path}")
            return []
        domains = []
        seen = set()
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                match = re.search(r"(https?://\S+)", line)
                if match:
                    url = match.group(1).rstrip("/")
                    if "wikipedia.org" in url:
                        continue
                    domain = self.normalize_domain(url)
                    if domain and domain not in seen:
                        seen.add(domain)
                        domains.append(domain)
        return domains

    def get_publication_info(self, newsletter: Newsletter) -> Optional[dict]:
        """
        Get publication metadata. Tries direct API first, then from first post.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        # 1) Direct GET /api/v1/publication (fails if domain doesn't resolve)
        try:
            endpoint = f"{newsletter.url}/api/v1/publication"
            response = requests.get(endpoint, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"   [!] Direct publication API failed: {e}")
        # 2) Fallback: get one post and derive publication info from metadata
        try:
            posts = newsletter.get_posts(limit=1)
            if posts:
                meta = posts[0].get_metadata()
                return extract_publication_info_from_post_metadata(meta)
        except Exception as e:
            print(f"   [!] Fallback from post metadata failed: {e}")
        return None

    def add_to_queue(self, domain: str, depth: int):
        """Add domain to crawl queue."""
        try:
            persist_queue_domain(self.conn, domain, depth)
            self.conn.commit()
        except Exception:
            pass

    def publication_exists(self, domain: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM publications WHERE domain = ? LIMIT 1",
            (domain,),
        ).fetchone()
        return row is not None

    def mark_failure_or_preserve_existing_publication(self, domain: str) -> None:
        if self.publication_exists(domain):
            print("   [i] Publication already exists in DB; preserving queue status as crawled.")
            mark_queue_status(self.conn, domain=domain, status="crawled")
            return
        mark_queue_status(self.conn, domain=domain, status="failed")

    def run_comment_enrichment(
        self,
        publication_url: str,
        publication_domain: str,
        *,
        post_limit: int,
        timeout: float,
        retries: int,
        classify_commenters: bool,
        classification_max_users: int,
        classification_workers: int,
    ) -> None:
        """Run optional comment enrichment for a crawled publication in fail-open mode."""
        try:
            from scripts.comments.comment_pipeline import process_comments

            stats = process_comments(
                publication_url,
                conn=self.conn,
                post_limit=post_limit,
                timeout=timeout,
                retries=retries,
                classify_commenters=classify_commenters,
                classify_max_users=classification_max_users,
                classify_workers=classification_workers,
            )
            summary_parts = [
                "   ",
                f"[comments] domain={publication_domain}",
                f"posts_seen={stats.get('posts_seen', 0)}",
                f"posts_created={stats.get('posts_created', 0)}",
                f"posts_updated={stats.get('posts_updated', 0)}",
                f"users_seen={stats.get('users_seen', 0)}",
                f"users_created={stats.get('users_created', 0)}",
                f"users_updated={stats.get('users_updated', 0)}",
                f"comments_fetched={stats.get('comments_fetched', 0)}",
                f"comments_unique={stats.get('comments_unique', 0)}",
                f"comments_created={stats.get('comments_created', 0)}",
                f"comments_updated={stats.get('comments_updated', 0)}",
                f"classified_users={stats.get('classified_users', 0)}",
                f"classified_owners={stats.get('classified_owners', 0)}",
            ]
            print(" ".join(summary_parts))
        except Exception as exc:
            print(f"   [comments][error] domain={publication_domain}: {exc}")

    def crawl(
        self,
        max_publications: int | None = None,
        delay: float = 1.0,
        max_attempts: int | None = None,
        enable_comments: bool = False,
        comment_post_limit: int = 20,
        comment_timeout: float = 15.0,
        comment_retries: int = 3,
        classify_commenters: bool = False,
        classification_max_users: int = 10,
        classification_workers: int = 1,
    ):
        """
        Crawl network using Newsletter API.

        Parameters
        ----------
        max_publications : int or None
            Maximum number of publications to crawl; None = no limit (run until queue empty or Ctrl-C).
        delay : float
            Seconds to sleep after each publication (rate limiting). Default 1.0; lower (e.g. 0.25) speeds up but may trigger rate limits.
        max_attempts : int or None
            Maximum queue rows to attempt in this run, regardless of success/failure.
            Useful safety cap in degraded network environments.
        enable_comments : bool
            If True, run comment ingestion after each successfully crawled publication.
        comment_post_limit : int
            Maximum archive posts per publication to inspect for comments.
        comment_timeout : float
            Request timeout in seconds for comment/archive endpoints.
        comment_retries : int
            Retry attempts for comment/archive endpoints.
        classify_commenters : bool
            If True, run profile-based commenter classification (`admin + hasPosts`).
        classification_max_users : int
            Maximum distinct commenter handles to classify per publication.
        classification_workers : int
            Worker threads for profile lookups when classification is enabled.
        """
        if Newsletter is None and max_publications != 0:
            raise RuntimeError(
                "substack_api is not installed. Run `source setup.sh` (or use `.venv/bin/python`) before crawling."
            )

        goal = (
            f"{max_publications} publications"
            if max_publications is not None
            else "no limit (Ctrl-C to stop)"
        )
        if max_attempts is not None:
            goal = f"{goal}, {max_attempts} attempts"
        if enable_comments:
            goal = f"{goal}, comments enabled"
        if enable_comments and classify_commenters:
            goal = f"{goal}, commenter classification enabled"
        print(f"--- Starting Network Crawl (Goal: {goal}) ---")

        count = 0
        attempts = 0
        while True:
            if max_publications is not None and count >= max_publications:
                break
            if max_attempts is not None and attempts >= max_attempts:
                print(f"--- Crawl Stopped (Attempt Cap Reached: {max_attempts}) ---")
                break

            c = self.conn.cursor()
            c.execute(
                "SELECT domain, depth FROM queue WHERE status='pending' ORDER BY depth ASC LIMIT 1"
            )
            row = c.fetchone()

            if not row:
                print("--- Crawl Complete (Queue Empty) ---")
                break

            current_domain, current_depth = row
            attempts += 1
            print(
                f"\n📰 Crawling: {current_domain} (Depth: {current_depth})"
            )

            url = domain_to_publication_url(current_domain)

            try:
                newsletter = Newsletter(url)

                # Get publication metadata
                pub_info = self.get_publication_info(newsletter)
                if pub_info:
                    upsert_publication(
                        self.conn,
                        domain=current_domain,
                        publication_info=pub_info,
                    )

                    # Get recommendations using Newsletter API
                    recommendations = newsletter.get_recommendations()
                    print(f"   🔗 Found {len(recommendations)} recommendations.")

                    persist_recommendations(
                        self.conn,
                        source_domain=current_domain,
                        depth=current_depth,
                        recommendation_objects=recommendations,
                    )
                    mark_queue_status(self.conn, domain=current_domain, status="crawled")
                    count += 1

                    if enable_comments:
                        publication_url = resolve_publication_url(current_domain, pub_info)
                        self.run_comment_enrichment(
                            publication_url,
                            current_domain,
                            post_limit=comment_post_limit,
                            timeout=comment_timeout,
                            retries=comment_retries,
                            classify_commenters=classify_commenters,
                            classification_max_users=classification_max_users,
                            classification_workers=classification_workers,
                        )

                    # Monitor-style totals (same output as crawl_monitor.py)
                    c.execute("SELECT COUNT(*) FROM publications")
                    pubs = c.fetchone()[0]
                    c.execute("SELECT COUNT(*) FROM recommendations")
                    recs = c.fetchone()[0]
                    c.execute("SELECT COUNT(*) FROM queue")
                    queued = c.fetchone()[0]
                    ts = datetime.now().strftime("%H:%M:%S")
                    print(
                        "   ",
                        _mon_c(f"[{ts}]", _MON_CYAN),
                        _mon_c("publications:", _MON_BOLD), _mon_c(str(pubs), _MON_GREEN),
                        _mon_c("recommendations:", _MON_BOLD), _mon_c(str(recs), _MON_GREEN),
                        _mon_c("queued:", _MON_BOLD), _mon_c(str(queued), _MON_GREEN),
                    )
                else:
                    print("   ❌ Could not fetch publication info.")
                    self.mark_failure_or_preserve_existing_publication(current_domain)

            except Exception as e:
                print(f"   ❌ Error: {e}")
                self.mark_failure_or_preserve_existing_publication(current_domain)

            self.conn.commit()
            if delay > 0:
                time.sleep(delay)

        print(f"\n✅ Crawl Complete. Processed {count} publications.")


if __name__ == "__main__":
    if Newsletter is None:
        _print_substack_api_import_help()
        sys.exit(1)

    if not _acquire_crawler_lock():
        lock_path = _repo_root() / ".crawler.lock"
        try:
            pid = int(lock_path.read_text().strip())
        except (ValueError, OSError):
            pid = None
        print("Crawler already running (another milestone01.py is using this directory).", file=sys.stderr)
        if pid is not None:
            print(f"  Locked by PID {pid}. Stop that process first.", file=sys.stderr)
        print("  If that process crashed, remove .crawler.lock and try again.", file=sys.stderr)
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Crawl Substack network from seeds (publications + recommendations)."
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        metavar="SECS",
        help="Seconds to sleep after each publication (rate limiting). Default 1.0; use 0.25–0.5 to speed up (risk of rate limits).",
    )
    parser.add_argument(
        "--seeds-file",
        type=str,
        default=None,
        help="Path to a file of seed URLs (one per line or pAIa newsletters.md format). "
        "Example: ../../substack-pAIa/config/newsletters.md",
    )
    parser.add_argument(
        "--max-publications",
        type=int,
        default=None,
        metavar="N",
        help="Max publications to crawl per run (default: no limit; use Ctrl-C to stop).",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=None,
        metavar="N",
        help="Max queue attempts per run (success + failure). Safety cap for degraded networks.",
    )
    parser.add_argument(
        "--enable-comments",
        action="store_true",
        help="Enable optional comment enrichment for each successfully crawled publication.",
    )
    parser.add_argument(
        "--comment-post-limit",
        type=int,
        default=20,
        metavar="N",
        help="Max archive posts per publication to inspect for comments (default: 20).",
    )
    parser.add_argument(
        "--comment-timeout",
        type=float,
        default=15.0,
        metavar="SECS",
        help="Timeout in seconds for comment/archive endpoint requests (default: 15.0).",
    )
    parser.add_argument(
        "--comment-retries",
        type=int,
        default=3,
        metavar="N",
        help="Retry attempts for comment/archive endpoint requests (default: 3).",
    )
    parser.add_argument(
        "--classify-commenters",
        action="store_true",
        help="Enable strict commenter classification (`admin + hasPosts`) using Substack public profiles.",
    )
    parser.add_argument(
        "--classification-max-users",
        type=int,
        default=10,
        metavar="N",
        help="Max unique commenter handles to classify per publication when classification is enabled (default: 10).",
    )
    parser.add_argument(
        "--classification-workers",
        type=int,
        default=1,
        metavar="N",
        help="Worker threads for commenter profile lookups when classification is enabled (default: 1).",
    )
    args = parser.parse_args()

    root = _repo_root()
    crawler = SubstackNetworkCrawler(db_name=str(root / "cartographer.db"))

    if args.seeds_file:
        domains = crawler.load_seeds_from_file(args.seeds_file)
        for d in domains:
            crawler.add_to_queue(d, 0)
        print(f"Loaded {len(domains)} seeds from {args.seeds_file}")
    else:
        # Only add default seed when queue has no pending domains (resume without adding)
        c = crawler.conn.cursor()
        c.execute("SELECT 1 FROM queue WHERE status='pending' LIMIT 1")
        if c.fetchone() is None:
            crawler.add_to_queue("heuristic", 0)
            print("Queue empty. Using default seed: heuristic")
        else:
            print("Resuming from existing queue (no new seeds added).")

    crawler.crawl(
        max_publications=args.max_publications,
        delay=args.delay,
        max_attempts=args.max_attempts,
        enable_comments=args.enable_comments,
        comment_post_limit=args.comment_post_limit,
        comment_timeout=args.comment_timeout,
        comment_retries=args.comment_retries,
        classify_commenters=args.classify_commenters,
        classification_max_users=args.classification_max_users,
        classification_workers=args.classification_workers,
    )
