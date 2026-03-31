#!/usr/bin/env python3
"""
Update the recommendation graph: start crawl in background (if not already running),
then run centrality, visualize, add_publication_lists, and open index.html.
Does not wait for crawl to finish; graph and lists use current DB state.

Run from repo root (or set CARTOGRAPHER_ROOT):

    python scripts/update_graph.py
"""

import os
import argparse
import sqlite3
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]


def runtime_root() -> Path:
    return Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()


def crawler_lock_path(root: Path) -> Path:
    return root / ".crawler.lock"


def investigator_lock_path(root: Path) -> Path:
    return root / ".investigator.lock"


def detached_log_path(root: Path, process_name: str) -> Path:
    log_dir = root / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"{process_name}.log"


def is_investigator_running(root: Path) -> bool:
    lock = investigator_lock_path(root)
    if not lock.exists():
        return False
    try:
        pid = int(lock.read_text().strip())
    except (ValueError, OSError):
        lock.unlink(missing_ok=True)
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        lock.unlink(missing_ok=True)
        return False
    return True


def is_crawl_running(root: Path) -> bool:
    lock = crawler_lock_path(root)
    if not lock.exists():
        return False
    try:
        pid = int(lock.read_text().strip())
    except (ValueError, OSError):
        lock.unlink(missing_ok=True)
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        lock.unlink(missing_ok=True)
        return False
    return True


def run(cmd: list[str], cwd: Path, name: str, env: dict[str, str]) -> None:
    print(f"\n--- {name} ---")
    r = subprocess.run(cmd, cwd=cwd, env=env)
    if r.returncode != 0:
        print(f"{name} failed with exit code {r.returncode}", file=sys.stderr)
        sys.exit(r.returncode)


def spawn_detached(
    cmd: list[str],
    cwd: Path,
    env: dict[str, str],
    *,
    log_path: Path | None = None,
) -> subprocess.Popen:
    """Start a background process without inheriting terminal IO.

    Detached jobs append stdout/stderr to a runtime log file so background
    failures are inspectable instead of disappearing into /dev/null.
    """
    if log_path is None:
        return subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            start_new_session=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    child_env = env.copy()
    child_env.setdefault("PYTHONUNBUFFERED", "1")
    with log_path.open("ab") as log_handle:
        return subprocess.Popen(
            cmd,
            cwd=cwd,
            env=child_env,
            start_new_session=True,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=log_handle,
        )


def has_recommendation_data(root: Path) -> bool:
    db_path = root / "cartographer.db"
    if not db_path.exists():
        return False

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT 1
              FROM sqlite_master
             WHERE type = 'table'
               AND name = 'recommendations'
            """
        )
        if cursor.fetchone() is None:
            return False
        cursor.execute("SELECT 1 FROM recommendations LIMIT 1")
        return cursor.fetchone() is not None
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh graph artifacts and optionally open the UI.")
    parser.add_argument("--no-open", action="store_true", help="Build artifacts without opening browser windows.")
    args = parser.parse_args()

    root = runtime_root()
    code_root = CODE_ROOT
    py = sys.executable
    scripts = code_root / "scripts"
    child_env = os.environ.copy()
    child_env["CARTOGRAPHER_ROOT"] = str(root)

    print("Update recommendation graph")
    print(f"Code root: {code_root}")
    print(f"Runtime root: {root}")

    # If no crawl is running, start one in the background so the DB keeps updating.
    # We do not wait for it; the graph and lists below use whatever is in the DB now.
    # Drop inherited terminal IO so the detached crawl stays quiet after update_graph exits.
    if is_crawl_running(root):
        print("Crawl already in progress, skipping. Graph will use current DB.")
    else:
        crawl_log = detached_log_path(root, "crawler")
        spawn_detached(
            [py, str(scripts / "milestone01" / "crawl.py"), "--enable-comments"],
            cwd=code_root,
            env=child_env,
            log_path=crawl_log,
        )
        print(
            "Crawl started in background with comment enrichment enabled "
            f"(log: {crawl_log})."
        )

    # Build the graph pipeline from current DB only when recommendations exist.
    # On a fresh runtime root the background crawl may not have produced edges yet.
    if has_recommendation_data(root):
        run(
            [py, str(scripts / "milestone01" / "centrality.py")],
            cwd=code_root,
            name="Centrality",
            env=child_env,
        )

        run(
            [py, str(scripts / "milestone01" / "visualize.py")],
            cwd=code_root,
            name="Visualize",
            env=child_env,
        )

        run(
            [py, str(scripts / "milestone02" / "add_publication_lists.py")],
            cwd=code_root,
            name="Add publication lists",
            env=child_env,
        )

        run(
            [py, str(scripts / "milestone02" / "layer_stats.py")],
            cwd=code_root,
            name="Layer stats (L, r)",
            env=child_env,
        )
    else:
        print("Recommendations not available yet; skipping graph artifact generation for this run.")

    run(
        [py, str(scripts / "milestone02" / "extract_failed.py")],
        cwd=code_root,
        name="Extract failed publications",
        env=child_env,
    )

    # Open the main page immediately so the user can use the graph while the investigator runs.
    index_html = root / "index.html"
    if index_html.exists() and not args.no_open:
        webbrowser.open(index_html.as_uri())

    # Investigator: run only if not already running; then wait and open report when done.
    if is_investigator_running(root):
        print("\nInvestigator already in progress, skipping. Open the report from Failed publications when it finishes.")
    else:
        # Default = only new failed URLs (not yet in log). Use --full or --all to re-probe all.
        print("\n--- Investigate failed (report) [background] ---")
        investigator_log = detached_log_path(root, "investigator")
        inv_proc = spawn_detached(
            [py, str(scripts / "milestone02" / "investigate_failed.py")],
            cwd=code_root,
            env=child_env,
            log_path=investigator_log,
        )
        t0 = time.monotonic()
        wait_timeout = 120
        try:
            inv_proc.wait(timeout=wait_timeout)
        except subprocess.TimeoutExpired:
            print(
                f"Investigator still running after {wait_timeout}s; leaving it in background.",
                file=sys.stderr,
            )
            print("\nDone.")
            return

        if inv_proc.returncode != 0:
            print(f"Investigate failed exited with code {inv_proc.returncode}", file=sys.stderr)
            sys.exit(inv_proc.returncode)

        # Open the report only if the investigator took at least a minute (meaningful run); else skip (user can open from Failed publications).
        elapsed = time.monotonic() - t0
        if elapsed >= 60 and not args.no_open:
            report_html = root / "data" / "failed_investigation.html"
            if report_html.exists():
                webbrowser.open(report_html.as_uri())
        else:
            print("Investigator finished in under 1 min or browser opening is disabled; report not opened automatically.")

    print("\nDone.")


if __name__ == "__main__":
    main()
