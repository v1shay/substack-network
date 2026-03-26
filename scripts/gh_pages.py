#!/usr/bin/env python3
"""
Prepare and push a 'pages' branch for GitHub Pages / Codeberg Pages.
Puts data/substack_graph.html and index.html on the branch; optionally
includes data/graph-publications.html and data/db-publications.html (milestone02).
Run from repo root.

Before deploying, the script commits any uncommitted changes on the source branch
and pushes it to the remote (so the site is deployed from the latest synced main).
Use --no-commit or --no-sync to skip either step.

Usage (from repo root):
    python scripts/gh_pages.py              # commit/sync main, then create or update pages branch
    python scripts/gh_pages.py --source main # take graph from branch 'main'
    python scripts/gh_pages.py --no-sync    # skip pushing main (e.g. offline)

First run: creates orphan branch 'pages', adds only the graph, commits, pushes.
Later runs: checks out 'pages', pulls graph from --source branch, commits, pushes.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=check)


def main():
    parser = argparse.ArgumentParser(description="Push substack graph to a 'pages' branch (GitHub/Codeberg Pages).")
    parser.add_argument("--source", default="main", help="Branch to take data/substack_graph.html from (default: main)")
    parser.add_argument("--remote", default="origin", help="Remote to push to (default: origin)")
    parser.add_argument("--no-commit", action="store_true", help="Skip auto-commit of uncommitted changes on source branch")
    parser.add_argument("--no-sync", action="store_true", help="Skip auto-push of source branch before deploying")
    parser.add_argument("--dry-run", action="store_true", help="Validate prerequisites and print what would happen without mutating git state")
    args = parser.parse_args()

    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    graph_path = root / "data" / "substack_graph.html"
    # Optional list pages (milestone02) in data/; deployed if present on source branch
    list_pages = [
        "data/graph-publications.html",
        "data/db-publications.html",
        "data/layer_stats.html",
        "data/failed_publications.html",
        "data/unfailed_publications.html",
        "data/failed_investigation.html",
    ]
    if not graph_path.exists():
        print(f"Graph not found: {graph_path}", file=sys.stderr)
        print("Run from repo root after: python scripts/milestone01/visualize.py", file=sys.stderr)
        sys.exit(1)

    # Check we're in a git repo
    res = run(["git", "rev-parse", "--is-inside-work-tree"], root, check=False)
    if res.returncode != 0:
        print("Not inside a git repository.", file=sys.stderr)
        sys.exit(1)

    # Require we're on the source branch so we deploy what's committed there
    current = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], root).stdout.strip()
    if current != args.source:
        print(f"You're not on branch '{args.source}' (current: {current}).", file=sys.stderr)
        print(f"Switch to it first (e.g. git checkout {args.source}), then run gh_pages.py again.", file=sys.stderr)
        sys.exit(1)

    status = run(["git", "status", "--porcelain"], root).stdout.strip()
    dirty = [line for line in status.splitlines() if line and not line.startswith("??")]
    branch_exists = run(["git", "rev-parse", "--verify", "pages"], root, check=False).returncode == 0
    if args.dry_run:
        print("Dry run: prerequisites validated.")
        print(f"  source branch: {args.source}")
        print(f"  remote: {args.remote}")
        print(f"  pages branch exists: {'yes' if branch_exists else 'no'}")
        print(f"  graph path: {graph_path}")
        print(f"  tracked modifications on source branch: {len(dirty)}")
        if dirty and not args.no_commit:
            print("  would auto-commit tracked changes on the source branch before deploy.")
        elif dirty:
            print("  source branch is dirty and would fail without auto-commit.")
        if not args.no_sync:
            print(f"  would push {args.source} to {args.remote} before deploy.")
        if branch_exists:
            print("  would update existing 'pages' branch via worktree and push.")
        else:
            print("  would create orphan 'pages' branch, add site artifacts, commit, and push.")
        return

    # Optionally commit and sync the source branch before deploying
    if not args.no_commit and dirty:
        print("Committing uncommitted changes on", args.source, "...")
        run(["git", "add", "-A"], root)
        run(["git", "commit", "-m", "Update before pages deploy"], root)
    if not args.no_sync:
        print("Syncing", args.source, "to", args.remote, "...")
        res = run(["git", "push", args.remote, args.source], root, check=False)
        if res.returncode != 0:
            print(res.stderr or res.stdout, file=sys.stderr)
            print(f"Push of '{args.source}' failed. Fix and re-run, or use --no-sync to skip.", file=sys.stderr)
            sys.exit(1)

    # Require data/substack_graph.html is committed (no local changes)
    res = run(["git", "ls-files", "--error-unmatch", "data/substack_graph.html"], root, check=False)
    if res.returncode != 0:
        print("data/substack_graph.html is not committed on the source branch.", file=sys.stderr)
        print("Add and commit it first (e.g. git add data/substack_graph.html && git commit -m 'Update graph'), then run gh_pages.py again.", file=sys.stderr)
        sys.exit(1)
    res = run(["git", "diff", "--quiet", "HEAD", "--", "data/substack_graph.html"], root, check=False)
    if res.returncode != 0:
        print("You have uncommitted changes to data/substack_graph.html.", file=sys.stderr)
        print("Commit them first, then run gh_pages.py again.", file=sys.stderr)
        sys.exit(1)

    # Require a clean working tree on the source branch (no other uncommitted changes)
    status = run(["git", "status", "--porcelain"], root).stdout.strip()
    dirty = [line for line in status.splitlines() if line and not line.startswith("??")]
    if dirty:
        print("You have uncommitted changes on the source branch.", file=sys.stderr)
        print("Commit or stash them first, then run gh_pages.py again.", file=sys.stderr)
        sys.exit(1)

    if not branch_exists:
        # Create orphan 'pages' branch (use checkout --orphan for compatibility with older Git)
        print("Creating orphan branch 'pages'...")
        res = run(["git", "checkout", "--orphan", "pages"], root, check=False)
        if res.returncode != 0:
            # Branch may exist locally; treat as update
            print(res.stderr or res.stdout, file=sys.stderr)
            print("Treating as update of existing 'pages' branch...", file=sys.stderr)
            branch_exists = True
        else:
            run(["git", "rm", "-rf", "--cached", "."], root, check=False)  # untrack all, keep files
            index_path = root / "index.html"
            if index_path.exists():
                pass  # use existing index.html (from milestone02-visualize.py)
            else:
                index_path.write_text(graph_path.read_text(encoding="utf-8"), encoding="utf-8")
            to_add = ["data/substack_graph.html", "index.html"]
            for path in list_pages:
                if (root / path).exists():
                    to_add.append(path)
            run(["git", "add"] + to_add, root)
            run(["git", "commit", "-m", "Pages: substack recommendation graph"], root)
            run(["git", "push", "-u", args.remote, "pages"], root)
            res = run(["git", "checkout", args.source], root, check=False)
            if res.returncode != 0:
                print("Done. Pages branch created and pushed.", file=sys.stderr)
                print(f"Could not switch back to '{args.source}': {res.stderr or res.stdout}", file=sys.stderr)
                print(f"Run: git checkout {args.source}  (or your default branch name)", file=sys.stderr)
            else:
                print("Done. Pages branch created and pushed. You're back on", args.source)
            return

    if branch_exists:
        # Update existing pages branch using a worktree so we never leave main (avoids IDE confusion)
        print("Updating 'pages' branch from", args.source, "...")
        worktree_dir = root / ".pages-worktree"
        # Remove leftover worktree from a previous run
        if worktree_dir.exists():
            run(["git", "worktree", "remove", "-f", str(worktree_dir)], root, check=False)
        run(["git", "worktree", "add", str(worktree_dir), "pages"], root)
        try:
            res = run(["git", "show", f"{args.source}:data/substack_graph.html"], root, check=False)
            if res.returncode != 0:
                print(f"Could not read data/substack_graph.html from branch '{args.source}'.", file=sys.stderr)
                sys.exit(1)
            graph_content = res.stdout
            (worktree_dir / "data").mkdir(parents=True, exist_ok=True)
            (worktree_dir / "data" / "substack_graph.html").write_text(graph_content, encoding="utf-8")
            # Use index.html from source branch if present (main has the full site); else copy from graph
            res_index = run(["git", "show", f"{args.source}:index.html"], root, check=False)
            index_content = res_index.stdout if res_index.returncode == 0 else graph_content
            (worktree_dir / "index.html").write_text(index_content, encoding="utf-8")
            for path in list_pages:
                res_f = run(["git", "show", f"{args.source}:{path}"], root, check=False)
                if res_f.returncode == 0:
                    (worktree_dir / path).parent.mkdir(parents=True, exist_ok=True)
                    (worktree_dir / path).write_text(res_f.stdout, encoding="utf-8")
            to_add = ["data/substack_graph.html", "index.html"]
            for path in list_pages:
                if (worktree_dir / path).exists():
                    to_add.append(path)
            run(["git", "add"] + to_add, cwd=worktree_dir)
            run(["git", "commit", "-m", "Pages: update substack graph"], cwd=worktree_dir, check=False)
            run(["git", "push", args.remote, "pages"], cwd=worktree_dir)
            print("Done. Pages branch updated and pushed. You stayed on", args.source)
        finally:
            run(["git", "worktree", "remove", "-f", str(worktree_dir)], root, check=False)


if __name__ == "__main__":
    main()
