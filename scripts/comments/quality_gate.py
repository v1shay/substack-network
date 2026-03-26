#!/usr/bin/env python3
"""Strict quality gate runner for comment-crawling release readiness."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.comments.db_audit import audit_db, ordered_anomaly_names, summarize
from scripts.comments.db_repair import run_repair


def _repo_root() -> Path:
    return Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()


def _run_unittest_discovery() -> bool:
    print("\n[1/7] Deterministic suite: python -m unittest discover -s tests -p \"test_*.py\" -v")
    suite = unittest.defaultTestLoader.discover(str(REPO_ROOT / "tests"), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result.wasSuccessful()


def _run_live_endpoint_test(require_no_skips: bool, strict_live: bool) -> bool:
    cmd = "SUBSTACK_LIVE_TESTS=1"
    if strict_live:
        cmd += " SUBSTACK_STRICT_LIVE=1"
    cmd += " python -m unittest discover -s tests -p \"test_substack_endpoint_vitality.py\" -v"
    print(f"\n[2/7] Live vitality: {cmd}")

    env = os.environ.copy()
    env["SUBSTACK_LIVE_TESTS"] = "1"
    if strict_live:
        env["SUBSTACK_STRICT_LIVE"] = "1"
    else:
        env.pop("SUBSTACK_STRICT_LIVE", None)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-p",
            "test_substack_endpoint_vitality.py",
            "-v",
        ],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, file=sys.stderr, end="")

    if completed.returncode != 0:
        return False

    skipped_count = 0
    if require_no_skips:
        match = re.search(r"skipped=(\d+)", f"{completed.stdout}\n{completed.stderr}")
        if match:
            skipped_count = int(match.group(1))
    if skipped_count > 0:
        print("Live vitality failed gate: test was skipped (network check did not execute).")
        return False
    return True


def _run_bounded_crawl(
    root: Path,
    seeds_file: str,
    max_publications: int,
    delay: float,
    max_attempts: int | None,
    *,
    enable_comments: bool,
    comment_post_limit: int,
    classify_commenters: bool,
    classification_max_users: int,
    classification_workers: int,
) -> bool:
    crawl_script = REPO_ROOT / "scripts/milestone01/crawl.py"
    seed_path = Path(seeds_file).expanduser()
    if not seed_path.is_absolute():
        repo_seed = (REPO_ROOT / seed_path).resolve()
        root_seed = (root / seed_path).resolve()
        if repo_seed.exists():
            seed_path = repo_seed
        elif root_seed.exists():
            seed_path = root_seed

    attempt_suffix = "" if max_attempts is None else f" --max-attempts {max_attempts}"
    comments_suffix = "" if not enable_comments else f" --enable-comments --comment-post-limit {comment_post_limit}"
    classify_suffix = ""
    if enable_comments and classify_commenters:
        classify_suffix = (
            " --classify-commenters"
            f" --classification-max-users {classification_max_users}"
            f" --classification-workers {classification_workers}"
        )
    print(
        f"\n[3/7] Bounded crawl: python {crawl_script} --seeds-file {seed_path} "
        f"--max-publications {max_publications} --delay {delay}{attempt_suffix}{comments_suffix}{classify_suffix}"
    )
    cmd = [
        sys.executable,
        str(crawl_script),
        "--seeds-file",
        str(seed_path),
        "--max-publications",
        str(max_publications),
        "--delay",
        str(delay),
    ]
    if max_attempts is not None:
        cmd.extend(["--max-attempts", str(max_attempts)])
    if enable_comments:
        cmd.extend(["--enable-comments", "--comment-post-limit", str(comment_post_limit)])
    if enable_comments and classify_commenters:
        cmd.extend(
            [
                "--classify-commenters",
                "--classification-max-users",
                str(classification_max_users),
                "--classification-workers",
                str(classification_workers),
            ]
        )
    env = os.environ.copy()
    env["CARTOGRAPHER_ROOT"] = str(root)
    completed = subprocess.run(cmd, cwd=REPO_ROOT, env=env)
    return completed.returncode == 0


def _run_audit_and_repair_loop(db_path: Path, max_repair_rounds: int) -> bool:
    print("\n[4/7] Zero-anomaly audit + repair loop")
    for round_idx in range(max_repair_rounds + 1):
        counts = audit_db(db_path)
        summary = summarize(counts)
        print(f"  Audit round {round_idx + 1}:")
        for key in ordered_anomaly_names():
            print(f"    {key}: {counts[key]}")

        if summary["all_zero"]:
            print("  Audit gate passed: all counters are zero.")
            return True

        if round_idx == max_repair_rounds:
            print("  Audit gate failed: counters still non-zero after max repair rounds.")
            return False

        fixes = run_repair(db_path)
        print("  Applied repair pass:")
        for key, value in fixes.items():
            print(f"    {key}: {value}")

    return False


def _run_wrapper_checks(root: Path) -> bool:
    print("\n[5/7] Wrapper validation")
    commands = [
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "update_graph.py"),
            "--no-open",
        ],
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "gh_pages.py"),
            "--dry-run",
            "--no-commit",
            "--no-sync",
        ],
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "milestone02" / "label_topics_llm.py"),
            "--check-config",
        ],
    ]
    env = os.environ.copy()
    env["CARTOGRAPHER_ROOT"] = str(root)
    for cmd in commands:
        completed = subprocess.run(cmd, cwd=REPO_ROOT, env=env)
        if completed.returncode != 0:
            return False
    return True


def _run_post_repair_deterministic_suite() -> bool:
    print("\n[6/7] Post-repair deterministic suite rerun")
    suite = unittest.defaultTestLoader.discover(str(REPO_ROOT / "tests"), pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result.wasSuccessful()


def _print_commit_step() -> None:
    print("\n[7/7] Commit command sequence (only after all prior steps pass):")
    print("  git lfs install")
    print("  git add scripts/milestone01/crawl.py docs/comment-pipeline.md scripts/comments tests cartographer.db")
    print("  git add -f scripts/milestone01/cartographer.db")
    print("  git commit -m \"Add standalone comment pipeline, integrity gates, and DB repair workflow\"")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run strict release-quality gate for comment-crawling changes.")
    parser.add_argument(
        "--db",
        default=str(_repo_root() / "cartographer.db"),
        help="Path to DB used for audit/repair checks.",
    )
    parser.add_argument(
        "--seeds-file",
        default="config/seeds.md",
        help="Seeds file for bounded crawl.",
    )
    parser.add_argument(
        "--max-publications",
        type=int,
        default=1000,
        help="Bound for crawl regression step.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Crawler delay in seconds for regression step.",
    )
    parser.add_argument(
        "--crawl-max-attempts",
        type=int,
        default=None,
        help="Optional attempt cap for bounded crawl step (success + failure attempts).",
    )
    parser.add_argument(
        "--max-repair-rounds",
        type=int,
        default=3,
        help="Max repair passes before audit gate fails.",
    )
    parser.add_argument(
        "--allow-live-skip",
        action="store_true",
        help="Allow skipped live vitality test (non-release mode).",
    )
    parser.add_argument(
        "--hard-fail-live",
        action="store_true",
        help="Treat live vitality and bounded crawl checks as hard failures (default: advisory only).",
    )
    parser.add_argument(
        "--strict-live",
        action="store_true",
        help="Require live vitality to pass using real endpoint data (no deterministic fallback).",
    )
    parser.add_argument(
        "--run-crawl",
        action="store_true",
        help="Run bounded crawl step (network-heavy; disabled by default).",
    )
    parser.add_argument(
        "--crawl-enable-comments",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable comments during bounded crawl check (default: true).",
    )
    parser.add_argument(
        "--crawl-comment-post-limit",
        type=int,
        default=3,
        help="Comment post limit used in bounded crawl check when comments are enabled.",
    )
    parser.add_argument(
        "--crawl-classify-commenters",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable commenter classification during bounded crawl check (default: true).",
    )
    parser.add_argument(
        "--crawl-classification-max-users",
        type=int,
        default=25,
        help="Classification max users used in bounded crawl check.",
    )
    parser.add_argument(
        "--crawl-classification-workers",
        type=int,
        default=4,
        help="Classification workers used in bounded crawl check.",
    )
    parser.add_argument(
        "--skip-crawl",
        action="store_true",
        help="Compatibility flag; crawl is skipped by default unless --run-crawl is set.",
    )
    parser.add_argument(
        "--run-wrapper-checks",
        action="store_true",
        help="Run update_graph --no-open, gh_pages --dry-run, and label_topics_llm --check-config.",
    )
    args = parser.parse_args()

    root = _repo_root()
    db_path = Path(args.db).resolve()

    if not _run_unittest_discovery():
        print("\nQUALITY GATE: FAIL (deterministic suite)")
        return 1

    advisory_failures: list[str] = []

    live_ok = _run_live_endpoint_test(
        require_no_skips=args.strict_live or not args.allow_live_skip,
        strict_live=args.strict_live,
    )
    if not live_ok:
        if args.hard_fail_live or args.strict_live:
            print("\nQUALITY GATE: FAIL (live vitality)")
            return 1
        advisory_failures.append("live vitality")
        print("\nQUALITY GATE: ADVISORY WARNING (live vitality)")

    if args.run_crawl and args.skip_crawl:
        print("\nQUALITY GATE: FAIL (--run-crawl and --skip-crawl are mutually exclusive)")
        return 1

    if args.run_crawl:
        crawl_ok = _run_bounded_crawl(
            root,
            args.seeds_file,
            args.max_publications,
            args.delay,
            args.crawl_max_attempts,
            enable_comments=args.crawl_enable_comments,
            comment_post_limit=args.crawl_comment_post_limit,
            classify_commenters=args.crawl_classify_commenters,
            classification_max_users=args.crawl_classification_max_users,
            classification_workers=args.crawl_classification_workers,
        )
        if not crawl_ok:
            if args.hard_fail_live:
                print("\nQUALITY GATE: FAIL (bounded crawl)")
                return 1
            advisory_failures.append("bounded crawl")
            print("\nQUALITY GATE: ADVISORY WARNING (bounded crawl)")
    else:
        print("\n[3/7] Bounded crawl skipped (use --run-crawl to enforce)")

    if not _run_audit_and_repair_loop(db_path, args.max_repair_rounds):
        print("\nQUALITY GATE: FAIL (audit/repair)")
        return 1

    if args.run_wrapper_checks:
        if not _run_wrapper_checks(root):
            print("\nQUALITY GATE: FAIL (wrapper validation)")
            return 1
    else:
        print("\n[5/7] Wrapper validation skipped (use --run-wrapper-checks to enforce)")

    if not _run_post_repair_deterministic_suite():
        print("\nQUALITY GATE: FAIL (post-repair deterministic suite)")
        return 1

    _print_commit_step()
    if advisory_failures:
        print("\nQUALITY GATE: PASS WITH ADVISORY WARNINGS")
        print("  advisory failures: " + ", ".join(advisory_failures))
    else:
        print("\nQUALITY GATE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
