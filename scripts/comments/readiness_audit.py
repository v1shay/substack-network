#!/usr/bin/env python3
"""Bounded live-DB readiness audit for full-surface comment crawling."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import subprocess
import sys
import time
from typing import Any
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.comments.db_audit import audit_db, summarize
from scripts.db_runtime import connect_db, ensure_schema


DEFAULT_STAGE1 = {
    "comment_post_limit": 10,
    "classification_max_users": 100,
    "classification_workers": 4,
    "max_publications": 25,
    "max_attempts": 60,
    "delay": 0.25,
}

DEFAULT_STAGE2 = {
    "comment_post_limit": 20,
    "classification_max_users": 250,
    "classification_workers": 4,
    "max_publications": 100,
    "max_attempts": 250,
    "delay": 0.25,
}

USER_CONTAINER_KEYS = ("user", "commenter", "author", "creator")
USER_IDENTITY_KEYS = (
    "id",
    "user_id",
    "handle",
    "username",
    "slug",
    "name",
    "full_name",
    "display_name",
    "profile_url",
    "url",
)


class AuditFailure(RuntimeError):
    """Raised when a hard launch gate fails."""


@dataclass(frozen=True)
class StageConfig:
    name: str
    comment_post_limit: int
    classification_max_users: int
    classification_workers: int
    max_publications: int
    max_attempts: int | None
    delay: float
    enable_comments: bool = True
    classify_commenters: bool = True


def _repo_root() -> Path:
    return Path(os.environ.get("CARTOGRAPHER_ROOT", REPO_ROOT)).resolve()


def _timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_text(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def _command_display(cmd: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def _git_output(repo_root: Path, args: list[str], *, allow_nonzero: bool = False) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0 and not allow_nonzero:
        raise AuditFailure(f"failed to run git {' '.join(args)}: {completed.stderr.strip()}")
    return completed.stdout.strip()


def _git_branch(repo_root: Path) -> str:
    return _git_output(repo_root, ["branch", "--show-current"])


def _git_head_sha(repo_root: Path) -> str:
    return _git_output(repo_root, ["rev-parse", "HEAD"])


def _git_status_short(repo_root: Path) -> str:
    return _git_output(repo_root, ["status", "--short"])


def _crawler_lock_path(root: Path) -> Path:
    return root / ".crawler.lock"


def _assert_no_running_crawler(root: Path) -> None:
    lock = _crawler_lock_path(root)
    if not lock.exists():
        return

    try:
        pid = int(lock.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        lock.unlink(missing_ok=True)
        return

    try:
        os.kill(pid, 0)
    except OSError:
        lock.unlink(missing_ok=True)
        return

    raise AuditFailure(f"crawler already running with PID {pid}; stop it before launching readiness audit")


def _backup_db(db_path: Path, backup_path: Path) -> None:
    src = sqlite3.connect(str(db_path))
    dst = sqlite3.connect(str(backup_path))
    try:
        src.backup(dst)
        dst.commit()
    finally:
        dst.close()
        src.close()


def _path_is_tracked(repo_root: Path, path: Path) -> bool:
    completed = subprocess.run(
        ["git", "ls-files", "--error-unmatch", str(path)],
        cwd=repo_root,
        text=True,
        capture_output=True,
    )
    return completed.returncode == 0


def capture_code_fingerprint(repo_root: Path, artifact_dir: Path, diff_paths: list[Path]) -> dict[str, str]:
    branch = _git_branch(repo_root)
    head_sha = _git_head_sha(repo_root)
    status_short = _git_status_short(repo_root)

    branch_path = artifact_dir / "branch.txt"
    head_path = artifact_dir / "head.txt"
    status_path = artifact_dir / "git-status-short.txt"
    diff_path = artifact_dir / "audit-files.diff"

    _write_text(branch_path, f"{branch}\n")
    _write_text(head_path, f"{head_sha}\n")
    _write_text(status_path, f"{status_short}\n" if status_short else "")

    combined_diff_parts: list[str] = []
    for path in diff_paths:
        if _path_is_tracked(repo_root, path):
            diff = _git_output(repo_root, ["diff", "--", str(path)], allow_nonzero=True)
        else:
            completed = subprocess.run(
                ["git", "diff", "--no-index", "--", "/dev/null", str(path)],
                cwd=repo_root,
                text=True,
                capture_output=True,
            )
            if completed.returncode not in (0, 1):
                raise AuditFailure(f"failed to diff untracked path {path}: {completed.stderr.strip()}")
            diff = completed.stdout
        if diff:
            combined_diff_parts.append(diff.rstrip() + "\n")
    _write_text(diff_path, "".join(combined_diff_parts))

    return {
        "branch_file": str(branch_path),
        "head_file": str(head_path),
        "status_file": str(status_path),
        "diff_file": str(diff_path),
    }


def _table_count(cur: sqlite3.Cursor, table_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cur.fetchone()[0])


def _max_id(cur: sqlite3.Cursor, table_name: str) -> int:
    cur.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}")
    return int(cur.fetchone()[0])


def collect_db_snapshot(db_path: Path | str) -> dict[str, Any]:
    conn = connect_db(str(db_path))
    try:
        ensure_schema(conn)
        cur = conn.cursor()
        counts = {
            name: _table_count(cur, name)
            for name in ("publications", "recommendations", "queue", "users", "posts", "comments")
        }
        max_ids = {
            name: _max_id(cur, name)
            for name in ("users", "posts", "comments")
        }
        cur.execute("SELECT status, COUNT(*) FROM queue GROUP BY status")
        queue_status_counts = {str(status): int(count) for status, count in cur.fetchall()}
        return {
            "counts": counts,
            "max_ids": max_ids,
            "queue_status_counts": queue_status_counts,
        }
    finally:
        conn.close()


def anomaly_increases(baseline_counts: dict[str, int], current_counts: dict[str, int]) -> dict[str, dict[str, int]]:
    increases: dict[str, dict[str, int]] = {}
    for name, current_value in current_counts.items():
        baseline_value = int(baseline_counts.get(name, 0))
        if int(current_value) > baseline_value:
            increases[name] = {
                "baseline": baseline_value,
                "current": int(current_value),
                "delta": int(current_value) - baseline_value,
            }
    return increases


def _run_capture(cmd: list[str], *, cwd: Path, env: dict[str, str], log_path: Path) -> int:
    completed = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
    )
    combined = ""
    if completed.stdout:
        combined += completed.stdout
    if completed.stderr:
        if combined and not combined.endswith("\n"):
            combined += "\n"
        combined += completed.stderr
    log_path.write_text(combined, encoding="utf-8")
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, file=sys.stderr, end="")
    return completed.returncode


def _run_logged(cmd: list[str], *, cwd: Path, env: dict[str, str], log_path: Path) -> dict[str, Any]:
    started_at = time.monotonic()
    with log_path.open("w", encoding="utf-8") as handle:
        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="")
            handle.write(line)
            handle.flush()
        returncode = process.wait()
    return {
        "command": cmd,
        "command_display": _command_display(cmd),
        "returncode": returncode,
        "elapsed_seconds": time.monotonic() - started_at,
    }


def _processed_publications_from_log(log_text: str) -> int:
    matches = re.findall(r"Processed (\d+) publications\.", log_text)
    if not matches:
        return 0
    return int(matches[-1])


def _error_domains_from_log(log_text: str) -> set[str]:
    domains: set[str] = set()
    for line in log_text.splitlines():
        if "[comments][error]" not in line:
            continue

        domain_match = re.search(r"\[comments\]\[error\]\s+domain=([^:\s]+)", line)
        if domain_match:
            domains.add(domain_match.group(1))
            continue

        url_match = re.search(r"(https?://\S+)", line)
        if not url_match:
            continue

        url = url_match.group(1)
        if "/api/v1/post/" not in url and "/api/v1/posts/" not in url:
            continue

        host = urlparse(url).netloc.strip().lower()
        if host:
            domains.add(host)
    return domains


def evaluate_comment_error_budget(stage_log_paths: dict[str, Path], *, max_rate: float) -> dict[str, Any]:
    per_stage: dict[str, dict[str, Any]] = {}
    all_error_domains: set[str] = set()
    total_processed = 0

    for stage_name, log_path in stage_log_paths.items():
        log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
        processed = _processed_publications_from_log(log_text)
        error_domains = _error_domains_from_log(log_text)
        per_stage[stage_name] = {
            "processed_publications": processed,
            "error_domains": sorted(error_domains),
            "error_publications": len(error_domains),
        }
        total_processed += processed
        all_error_domains.update(error_domains)

    total_error_publications = len(all_error_domains)
    if total_processed <= 0:
        error_rate = 1.0 if total_error_publications > 0 else 0.0
    else:
        error_rate = total_error_publications / total_processed

    return {
        "per_stage": per_stage,
        "total_crawled_publications": total_processed,
        "comment_error_publications": total_error_publications,
        "comment_error_domains": sorted(all_error_domains),
        "comment_error_rate": error_rate,
        "max_comment_error_rate": max_rate,
        "passes": error_rate <= max_rate,
    }


def _stage_command(repo_root: Path, seeds_file: str, stage: StageConfig) -> list[str]:
    seed_path = Path(seeds_file)
    resolved_seed_path = seed_path.resolve() if seed_path.is_absolute() else (repo_root / seed_path).resolve()
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "milestone01" / "crawl.py"),
        "--seeds-file",
        str(resolved_seed_path),
    ]
    if stage.enable_comments:
        cmd.extend(["--enable-comments", "--comment-post-limit", str(stage.comment_post_limit)])
    if stage.classify_commenters:
        cmd.extend(
            [
                "--classify-commenters",
                "--classification-max-users",
                str(stage.classification_max_users),
                "--classification-workers",
                str(stage.classification_workers),
            ]
        )
    cmd.extend(["--max-publications", str(stage.max_publications)])
    if stage.max_attempts is not None:
        cmd.extend(["--max-attempts", str(stage.max_attempts)])
    cmd.extend(["--delay", str(stage.delay)])
    return cmd


def _parse_raw_json(raw_json: str | None) -> dict[str, Any]:
    if raw_json in (None, ""):
        return {}
    try:
        parsed = json.loads(raw_json)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _valid_timestamp(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if not candidate:
        return False
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return True


def _body_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _user_identity_explicitly_unavailable(raw: dict[str, Any]) -> bool:
    for key in USER_CONTAINER_KEYS:
        if key not in raw:
            continue
        value = raw.get(key)
        if value in (None, "", {}):
            return True
        if isinstance(value, dict):
            if not any(value.get(field) not in (None, "") for field in USER_IDENTITY_KEYS):
                return True
    return "user_id" in raw and raw.get("user_id") in (None, "")


def _append_sample(samples: dict[str, list[dict[str, Any]]], key: str, row: dict[str, Any], limit: int) -> None:
    bucket = samples.setdefault(key, [])
    if len(bucket) >= limit:
        return
    bucket.append(row)


def audit_delta_comment_metadata(
    db_path: Path | str,
    *,
    baseline_max_ids: dict[str, int],
    sample_limit: int = 20,
) -> dict[str, Any]:
    conn = connect_db(str(db_path))
    try:
        ensure_schema(conn)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM users WHERE id > ?", (int(baseline_max_ids["users"]),))
        delta_users = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM posts WHERE id > ?", (int(baseline_max_ids["posts"]),))
        delta_posts = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM comments WHERE id > ?", (int(baseline_max_ids["comments"]),))
        delta_comments = int(cur.fetchone()[0])

        metrics = {
            "delta_users": delta_users,
            "delta_posts": delta_posts,
            "delta_comments": delta_comments,
            "comments_missing_post_id": 0,
            "comments_invalid_commented_at": 0,
            "reply_comments": 0,
            "reply_comments_missing_parent_comment_id": 0,
            "comments_orphan_post_id": 0,
            "comments_missing_post_publication_substack_id": 0,
            "comments_missing_publication_join": 0,
            "comments_missing_user_id": 0,
            "comments_missing_user_id_allowed_edge_case": 0,
            "comments_missing_user_id_blocking": 0,
            "comments_missing_body": 0,
            "comments_missing_body_allowed_deleted": 0,
            "comments_missing_body_blocking": 0,
            "comments_missing_external_comment_id": 0,
        }
        samples: dict[str, list[dict[str, Any]]] = {}

        cur.execute(
            """
            SELECT
                c.id,
                c.external_comment_id,
                c.user_id,
                c.post_id,
                c.parent_comment_id,
                c.parent_external_comment_id,
                c.body,
                c.commented_at,
                c.raw_json,
                p.id,
                p.publication_substack_id,
                pub.id
            FROM comments c
            LEFT JOIN posts p
              ON p.id = c.post_id
            LEFT JOIN publications pub
              ON pub.substack_id = p.publication_substack_id
            WHERE c.id > ?
            ORDER BY c.id ASC
            """,
            (int(baseline_max_ids["comments"]),),
        )
        rows = cur.fetchall()

        for (
            comment_id,
            external_comment_id,
            user_id,
            post_id,
            parent_comment_id,
            parent_external_comment_id,
            body,
            commented_at,
            raw_json,
            joined_post_id,
            publication_substack_id,
            joined_publication_id,
        ) in rows:
            raw = _parse_raw_json(raw_json)
            sample = {
                "comment_id": int(comment_id),
                "external_comment_id": external_comment_id,
                "user_id": user_id,
                "post_id": post_id,
                "parent_comment_id": parent_comment_id,
                "parent_external_comment_id": parent_external_comment_id,
                "commented_at": commented_at,
                "publication_substack_id": publication_substack_id,
                "raw_json": raw,
            }

            if external_comment_id in (None, ""):
                metrics["comments_missing_external_comment_id"] += 1

            if post_id is None:
                metrics["comments_missing_post_id"] += 1
                _append_sample(samples, "comments_missing_post_id", sample, sample_limit)
            elif joined_post_id is None:
                metrics["comments_orphan_post_id"] += 1
                _append_sample(samples, "comments_orphan_post_id", sample, sample_limit)
            elif publication_substack_id is None or str(publication_substack_id).strip() == "":
                metrics["comments_missing_post_publication_substack_id"] += 1
                _append_sample(samples, "comments_missing_post_publication_substack_id", sample, sample_limit)
            elif joined_publication_id is None:
                metrics["comments_missing_publication_join"] += 1
                _append_sample(samples, "comments_missing_publication_join", sample, sample_limit)

            if not _valid_timestamp(commented_at):
                metrics["comments_invalid_commented_at"] += 1
                _append_sample(samples, "comments_invalid_commented_at", sample, sample_limit)

            if parent_external_comment_id not in (None, ""):
                metrics["reply_comments"] += 1
                if parent_comment_id is None:
                    metrics["reply_comments_missing_parent_comment_id"] += 1
                    _append_sample(samples, "reply_comments_missing_parent_comment_id", sample, sample_limit)

            if user_id is None:
                metrics["comments_missing_user_id"] += 1
                if _user_identity_explicitly_unavailable(raw):
                    metrics["comments_missing_user_id_allowed_edge_case"] += 1
                else:
                    metrics["comments_missing_user_id_blocking"] += 1
                    _append_sample(samples, "comments_missing_user_id_blocking", sample, sample_limit)

            if _body_missing(body):
                metrics["comments_missing_body"] += 1
                if raw.get("deleted") is True:
                    metrics["comments_missing_body_allowed_deleted"] += 1
                else:
                    metrics["comments_missing_body_blocking"] += 1
                    _append_sample(samples, "comments_missing_body_blocking", sample, sample_limit)

        cur.execute(
            """
            SELECT
                COUNT(*) AS total_users,
                SUM(CASE WHEN publication_substack_id IS NOT NULL AND TRIM(publication_substack_id) <> '' THEN 1 ELSE 0 END),
                SUM(CASE WHEN publication_role IS NOT NULL AND TRIM(publication_role) <> '' THEN 1 ELSE 0 END),
                SUM(CASE WHEN is_publication_owner = 1 THEN 1 ELSE 0 END)
            FROM users
            WHERE id > ?
            """,
            (int(baseline_max_ids["users"]),),
        )
        total_delta_users, classified_users, role_tagged_users, owner_users = cur.fetchone()
        advisory = {
            "delta_users_total": int(total_delta_users or 0),
            "users_with_publication_substack_id": int(classified_users or 0),
            "users_with_publication_role": int(role_tagged_users or 0),
            "users_marked_publication_owner": int(owner_users or 0),
            "comments_missing_external_comment_id": metrics["comments_missing_external_comment_id"],
        }

        hard_failures: list[str] = []
        if metrics["comments_missing_post_id"] > 0:
            hard_failures.append("new comments missing post_id")
        if metrics["comments_invalid_commented_at"] > 0:
            hard_failures.append("new comments missing valid commented_at")
        if metrics["reply_comments_missing_parent_comment_id"] > 0:
            hard_failures.append("reply comments missing parent_comment_id")
        if metrics["comments_orphan_post_id"] > 0:
            hard_failures.append("new comments reference post_id values that do not join to posts.id")
        if metrics["comments_missing_post_publication_substack_id"] > 0:
            hard_failures.append("new comments belong to posts missing publication_substack_id")
        if metrics["comments_missing_publication_join"] > 0:
            hard_failures.append("new comments cannot join to publications via posts.publication_substack_id")
        if metrics["comments_missing_user_id_blocking"] > 0:
            hard_failures.append("new comments missing user_id without explicit unavailable-user payload markers")
        if metrics["comments_missing_body_blocking"] > 0:
            hard_failures.append("new comments missing raw text without deleted=true payload markers")

        return {
            "baseline_max_ids": baseline_max_ids,
            "metrics": metrics,
            "advisory": advisory,
            "samples": samples,
            "hard_failures": hard_failures,
        }
    finally:
        conn.close()


def run_metadata_check_from_files(
    *,
    db_path: Path,
    baseline_snapshot_path: Path,
    output_path: Path,
    sample_limit: int = 20,
) -> int:
    baseline_snapshot = json.loads(baseline_snapshot_path.read_text(encoding="utf-8"))
    report = audit_delta_comment_metadata(
        db_path,
        baseline_max_ids={
            "users": int(baseline_snapshot["max_ids"]["users"]),
            "posts": int(baseline_snapshot["max_ids"]["posts"]),
            "comments": int(baseline_snapshot["max_ids"]["comments"]),
        },
        sample_limit=sample_limit,
    )
    _write_json(output_path, report)
    return 1 if report["hard_failures"] else 0


def write_metadata_check_script(
    *,
    repo_root: Path,
    db_path: Path,
    baseline_snapshot_path: Path,
    output_path: Path,
    script_path: Path,
    sample_limit: int = 20,
) -> None:
    script_body = f"""#!/usr/bin/env python3
from pathlib import Path
import sys

REPO_ROOT = Path({str(repo_root)!r})
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.comments.readiness_audit import run_metadata_check_from_files

if __name__ == "__main__":
    raise SystemExit(
        run_metadata_check_from_files(
            db_path=Path({str(db_path)!r}),
            baseline_snapshot_path=Path({str(baseline_snapshot_path)!r}),
            output_path=Path({str(output_path)!r}),
            sample_limit={sample_limit},
        )
    )
"""
    script_path.write_text(script_body, encoding="utf-8")
    script_path.chmod(0o755)


def _stage_artifact_paths(artifact_dir: Path, stage_name: str) -> dict[str, Path]:
    return {
        "log": artifact_dir / f"{stage_name}-crawl.log",
        "snapshot": artifact_dir / f"{stage_name}-snapshot.json",
        "audit": artifact_dir / f"{stage_name}-audit.json",
        "metadata_script": artifact_dir / f"{stage_name}-metadata-check.py",
        "metadata_report": artifact_dir / f"{stage_name}-metadata.json",
        "summary": artifact_dir / f"{stage_name}-summary.json",
    }


def _project_comments(delta_comments: int, elapsed_seconds: float) -> dict[str, float]:
    if elapsed_seconds <= 0:
        return {
            "comments_per_hour": 0.0,
            "projected_comments_8h": 0.0,
            "projected_comments_24h": 0.0,
        }
    comments_per_hour = (float(delta_comments) / elapsed_seconds) * 3600.0
    return {
        "comments_per_hour": comments_per_hour,
        "projected_comments_8h": comments_per_hour * 8.0,
        "projected_comments_24h": comments_per_hour * 24.0,
    }


def _assert_no_anomaly_regressions(
    *,
    baseline_anomalies: dict[str, int],
    current_anomalies: dict[str, int],
    stage_name: str,
) -> None:
    increases = anomaly_increases(baseline_anomalies, current_anomalies)
    if increases:
        raise AuditFailure(f"{stage_name} increased anomaly counters: {sorted(increases)}")


def _run_stage(
    *,
    repo_root: Path,
    db_path: Path,
    artifact_dir: Path,
    seeds_file: str,
    stage: StageConfig,
    baseline_snapshot_path: Path,
    baseline_anomalies: dict[str, int],
    sample_limit: int,
    projection_thresholds: dict[str, int] | None = None,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["CARTOGRAPHER_ROOT"] = str(repo_root)
    paths = _stage_artifact_paths(artifact_dir, stage.name)
    command = _stage_command(repo_root, seeds_file, stage)
    command_result = _run_logged(command, cwd=repo_root, env=env, log_path=paths["log"])
    snapshot = collect_db_snapshot(db_path)
    _write_json(paths["snapshot"], snapshot)

    audit_summary = summarize(audit_db(db_path))
    _write_json(paths["audit"], audit_summary)
    _assert_no_anomaly_regressions(
        baseline_anomalies=baseline_anomalies,
        current_anomalies=audit_summary["counters"],
        stage_name=stage.name,
    )

    write_metadata_check_script(
        repo_root=repo_root,
        db_path=db_path,
        baseline_snapshot_path=baseline_snapshot_path,
        output_path=paths["metadata_report"],
        script_path=paths["metadata_script"],
        sample_limit=sample_limit,
    )
    metadata_rc = subprocess.run(
        [sys.executable, str(paths["metadata_script"])],
        cwd=repo_root,
        env=env,
    ).returncode
    metadata_report = json.loads(paths["metadata_report"].read_text(encoding="utf-8"))

    stage_summary: dict[str, Any] = {
        "stage": asdict(stage),
        "command": command_result["command"],
        "command_display": command_result["command_display"],
        "returncode": command_result["returncode"],
        "elapsed_seconds": command_result["elapsed_seconds"],
        "snapshot": snapshot,
        "audit": audit_summary,
        "metadata_report": metadata_report,
        "metadata_returncode": metadata_rc,
        "artifacts": {name: str(path) for name, path in paths.items()},
    }

    if command_result["returncode"] != 0:
        _write_json(paths["summary"], stage_summary)
        raise AuditFailure(f"{stage.name} crawl exited with status {command_result['returncode']}")
    if metadata_rc != 0:
        _write_json(paths["summary"], stage_summary)
        raise AuditFailure(f"{stage.name} metadata validation failed")

    if projection_thresholds is not None:
        delta_comments = int(metadata_report["metrics"]["delta_comments"])
        projection = _project_comments(delta_comments, float(command_result["elapsed_seconds"]))
        stage_summary["projection"] = projection
        if projection["projected_comments_8h"] < float(projection_thresholds["8h"]):
            _write_json(paths["summary"], stage_summary)
            raise AuditFailure(
                f"{stage.name} projected 8h comment volume {projection['projected_comments_8h']:.2f} is below threshold {projection_thresholds['8h']}"
            )
        if projection["projected_comments_24h"] < float(projection_thresholds["24h"]):
            _write_json(paths["summary"], stage_summary)
            raise AuditFailure(
                f"{stage.name} projected 24h comment volume {projection['projected_comments_24h']:.2f} is below threshold {projection_thresholds['24h']}"
            )

    _write_json(paths["summary"], stage_summary)
    return stage_summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a bounded live-DB readiness audit for full-surface comment crawling."
    )
    parser.add_argument(
        "--db",
        default=str(_repo_root() / "cartographer.db"),
        help="Path to the live SQLite database to audit.",
    )
    parser.add_argument(
        "--seeds-file",
        default="config/seeds.md",
        help="Seeds file passed to crawl.py.",
    )
    parser.add_argument(
        "--artifact-base-dir",
        default="audit",
        help="Directory under runtime root where audit artifacts will be written.",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="Optional timestamp slug for the artifact directory.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help="Maximum sampled rows to retain per blocking metadata category.",
    )
    parser.add_argument(
        "--min-projected-comments-8h",
        type=int,
        default=10000,
        help="Minimum projected comments after 8 hours from stage 2 throughput.",
    )
    parser.add_argument(
        "--min-projected-comments-24h",
        type=int,
        default=30000,
        help="Minimum projected comments after 24 hours from stage 2 throughput.",
    )
    parser.add_argument("--stage1-comment-post-limit", type=int, default=DEFAULT_STAGE1["comment_post_limit"])
    parser.add_argument("--stage1-classification-max-users", type=int, default=DEFAULT_STAGE1["classification_max_users"])
    parser.add_argument("--stage1-classification-workers", type=int, default=DEFAULT_STAGE1["classification_workers"])
    parser.add_argument("--stage1-max-publications", type=int, default=DEFAULT_STAGE1["max_publications"])
    parser.add_argument("--stage1-max-attempts", type=int, default=DEFAULT_STAGE1["max_attempts"])
    parser.add_argument("--stage1-delay", type=float, default=DEFAULT_STAGE1["delay"])
    parser.add_argument("--stage2-comment-post-limit", type=int, default=DEFAULT_STAGE2["comment_post_limit"])
    parser.add_argument("--stage2-classification-max-users", type=int, default=DEFAULT_STAGE2["classification_max_users"])
    parser.add_argument("--stage2-classification-workers", type=int, default=DEFAULT_STAGE2["classification_workers"])
    parser.add_argument("--stage2-max-publications", type=int, default=DEFAULT_STAGE2["max_publications"])
    parser.add_argument("--stage2-max-attempts", type=int, default=DEFAULT_STAGE2["max_attempts"])
    parser.add_argument("--stage2-delay", type=float, default=DEFAULT_STAGE2["delay"])
    parser.add_argument(
        "--max-comment-error-rate",
        type=float,
        default=0.01,
        help="Maximum allowed unique-publication comment error rate across stage 1 and stage 2 logs.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        raise AuditFailure(f"database does not exist: {db_path}")

    timestamp = args.timestamp or _timestamp_slug()
    artifact_dir = (repo_root / args.artifact_base_dir / timestamp).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=False)

    summary_path = artifact_dir / "summary.json"
    branch = _git_branch(repo_root)
    if branch != "graphs/llms":
        raise AuditFailure(f"readiness audit must run on graphs/llms; current branch is {branch!r}")
    _assert_no_running_crawler(repo_root)

    diff_paths = [
        repo_root / "README.md",
        repo_root / "docs" / "comment-pipeline.md",
        repo_root / "scripts" / "comments" / "readiness_audit.py",
        repo_root / "tests" / "test_readiness_audit.py",
    ]

    summary: dict[str, Any] = {
        "repo_root": str(repo_root),
        "db_path": str(db_path),
        "artifact_dir": str(artifact_dir),
        "branch": branch,
        "launch_ready": False,
        "stages": {},
    }
    _write_json(summary_path, summary)

    backup_path = artifact_dir / "cartographer-pre.db"
    pre_snapshot_path = artifact_dir / "pre-snapshot.json"
    pre_audit_path = artifact_dir / "pre-audit.json"

    env = os.environ.copy()
    env["CARTOGRAPHER_ROOT"] = str(repo_root)

    try:
        deterministic_log = artifact_dir / "preflight-deterministic.log"
        deterministic_rc = _run_capture(
            [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py", "-v"],
            cwd=repo_root,
            env=env,
            log_path=deterministic_log,
        )
        if deterministic_rc != 0:
            raise AuditFailure("deterministic preflight suite failed")

        live_log = artifact_dir / "preflight-live.log"
        live_env = env.copy()
        live_env["SUBSTACK_LIVE_TESTS"] = "1"
        live_env["SUBSTACK_STRICT_LIVE"] = "1"
        live_rc = _run_capture(
            [sys.executable, "-m", "unittest", "tests.test_substack_endpoint_vitality", "-v"],
            cwd=repo_root,
            env=live_env,
            log_path=live_log,
        )
        if live_rc != 0:
            raise AuditFailure("strict live endpoint preflight failed")

        fingerprint = capture_code_fingerprint(repo_root, artifact_dir, diff_paths)

        _backup_db(db_path, backup_path)
        pre_snapshot = collect_db_snapshot(db_path)
        _write_json(pre_snapshot_path, pre_snapshot)
        pre_audit = summarize(audit_db(db_path))
        _write_json(pre_audit_path, pre_audit)

        summary["preflight"] = {
            "deterministic_log": str(deterministic_log),
            "live_log": str(live_log),
            "backup_path": str(backup_path),
            "pre_snapshot_path": str(pre_snapshot_path),
            "pre_audit_path": str(pre_audit_path),
        }
        summary["fingerprint"] = fingerprint
        _write_json(summary_path, summary)

        stage1 = StageConfig(
            name="stage1",
            comment_post_limit=args.stage1_comment_post_limit,
            classification_max_users=args.stage1_classification_max_users,
            classification_workers=args.stage1_classification_workers,
            max_publications=args.stage1_max_publications,
            max_attempts=args.stage1_max_attempts,
            delay=args.stage1_delay,
        )
        stage1_summary = _run_stage(
            repo_root=repo_root,
            db_path=db_path,
            artifact_dir=artifact_dir,
            seeds_file=args.seeds_file,
            stage=stage1,
            baseline_snapshot_path=pre_snapshot_path,
            baseline_anomalies=pre_audit["counters"],
            sample_limit=args.sample_limit,
        )
        summary["stages"]["stage1"] = stage1_summary
        _write_json(summary_path, summary)

        stage1_snapshot_path = _stage_artifact_paths(artifact_dir, "stage1")["snapshot"]
        stage2 = StageConfig(
            name="stage2",
            comment_post_limit=args.stage2_comment_post_limit,
            classification_max_users=args.stage2_classification_max_users,
            classification_workers=args.stage2_classification_workers,
            max_publications=args.stage2_max_publications,
            max_attempts=args.stage2_max_attempts,
            delay=args.stage2_delay,
        )
        stage2_summary = _run_stage(
            repo_root=repo_root,
            db_path=db_path,
            artifact_dir=artifact_dir,
            seeds_file=args.seeds_file,
            stage=stage2,
            baseline_snapshot_path=stage1_snapshot_path,
            baseline_anomalies=pre_audit["counters"],
            sample_limit=args.sample_limit,
            projection_thresholds={
                "8h": args.min_projected_comments_8h,
                "24h": args.min_projected_comments_24h,
            },
        )
        summary["stages"]["stage2"] = stage2_summary

        comment_error_budget = evaluate_comment_error_budget(
            {
                "stage1": _stage_artifact_paths(artifact_dir, "stage1")["log"],
                "stage2": _stage_artifact_paths(artifact_dir, "stage2")["log"],
            },
            max_rate=args.max_comment_error_rate,
        )
        comment_error_budget_path = artifact_dir / "comment-error-budget.json"
        _write_json(comment_error_budget_path, comment_error_budget)
        summary["comment_error_budget"] = {
            **comment_error_budget,
            "report_path": str(comment_error_budget_path),
        }
        if not comment_error_budget["passes"]:
            raise AuditFailure(
                f"comment error rate {comment_error_budget['comment_error_rate']:.6f} exceeded max rate {args.max_comment_error_rate:.6f}"
            )

        summary["launch_ready"] = True
        _write_json(summary_path, summary)
        print(f"Readiness audit passed. Artifacts written to {artifact_dir}")
        return 0
    except AuditFailure as exc:
        summary["launch_ready"] = False
        summary["failure"] = str(exc)
        _write_json(summary_path, summary)
        print(f"Readiness audit failed: {exc}", file=sys.stderr)
        print(f"Artifacts preserved at {artifact_dir}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
