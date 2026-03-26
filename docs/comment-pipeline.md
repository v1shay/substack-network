# Comment Pipeline

The comment ingestion pipeline is now available in two modes:

- standalone/manual via `scripts/comments/comment_pipeline.py`
- integrated into `scripts/milestone01/crawl.py` behind `--enable-comments`

Integration behavior and flags are documented in
[comment-integration-spec.md](./comment-integration-spec.md).

## Modules

- `scripts/comments/comment_api.py`
  - `fetch_archive(publication_url, ...)`
  - `fetch_post_comments(publication_url, post_id, ...)`
  - Handles retries, redirects, timeouts, malformed JSON, and pagination.
- `scripts/comments/parsers.py`
  - `extract_posts_from_archive(payload)`
  - `extract_comments_from_response(payload)`
  - `normalize_user(comment)`
  - `normalize_comment(comment, ...)`
  - Flattens nested comment trees and normalizes missing/deleted fields.
- `scripts/comments/db_helpers.py`
  - `insert_user_if_not_exists(conn, user)`
  - `insert_post_if_not_exists(conn, post)`
  - `insert_comment_if_not_exists(conn, comment, ...)`
  - Idempotent, duplicate-safe SQLite writes.
- `scripts/comments/comment_pipeline.py`
  - `discover_posts(publication_url, limit, ...)`
  - `fetch_comments_for_post(publication_url, post_id, ...)`
  - `process_comments(publication_url, ...)`
  - End-to-end call path used by standalone runs and crawler integration.
- `scripts/comments/user_classifier.py`
  - `fetch_public_profile(handle, ...)`
  - `classify_profile(profile)`
  - `classify_users(conn, user_ids, ...)`
  - Optional profile-based commenter classification (`admin + hasPosts`) behind crawler flag.

## Logging

The pipeline emits structured messages:

- `[comments] archive fetched`
- `[comments] comments fetched`
- `[comments] parsing success`
- `[comments] db insert success`
- `[comments][error] ...`

## Standalone CLI

Run from repo root:

```bash
source .venv/bin/activate
python scripts/comments/comment_pipeline.py paulkrugman.substack.com --post-limit 3
```

Common options:

```bash
python scripts/comments/comment_pipeline.py paulkrugman.substack.com \
  --db /tmp/cartographer-comments.db \
  --post-limit 5 \
  --timeout 15 \
  --retries 3 \
  --classify-commenters \
  --classification-max-users 25 \
  --classification-workers 4
```

Behavior:

- The CLI is a thin wrapper around `process_comments(...)`; it does not change comment ingestion semantics.
- Default DB path: `CARTOGRAPHER_ROOT/cartographer.db` or `./cartographer.db` when `CARTOGRAPHER_ROOT` is unset.
- On success it prints one concise stats summary and exits `0`; true empty results still count as success.
- Fatal archive/comment fetch failures and other CLI/runtime failures exit `1`.

## Running tests

From repo root:

```bash
source .venv/bin/activate
python -m unittest discover -s tests -p "test_*.py" -v
```

Optional live endpoint vitality test:

```bash
SUBSTACK_LIVE_TESTS=1 python -m unittest tests.test_substack_endpoint_vitality -v
```

Strict live-only vitality test (fails if deterministic fallback would be used):

```bash
SUBSTACK_LIVE_TESTS=1 SUBSTACK_STRICT_LIVE=1 python -m unittest tests.test_substack_endpoint_vitality -v
```

## Release gate scripts

Run strict audit/repair + test gate:

```bash
source .venv/bin/activate
python scripts/comments/quality_gate.py
```

Run strict live-only release gate in DNS-healthy environments:

```bash
source .venv/bin/activate
python scripts/comments/quality_gate.py --strict-live --run-crawl --max-publications 25 --delay 0.25 --crawl-max-attempts 50
```

Run anomaly audit only:

```bash
source .venv/bin/activate
python scripts/comments/db_audit.py --fail-on-anomaly
```

Apply targeted repair rules only:

```bash
source .venv/bin/activate
python scripts/comments/db_repair.py
```
