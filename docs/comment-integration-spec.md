# Comment Integration Spec

This spec describes the implemented contract for comment enrichment inside
the publication crawler.

## Goals

- Add optional comment enrichment to `scripts/milestone01/crawl.py`.
- Keep publication crawl behavior stable when comment endpoints fail.
- Emit per-publication comment ingestion stats for operational visibility.

## CLI flags

- `--enable-comments`
  - Default: `False`.
  - When true, run comment enrichment for each successfully crawled publication.
- `--comment-post-limit`
  - Default: `20`.
  - Maximum number of archive posts per publication to scan for comments.
- `--comment-timeout`
  - Default: `15.0`.
  - Timeout in seconds for comment/archive endpoint requests.
- `--comment-retries`
  - Default: `3`.
  - Retry attempts for transient comment/archive request failures.
- `--classify-commenters`
  - Default: `False`.
  - When true, classify commenters as publication owners/writers using Substack `public_profile`.
- `--classification-max-users`
  - Default: `25`.
  - Maximum distinct commenter handles to classify per publication.
- `--classification-workers`
  - Default: `4`.
  - Number of worker threads for profile lookups when classification is enabled.

## Behavioral contract

- Integration entrypoint: invoke `scripts.comments.comment_pipeline.process_comments(...)` only after a publication is successfully processed by the BFS crawler.
- Fail-open guarantee:
  - Any exception or zero-result condition in comment ingestion must not change publication crawl outcome.
  - Publication status updates (`crawled` / `failed`) remain owned by existing crawl logic only.
- Structured per-publication logging:
  - On completion, emit one stats log containing publication identifier and counts (`posts`, `users`, `comments`).
  - If classification is enabled, include `classified_users` and `classified_owners`.
  - On failure, emit one error log prefixed with `[comments][error]` and continue crawler loop.

- Classification contract (`--classify-commenters`):
  - Profile endpoint: `https://substack.com/api/v1/user/{handle}/public_profile`.
  - Rule is strict `admin + hasPosts`:
    - `users.is_publication_owner = 1` only when profile indicates both admin-level ownership and `hasPosts = true`.
  - `users.publication_id` is updated from resolved profile publication metadata when available.
  - Lookup failures are fail-open and never change publication queue status.

## Non-goals for this step

- No change to recommendation graph traversal semantics.
- No change to queue prioritization or status lifecycle.
- No hard dependency on comments for graph completeness.
