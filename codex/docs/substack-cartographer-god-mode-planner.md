# Substack Cartographer God Mode Planner

Last updated: 2026-05-14

Use this document as the project control plane for future Codex chats. It merges three sources:

- Gmail thread with Professor Alexander Kurz, subject `Substack Project`, March 8-April 9, 2026.
- Current local codebase truth from `/Users/agarwal/coding/substack comment graphs/substack-cartographer/substack-cartographer-comment-crawling`.
- CHRONOS/Obsidian project truth in `/Users/agarwal/coding/CHRONOS/chronos/03 Projects/Substack Cartographer`.

## Operating Truth

The project is not just "crawl Substack." It is a graph-analysis system for mapping publications, recommendations, comments, users, topics, stance, and engagement dynamics.

Current implementation already supports:

- Recommendation graph crawling with BFS over publication recommendations.
- SQLite persistence in `cartographer.db`.
- PageRank / RecommendationRank over `recommendations`.
- Interactive HTML visualization.
- Failed-publication retry and investigation tooling.
- Comment ingestion from archive and comment endpoints.
- Durable historical comment backfill.
- User/commenter classification using Substack public profiles.
- Semantic embeddings scaffold for comments, posts, and publications.
- Test coverage across crawling, comments, schema, retry, update graph, embeddings, and quality gates.

Current major gap:

- The current worktree has a broken `.git` pointer:
  - `.git` points at `/Users/agarwal/coding/substack-cartographer-feature:comment-crawling/substack-cartographer/substack-cartographer/.git/worktrees/substack-cartographer-comment-crawling`.
  - `git status` fails from the current path.
  - No commit plan can be executed safely until canonical repo/path and Git metadata are repaired.

## Professor Kurz Thread Requirements

### Professor-Attributed Direction

Professor Kurz explicitly raised or endorsed:

- Relevancy metric through monthly PageRank snapshots.
- Trending topics by comparing month-to-month changes.
- Crawling is slow, roughly one new publication per 10 seconds in the discussed state.
- Need to think harder about how to obtain topics.
- Need to find whether comments can be crawled because the API is undocumented.
- Use comments to identify super-connectors across topic islands.
- Store comment counts to detect active/hot topics.
- Integrate LLMs for sentiment and topic labels.
- Understand failed publications where pages work manually but the crawler cannot access them.
- Send pull requests for completed work.
- Clarify how Substack distinguishes proper publications from users who only comment.
- BFS was around layer 8/9 and shrinking; estimate full graph around 169k nodes plus more.
- Do not keep generated database churn in normal PRs without a deliberate strategy.
- Look at Dan Jurafsky's work on computational framing, stance, bias, and media.
- Distinguish `RecommendationRank` and `CommentRank`.
- Eigenvector-style CommentRank looks good.
- Self-implemented PageRank/CommentRank is valuable if the goal is learning.
- Fourier/time-series comment activity is a great idea, but data collection is expensive and must be narrowed to a specific question.

### User-Originated Feature Ideas In Thread

- Use `httpx` and `asyncio` for faster crawling with bounded concurrency.
- Use a semaphore to cap active connections and avoid rate limits.
- Separate monthly PageRank calculations from metadata retrieval.
- Crawl posts through `/api/v1/archive?sort=new&limit=...`.
- Crawl comments through `/api/v1/posts/{post_id}/comments?sort=top`.
- Allow redirects and handle domain mismatch to reduce failed publications.
- Add live comment crawling to crawler.
- Store comments and related user metadata.
- Determine user vs publication by checking public profiles and ownership/admin signals.
- Use users and replies as graph nodes/edges.
- Weight comment influence by replies and comment likes.
- Define `CommentRank` as importance from attention received from other important comments.
- Use a teleportation vector for dangling one-time comments.
- Extract comment timestamps into time series.
- Use Fourier transforms to measure engagement-frequency amplitudes and community activity rhythms.

## Codebase Map

### Core Crawl

- `scripts/milestone01/crawl.py`
  - BFS queue over publications.
  - Uses `Newsletter` from `substack_api`.
  - Gets publication metadata through direct `/api/v1/publication`, then fallback post metadata.
  - Persists publications, recommendations, and queue status.
  - Optional `--enable-comments` hook invokes `process_comments(...)`.
  - Comment enrichment is fail-open and does not decide publication crawl success.

- `scripts/crawl_persistence.py`
  - Shared queue/domain/recommendation persistence helpers.

- `scripts/db_runtime.py`
  - Source of truth for SQLite schema and migrations.
  - Current schema version: `2`.
  - Core tables: `publications`, `recommendations`, `queue`, `users`, `posts`, `comments`.
  - Sidecar tables: `comment_ingestion_runs`, `comment_publication_status`, `semantic_embedding_runs`, `semantic_embeddings`.

### RecommendationRank

- `scripts/milestone01/centrality.py`
  - Current implementation of PageRank over the recommendation graph.
  - This should be referred to as current `RecommendationRank`.

- `scripts/milestone02/pagerank_distribution.py`
  - Computes rank/PageRank distribution and power-law fit.
  - Supports Zipf-like analysis of publication centrality.

- `scripts/milestone02/add_publication_lists.py`
  - Adds graph and DB publication list pages.
  - Embeds PageRank distribution output.

### Comments

- `scripts/comments/comment_api.py`
  - Archive and comment endpoint fetching.

- `scripts/comments/parsers.py`
  - Normalizes posts, comments, nested replies, deleted/missing fields.

- `scripts/comments/db_helpers.py`
  - Inserts/upserts users, posts, comments.
  - Resolves parent comment links.

- `scripts/comments/comment_pipeline.py`
  - End-to-end comment ingestion for one publication.
  - Fetches archive posts, fetches comments per post, dedupes comments, persists rows.

- `scripts/comments/comment_backfill.py`
  - Durable historical comment ingestion for already-crawled publications.
  - Tracks run and per-publication status.

- `scripts/comments/user_classifier.py`
  - Classifies commenters via `https://substack.com/api/v1/user/{handle}/public_profile`.
  - Current rule: `admin + hasPosts`.
  - Persists `publication_substack_id`, `publication_role`, `is_publication_owner`.

### Semantic Layer

- `scripts/comments/semantic_embeddings.py`
  - Embeds comments, posts, or publications with model/version/hash tracking.
  - Does not currently build a semantic graph or UI.

- `docs/semantic-embeddings.md`
  - Explicitly separates semantic similarity from recommendation topology.

### Quality Gates

Existing tests:

- `tests/test_archive_api.py`
- `tests/test_comment_api.py`
- `tests/test_comment_backfill.py`
- `tests/test_comment_db_integrity.py`
- `tests/test_comment_db_repair.py`
- `tests/test_comment_parsing.py`
- `tests/test_comment_pipeline.py`
- `tests/test_crawl_comment_integration.py`
- `tests/test_db_runtime.py`
- `tests/test_get_recommendations.py`
- `tests/test_gh_pages.py`
- `tests/test_investigate_failed.py`
- `tests/test_label_topics_llm.py`
- `tests/test_publication_crawl_integrity.py`
- `tests/test_quality_gate.py`
- `tests/test_retry_failed.py`
- `tests/test_semantic_embeddings.py`
- `tests/test_substack_endpoint_vitality.py`
- `tests/test_update_graph.py`
- `tests/test_user_classifier.py`

## Feature Plan

### 0. Canonical Repo And Git Repair

Goal: make the project committable before feature work continues.

Codebase truth:

- CHRONOS currently says canonical path is unclear.
- Current local copy contains a broken `.git` file.
- Existing Obsidian truth lists multiple candidates and warns against destructive cleanup.

Plan:

- Choose the canonical repo path.
- Repair or reclone the worktree so `git status` works.
- Confirm remotes and current branch.
- Add or verify `.gitignore` / Git LFS policy for `cartographer.db` and generated HTML/CSV files.
- Do not modify database tracking rules accidentally. Professor Kurz flagged DB churn as PR-hostile.

Checkpoint:

- `git status --short` works.
- `git remote -v` is understood.
- One commit records documentation-only planner files if this repo is the canonical target.

### 1. RecommendationRank Baseline

Goal: make current publication PageRank explicit as `RecommendationRank`.

Current code:

- `scripts/milestone01/centrality.py`
- `scripts/milestone02/pagerank_distribution.py`
- `scripts/milestone02/add_publication_lists.py`
- `data/recommendations_analysis.md`

Plan:

- Rename docs/UI language from generic PageRank to `RecommendationRank` where it represents publication recommendation centrality.
- Keep implementation compatible with `networkx.pagerank`.
- Export stable CSV/JSON artifacts for downstream comparison.
- Add docs explaining that RecommendationRank is computed over `publications` and `recommendations`.

Tests:

- Existing `test_get_recommendations.py`.
- Existing update graph tests.
- Add focused test only if changing output contracts.

Commit size:

- One commit for terminology/docs.
- One commit for any code/output contract changes.

### 2. Monthly Snapshots And Relevancy Metric

Goal: implement Professor Kurz's relevancy metric by comparing RecommendationRank over time.

Current code:

- No dedicated snapshot table exists.
- `semantic_embedding_runs` and `comment_ingestion_runs` show the pattern for batch tracking.

Plan:

- Add tables:
  - `recommendation_rank_runs`: run id, started/finished, graph stats, algorithm params.
  - `recommendation_rank_snapshots`: run id, domain, rank, score, in_degree, out_degree, depth.
- Add `scripts/milestone03/recommendation_rank_snapshots.py`.
- Compute deltas:
  - score delta.
  - rank delta.
  - percentile delta.
  - new entrant / disappeared flags.
- Define "relevancy" as a documented function, initially:
  - `relevancy_delta = zscore(score_delta) + zscore(comment_activity_delta)` when comment stats exist.
  - fallback to RecommendationRank delta only.
- Add CLI:
  - `--snapshot`
  - `--compare latest previous`
  - `--month YYYY-MM`

Tests:

- Schema migration test.
- Snapshot insert/idempotency test.
- Delta calculation test with tiny fixture graph.

Commit size:

- Commit 1: schema/docs.
- Commit 2: snapshot CLI and tests.
- Commit 3: delta report output.

### 3. Failed Publication Recovery

Goal: reduce false failures where pages work manually.

Current code:

- `scripts/milestone02/retry_failed.py`
- `scripts/milestone02/extract_failed.py`
- `scripts/milestone02/investigate_failed.py`
- `crawl.py` direct API fallback exists.

Plan:

- Audit failure classes:
  - DNS.
  - redirect/canonical mismatch.
  - timeout.
  - HTTP status.
  - JSON parse.
  - Substack custom domain behavior.
- Persist normalized failure reason in queue or a sidecar table.
- Use `allow_redirects=True` consistently where safe.
- Store canonical resolved URL for successful custom-domain recovery.
- Make retry reporting answer: "why did crawler fail when browser worked?"

Tests:

- Mock redirect from custom domain to canonical Substack endpoint.
- Mock timeout vs 404 vs parse failure.
- Existing retry/investigate tests.

Commit size:

- One commit for classification schema/report.
- One commit for redirect/canonical handling.
- One commit for tests and docs.

### 4. Faster Crawling With Bounded Concurrency

Goal: improve crawl throughput without abusing Substack or corrupting SQLite.

Current code:

- `crawl.py` is synchronous and sleeps per publication.
- SQLite writes are single-connection.

Plan:

- Do not make BFS persistence concurrent first.
- Split fetch from write:
  - concurrent fetch workers retrieve publication metadata and recommendations.
  - single writer commits queue/publication/recommendation changes.
- Use `httpx.AsyncClient` for direct HTTP fetches.
- Keep `substack_api` usage isolated unless it supports async cleanly.
- Add semaphore and per-domain backoff.
- Preserve deterministic queue status transitions.
- Add `--concurrency N`, default conservative.

Tests:

- Unit test bounded concurrency with fake fetcher.
- Integration test queue status remains valid after mixed success/failure.
- DB integrity test after concurrent batch.

Commit size:

- Commit 1: fetch/write abstraction.
- Commit 2: async fetch path behind flag.
- Commit 3: tests and docs.

### 5. Comment Ingestion At Scale

Goal: complete reliable mass comment crawl/backfill.

Current code:

- Live integration exists behind `crawl.py --enable-comments`.
- Historical backfill exists in `comment_backfill.py`.
- Status tables exist.

Plan:

- Run small pilots before scale:
  - `comment_backfill.py --dry-run --limit 50`
  - `comment_backfill.py --limit 50 --post-limit 3 --delay 1`
- Add per-publication activity summary:
  - posts scanned.
  - comments fetched.
  - unique commenters.
  - reply edges.
  - latest comment timestamp.
- Add `comment_activity_snapshots` or derive from comments table by date buckets.
- Make comment endpoint vitality checks part of pre-scale quality gates.

Tests:

- Existing comment pipeline/backfill tests.
- Existing endpoint vitality test.
- Add activity aggregation test.

Commit size:

- Commit 1: aggregation query/report.
- Commit 2: status/export artifact.
- Commit 3: docs/tests.

### 6. Publication vs Comment-Only User Distinction

Goal: answer Professor Kurz's question about proper publications vs users who only comment.

Current code:

- `user_classifier.py` uses strict `admin + hasPosts`.
- `users` table has classification fields.

Plan:

- Keep current strict rule as baseline.
- Add an audit report:
  - commenter handle.
  - has profile.
  - hasPosts.
  - admin role.
  - primary publication id.
  - publication domain if resolvable.
- Add confidence states:
  - `publication_owner_confirmed`.
  - `comment_only_likely`.
  - `unknown_private_or_failed`.
- Avoid collapsing users into publications unless ownership is confirmed.

Tests:

- Existing `test_user_classifier.py`.
- Add classification confidence fixture tests.

Commit size:

- One commit for confidence model/schema if needed.
- One commit for report and docs.

### 7. CommentRank

Goal: implement comment influence using eigenvector/PageRank-style centrality.

Current code:

- Comments are stored in `comments`.
- Parent reply links are resolved.
- Likes are not confirmed in current planner scan as a persisted field; inspect endpoint payload before promising weighted likes.

Graph model:

- Nodes:
  - comments.
  - optionally users as a projection layer.
- Edges:
  - reply comment -> parent comment.
  - commenter -> comment if user projection is needed.
  - comment -> commenter is optional and should be avoided until semantics are clear.
- Weights:
  - reply edge count.
  - like count only after parser/storage confirms it.
  - recency/time decay later.

Algorithm:

- Build adjacency matrix `A`.
- Normalize into transition matrix `P`.
- Apply damping factor `alpha`, default `0.85`.
- Use teleportation vector for dangling comments.
- Compute stationary vector:
  - `r_next = alpha * P.T @ r + (1 - alpha) * v`.
- Stop by tolerance and max iterations.

Plan:

- Add `scripts/comments/comment_rank.py`.
- Add tables:
  - `comment_rank_runs`.
  - `comment_rank_scores`.
- Support:
  - per-publication ranking.
  - global ranking.
  - configurable edge weighting.
  - output CSV.
- Compare with simple baselines:
  - replies received.
  - likes received if available.
  - author-level aggregation.

Tests:

- Tiny graph with known ranking.
- Dangling-node behavior.
- Parent/child edge extraction.
- Idempotent score persistence.

Commit size:

- Commit 1: pure algorithm and tests.
- Commit 2: DB graph extraction.
- Commit 3: CLI persistence/output.
- Commit 4: docs.

### 8. User Super-Connectors Across Topic Islands

Goal: identify users who comment across otherwise separate topic clusters.

Current code:

- Users/comments/posts exist.
- Topic labels exist for publications via `publication_topics` tooling.
- Semantic embeddings scaffold exists.

Plan:

- Build bipartite graph:
  - user -> publication if user comments on publication.
  - user -> topic if publication has topic label.
- Metrics:
  - number of distinct publications commented on.
  - number of distinct topic labels crossed.
  - entropy over topics.
  - betweenness centrality on user-publication graph.
- Define `SuperConnectorScore`.
- Output:
  - CSV of top users.
  - evidence columns with topic/publication counts.
  - optional anonymization if needed.

Tests:

- Fixture with users spanning one vs many topics.
- Entropy and bridge score tests.

Commit size:

- Commit 1: aggregation query.
- Commit 2: score function and tests.
- Commit 3: report.

### 9. Topic Detection, LLM Labels, Framing, Stance, Sentiment

Goal: turn graph nodes and comments into interpretable topical and rhetorical maps.

Current code:

- `scripts/milestone02/topic_labels_db.py`
- `scripts/milestone02/label_topics_llm.py`
- `scripts/comments/semantic_embeddings.py`

Professor direction:

- Use LLMs.
- Look at Dan Jurafsky's work on computational framing, stance, bias, and media.

Plan:

- Keep topic labels separate from stance/framing labels.
- Add tables or clearly separated columns for:
  - topic label.
  - sentiment.
  - stance target.
  - stance polarity.
  - frame label.
  - model/provider/version.
  - prompt hash.
  - evidence text span or source row id.
- Start with a small fixed label taxonomy.
- Add batch review output before writing large labels.
- Use embeddings for candidate clustering, then LLM for labels/explanations.

Tests:

- Prompt formatting tests.
- Idempotent writes by model/prompt hash.
- Mock LLM response parser tests.

Commit size:

- Commit 1: taxonomy and schema.
- Commit 2: batch labeling CLI.
- Commit 3: report/evaluation docs.

### 10. Fourier And Time-Series Engagement Analysis

Goal: implement the Fourier idea only after narrowing the question.

Professor constraint:

- Time-series collection takes much more effort, so narrow to a specific question.

Narrow question options:

- Daily/weekly activity rhythm for one publication.
- Compare activity rhythms across top RecommendationRank publications.
- Detect bursts after a publication enters a topic trend.
- Compare comment frequency spectrum between topic islands.

Current code:

- `comments.commented_at` exists.
- `posts.published_at` exists.
- No dedicated time-bucket table exists.

Plan:

- Start with per-publication comment time series:
  - bucket size: hour or day.
  - value: comment count.
  - optional: unique commenters.
- Detrend or normalize before FFT.
- Use `numpy.fft.rfft`.
- Output:
  - dominant periods.
  - amplitude spectrum.
  - activity rhythm report.
- Do not over-interpret sparse data.

Tests:

- Synthetic sine-wave fixture identifies expected frequency.
- Sparse series returns low-confidence result.
- Time bucket boundaries are deterministic.

Commit size:

- Commit 1: time-bucket extraction.
- Commit 2: Fourier analyzer with synthetic tests.
- Commit 3: report docs/output.

### 11. Visualization Roadmap

Goal: make the analysis inspectable without mixing signal types.

Current code:

- Recommendation graph visualization exists.
- Semantic embeddings explicitly do not alter recommendation graph.

Plan:

- Keep separate views:
  - Recommendation graph.
  - Comment reply graph.
  - User-publication bridge graph.
  - Topic/semantic graph.
  - Time-series dashboard.
- Add UI labels that state which graph is being shown.
- Do not make force-layout distance imply semantic similarity unless using embeddings graph.

Commit size:

- One graph/view per commit unless the files are tightly coupled.

## Commit And Checkpoint Rules

This is mandatory for future work.

### First Rule: Fix Git Before Feature Work

Current path cannot commit because `.git` points to a missing worktree. Before implementing features:

1. Choose canonical repo.
2. Make `git status --short` work.
3. Confirm branch and remote.
4. Confirm whether `cartographer.db` is tracked through Git LFS, ignored, or intentionally external.

### Commit Granularity

Do not make one fat pass. Commit by coherent file/feature checkpoint.

Default checkpoint sizes:

- Documentation-only checkpoint: 1-3 files.
- Pure tests checkpoint: 1-3 test files.
- Schema checkpoint: `scripts/db_runtime.py` plus migration tests and docs only.
- CLI feature checkpoint: 1 implementation file plus 1-2 test files.
- UI/report checkpoint: 1 generator file plus output contract test.
- Generated artifacts checkpoint: only if explicitly desired; keep separate from source changes.

Hard limits without user approval:

- No commit should mix more than one feature.
- No commit should touch more than 5 source/test/docs files unless it is a mechanical rename or formatting pass.
- No commit should include `cartographer.db` or large generated outputs unless the commit message explicitly says why.
- No commit should combine schema migration, crawler behavior, and UI output in one commit.

### Recommended Pass Structure

For each feature:

1. Write or update docs/spec.
2. Commit docs/spec.
3. Add or update tests.
4. Commit tests if they are meaningful independently.
5. Implement the smallest code slice.
6. Run targeted tests.
7. Commit implementation.
8. Run broader tests.
9. Commit docs/output cleanup if needed.

### Commit Message Format

Use:

```text
area: imperative summary

- What changed
- Why it changed
- Tests run
```

Examples:

```text
comments: add CommentRank power iteration

- Add pure CommentRank solver with damping and teleportation
- Cover dangling comments and simple reply graph ordering
- Tests: pytest tests/test_comment_rank.py
```

```text
rank: persist RecommendationRank snapshots

- Add rank run and snapshot tables
- Add CLI for monthly snapshot capture
- Tests: pytest tests/test_recommendation_rank_snapshots.py tests/test_db_runtime.py
```

## Future Chat Bootstrap Prompt

Use this at the start of any future Codex chat:

```text
Read /Users/agarwal/coding/substack comment graphs/codex/docs/substack-cartographer-god-mode-planner.md and /Users/agarwal/coding/CHRONOS/chronos/03 Projects/Substack Cartographer/Substack Cartographer God Mode Planner.md first. Treat those as project control-plane context. Then inspect the actual repo before editing, verify git status, and commit only by the checkpoint rules in the planner.
```

## Immediate Next Actions

1. Decide canonical repo/path.
2. Repair the broken current `.git` pointer or move work to a clean canonical clone.
3. Commit this planner and Obsidian mirror if the repo is canonical.
4. Start with `RecommendationRank` naming and snapshot schema before more advanced math.
5. Keep CommentRank and Fourier work behind small, testable CLIs.
