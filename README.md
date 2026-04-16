# Substack Cartographer

Maps the Substack network: crawl publications and recommendations, then analyze and visualize.

[View the visualization](https://alexhkurz.codeberg.page/substack-cartographer/): opens in a browser; size of a node in the graph represents the centrality (PageRank); use two fingers to zoom; press Shift to freeze, click a node to open its Substack page. See also [docs/visualization.md](docs/visualization.md).

**Summary of commands to make the graph** (run from repo root; `cartographer.db` is at repo root):

One can run the scripts separately, see below, but it should be possible to get away with only (after `source setup.sh` once):

```
python scripts/update_graph.py
```

**Note:** `update_graph.py` starts the crawler in the background (if it isn’t already running) so the DB keeps updating; the rest of the pipeline uses the current DB and exits. The crawler keeps running after you close the terminal. To stop it: `pkill -f "crawl.py"` (or `pgrep -f "crawl.py"` to get the process ID, then `kill <pid>`).
The background crawl now starts with comment enrichment enabled by default, and detached crawler output is written to `log/crawler.log`.

Here is an example workflow calling the scripts separately:


```bash
source setup.sh
python scripts/milestone01/crawl.py --seeds-file config/seeds.md
python scripts/milestone01/view_db.py --counts
python scripts/milestone01/summarize_db.py
python scripts/milestone01/db_to_csv.py
python scripts/milestone01/centrality.py
python scripts/milestone02/layer_stats.py
python scripts/milestone02/investigate_failed.py
python scripts/milestone02/extract_failed.py
python scripts/milestone02/topic_labels_db.py
python scripts/milestone02/label_topics_llm.py
python scripts/milestone01/visualize.py
python scripts/milestone02/add_publication_lists.py
python scripts/get_recommendations.py
python scripts/update_graph.py
open data/substack_graph.html
python scripts/gh_pages.py
open https://alexhkurz.codeberg.page/substack-cartographer/
```

For details see below.

## Setup

Run with **source** (not bash) so the venv stays active in this terminal:

```bash
cd path/to/substack-cartographer
source setup.sh
```

`setup.sh` creates the venv, installs the `substack_api` library, and installs all Python deps from `requirements.txt` (including `requests`, `networkx`, `numpy`, `scipy`, and `pyvis`). Cartographer supports Python 3.11+ and prefers a sibling clone of the `substack_api` repo (`../substack_api`; override with `SUBSTACK_API_REPO`), but if that repo is missing it now falls back to installing `substack_api` from PyPI. If editable install metadata resolution fails, `setup.sh` falls back to source-path injection so the sibling repo still imports cleanly. In other terminals, activate the venv with `source .venv/bin/activate`.

**Paths:** Scripts expect to be run from the repo root; `cartographer.db` and `data/` are at the repo root. You can override the root with the **`CARTOGRAPHER_ROOT`** environment variable (e.g. for CI or a different install location).

**Where the database lives:** The database (`cartographer.db`) is committed to this (public) repo and stored via **Git LFS** so the repo stays under size limits. Run the crawler and all scripts from this repo. One-time setup for Git LFS on your machine: `git lfs install`. Then clone or pull as usual; `git lfs pull` fetches the DB if you have a fresh clone. The private repo does not track the DB (it stays small); when merging from public you can copy the DB from this repo or set `CARTOGRAPHER_ROOT` to this repo’s path when running scripts from the private clone. The DB is on the **main** branch only; the **pages** branch (used by `gh_pages.py` for the site) does not contain it. To get the DB on the remote, push main: `git push origin main`.

## Running the scripts

### Crawl the network

```bash
# Default seed (single publication)
python scripts/milestone01/crawl.py

# Load seeds from config and crawl (recommended; no limit by default—Ctrl-C to stop)
python scripts/milestone01/crawl.py --seeds-file config/seeds.md
# Optional: cap this run, e.g. python scripts/milestone01/crawl.py --seeds-file config/seeds.md --max-publications 100
```

Resume anytime: run the same command again; it continues from the next pending domain. With `--seeds-file`, seeds are added only if not already in the queue. Without it, the default seed is added only when the queue is empty.

The crawler sleeps **1 second** between publications (rate limiting) to avoid hitting Substack’s rate limits and to be polite to their servers. You can override with `--delay SECS` (e.g. `--delay 0.5`); lower values speed up but may trigger rate limits.

To see if the crawler is running: `pgrep -f "crawl.py"` (prints the process ID, or nothing if not running). To stop it: `pkill -f "crawl.py"` or `kill <pid>`. 

### Comment pipeline (standalone)

```bash
python scripts/comments/comment_pipeline.py paulkrugman.substack.com --post-limit 3
# Optional classification:
python scripts/comments/comment_pipeline.py paulkrugman.substack.com --post-limit 3 --classify-commenters
```

- **comment_pipeline.py** — Thin standalone CLI wrapper around the same `process_comments(...)` pipeline used by `crawl.py --enable-comments`. By default it writes to `CARTOGRAPHER_ROOT/cartographer.db` (or `./cartographer.db` if `CARTOGRAPHER_ROOT` is unset). Use `--db path/to/cartographer.db` to target another SQLite file.

### Comment backfill (already-crawled publications)

`crawl.py --enable-comments` enriches only publications as they are newly crawled. To populate comments for publications already in `cartographer.db`, use the durable backfill runner:

```bash
# Preview targets without writes or network calls:
python scripts/comments/comment_backfill.py --dry-run --limit 50

# Safe pilot: 50 publications, 3 posts each, 1s delay:
python scripts/comments/comment_backfill.py --limit 50 --post-limit 3 --delay 1
```

- **comment_backfill.py** — Iterates over existing `publications`, calls the same `process_comments(...)` pipeline, and records resumable status in `comment_ingestion_runs` and `comment_publication_status`.
- See [docs/comment-backfill.md](docs/comment-backfill.md) for retry behavior, status summaries, and pre-scale gates.

### Semantic embeddings (optional)

```bash
# Validate OpenAI config and candidate rows without API calls:
python scripts/comments/semantic_embeddings.py --check-config --source comments --limit 10

# Preview rows needing embeddings:
python scripts/comments/semantic_embeddings.py --dry-run --source comments --limit 100
```

- **semantic_embeddings.py** — Stores model-versioned embeddings for comments, posts, or publications in `semantic_embeddings`; it does not change the recommendation graph visualization.
- See [docs/semantic-embeddings.md](docs/semantic-embeddings.md).

### Live ingestion validation

```bash
python scripts/comments/validate_live_ingestion.py paulkrugman.substack.com --post-limit 1
```

- **validate_live_ingestion.py** — Runs the real archive/comment endpoints for one publication, persists results to SQLite, verifies `users`, `posts`, and `comments` are populated, checks reply linkage and publication joins, and prints sample rows from the DB.

### PageRank and visualization

```bash
python scripts/milestone01/centrality.py
python scripts/milestone01/centrality.py -n 100 -o data/pagerank.csv
python scripts/milestone01/visualize.py
python scripts/milestone01/visualize.py -n 200 -o data/substack_graph.html
```

- **centrality.py:** PageRank on the recommendation graph; prints top-N and optional CSV.
- **visualize.py:** Builds an interactive HTML graph (pyvis): node size = PageRank, hover for details. Output: `data/substack_graph.html` by default. Open in a browser to zoom, pan, and explore.

See [docs/data-analysis.md](docs/data-analysis.md) and [docs/visualization.md](docs/visualization.md) (layout, click-to-archive, Shift-freeze).

### Publication lists and PageRank distribution

```bash
python scripts/milestone02/add_publication_lists.py
# Optional: python scripts/milestone02/add_publication_lists.py -n 300
```

- **add_publication_lists.py** — Generates `data/graph-publications.html` (top-N by PageRank), `data/db-publications.html` (all publications in the DB), and `index.html` (iframe wrapper with links to the graph and list pages). Run after `visualize.py` so the graph and index exist. The script also runs **pagerank_distribution.py** and embeds its output above the db-publications table.
- **pagerank_distribution.py** — Standalone: loads the recommendation graph, computes PageRank, fits a power law PR(r) ≈ c·r^−α, and can write `data/pagerank_distribution.json` and/or an HTML fragment. The fragment shows a rank–PageRank chart (log y) with data points and the fitted curve; add_publication_lists embeds it so it appears above “All publications in the database.” Run directly for JSON only: `python scripts/milestone02/pagerank_distribution.py --json`.

### View the database

```bash
python scripts/milestone01/view_db.py
# Or: python scripts/milestone01/view_db.py path/to/cartographer.db
# Table sizes only: python scripts/milestone01/view_db.py --counts
# Summary (counts only, no rows): python scripts/milestone01/summarize_db.py
# Export to CSV: python scripts/milestone01/db_to_csv.py
```

Prints all tables in readable column form. Use `summarize_db.py` for a short summary (row counts and queue status breakdown only, no row data). Use `db_to_csv.py` to export each table to CSV in the `data/` folder (e.g. `data/publications.csv`). Use `-o dir` to choose another output directory. Use `--counts` (or `-c`) to print only row counts per table. **Run from repo root;** the DB is at repo root (`cartographer.db`) and is created there when you first run the crawler.

### Failed publications: retry → extract → investigate

Domains the crawler couldn’t fetch end up in the queue with `status = 'failed'`. To retry them, refresh the failed list, and update the investigation report, run in this order:

1. **retry_failed.py** — Retries each failed domain; on success adds to `publications`, `recommendations`, and `queue`, sets status to `'crawled'`, and records the domain in the `unfailed` table. Does not write any HTML or CSV.
2. **extract_failed.py** — Reads `queue WHERE status = 'failed'` and overwrites `data/failed_publications.csv` and `data/failed_publications.html`. Run after retries so the list only contains domains that are still failed.
3. **investigate_failed.py** — Reads `data/failed_publications.csv`, probes each URL (e.g. GET homepage), and writes `data/failed_investigation.html` and `data/failed_investigation.log`. Run after extract so the report matches the current failed list.

```bash
# Don’t run retry_failed.py while the main crawler is running
python scripts/milestone02/retry_failed.py
python scripts/milestone02/extract_failed.py
python scripts/milestone02/investigate_failed.py
```

Optional: `retry_failed.py --max 50 --delay 1.5` to limit retries and add a delay between requests.

## Documentation

- [docs/README.md](docs/README.md) — Documentation index
- [docs/visualization.md](docs/visualization.md) — Interactive graph: layout, click-to-archive, Shift-freeze
- [docs/multiple-repo-workflow.md](docs/multiple-repo-workflow.md) — Repo setup and daily workflow
- [docs/database.md](docs/database.md) — Database schema and inspection
- [docs/bfs.md](docs/bfs.md) — BFS, depth, and loop avoidance
- [docs/development-plan.md](docs/development-plan.md) — Roadmap and design

## License

This project is licensed under the GNU General Public License v3.0 — see [LICENSE](LICENSE).
