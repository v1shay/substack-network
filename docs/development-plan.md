# Substack Cartographer - Development Plan

Discover and catalog the network of publications and recommendations.

## M1: Central Nodes in the Recommendation Graph

Everything we have so far lives under **`scripts/milestone01/`** plus **`scripts/gh_pages.py`**. 

One workflow: crawl the recommendation graph, inspect or export the DB, compute PageRank, build an interactive graph, and publish it.

### Scripts

| Script | Purpose |
|--------|---------|
| `scripts/milestone01/crawl.py` | Network crawler using forked `substack_api`. BFS over recommendations, depth, seeds file (`config/seeds.md`), resume. Writes `cartographer.db` at repo root. |
| `scripts/milestone01/view_db.py` | Print all tables (or `--counts`). DB path from repo root or `CARTOGRAPHER_ROOT`. |
| `scripts/milestone01/summarize_db.py` | Row counts and queue status only. |
| `scripts/milestone01/db_to_csv.py` | Export tables to CSV in `data/`. |
| `scripts/milestone01/centrality.py` | PageRank (and in-degree) on recommendation graph; top-N table and optional CSV. |
| `scripts/milestone01/visualize.py` | Interactive HTML graph (pyvis): node size = PageRank, click node → open Substack archive. Output: `data/substack_graph.html`, plus `index.html` at repo root. |
| `scripts/gh_pages.py` | Prepare and push branch for static hosting (e.g. Codeberg Pages). |

### Database and paths

- **DB:** `cartographer.db` at **repo root**. Tables: `publications`, `recommendations`, `queue`. See [database.md](database.md).
- **Run from repo root** (or set `CARTOGRAPHER_ROOT`). Setup: `source setup.sh` (venv, `substack_api` editable, `requirements.txt`).

### Docs and support

- **Ontology:** [project-ontology.md](project-ontology.md) — Substack API terms and metaphors.
- **Analysis:** [data-analysis.md](data-analysis.md) — questions (in-degree, reciprocity, centrality, scale).
- **Visualization:** [visualization.md](visualization.md) — pyvis, click-to-archive, Shift-freeze.
- **Workflow:** [multiple-repo-workflow.md](multiple-repo-workflow.md) — cartographer + `substack_api` sibling repo.
- **BFS and depth:** [bfs.md](bfs.md).

## M2: Improve User Interface


| Script | Purpose |
|--------|---------|
| `scripts/milestone02/add_publication_lists.py` | Add graph + DB publication list pages and links in index.html (graph-publications.html, db-publications.html). Run after visualize.py. |
| `scripts/milestone02/extract_failed.py` | Extract failed publications from the crawl queue (status='failed'). Writes data/failed_publications.html and failed_publications.csv. |
| `scripts/milestone02/investigate_failed.py` | Probe failed URLs from extract_failed output. Default: only new; --full/--all: re-probe all and archive current report. |
| `scripts/milestone02/layer_stats.py` | Compute layer sizes L(d), accumulation A(d), growth ratio r(d), L'(d) from queue. Writes data/layer_stats.html (table + chart). |
| `scripts/milestone02/topic_labels_db.py` | Shared DB schema for topic labels (publication_topics table). Used by label_topics_llm and label_topics_cluster. |
| `scripts/milestone02/label_topics_llm.py` | Label publications by topic using an LLM and a fixed label list; writes to publication_topics.topic_llm. |

**`scripts/update_graph.py`** — One-shot pipeline to refresh the recommendation graph and UI from the current DB. Run from repo root.

1. **Crawl** — If no crawl is running (no `.crawler.lock` or PID dead), starts `crawl.py` in the background (detached with `start_new_session=True` and stdio redirected away from the caller so it keeps running quietly after the script and terminal close). Does not wait for the crawl; graph and lists use whatever is in the DB now.
2. **Centrality** — Runs `centrality.py` (PageRank, top-N).
3. **Visualize** — Runs `visualize.py` (interactive graph → `data/substack_graph.html`, `index.html`).
4. **Add publication lists** — Runs `add_publication_lists.py` (graph-publications.html, db-publications.html, links in index.html).
5. **Layer stats** — Runs `layer_stats.py` (L(d), r(d), etc. → `data/layer_stats.html`).
6. **Extract failed** — Runs `extract_failed.py` (failed_publications.html, failed_publications.csv).
7. **Open main page** — Opens `index.html` in the browser so you can use the graph while the investigator runs.
8. **Investigator** — If no investigator is running (no `.investigator.lock` or PID dead), runs `investigate_failed.py` (default: only new failed URLs). Waits for it to finish; if it took ≥ 1 minute, opens `data/failed_investigation.html`. Otherwise you can open the report from the Failed publications page.

See [investigator.md](investigator.md) for the investigator lock and `--full`/`--all` behaviour.

## Setup: cartographer + substack_api

- **Layout:** Cartographer and forked **substack_api** as sibling repos (e.g. `../substack_api`). `setup.sh` uses that by default; override with `SUBSTACK_API_REPO`.
- **One-time:** From repo root run `source setup.sh`. It creates the venv, installs `substack_api` in editable mode, and installs `requirements.txt` (networkx, numpy, scipy, pyvis). Other terminals: `source .venv/bin/activate`.
- **Details:** [multiple-repo-workflow.md](multiple-repo-workflow.md).
