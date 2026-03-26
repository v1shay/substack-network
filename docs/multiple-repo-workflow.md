# Multiple Repository Workflow

## Overview

- **substack-cartographer**: Main project (this repo) — network crawler, DB, scripts
- **substack_api**: Forked API library — shared by cartographer and other consumers

Keep these repos as siblings in one directory (e.g. `~/repos/` or your workspace root).

## Initial Setup

Run with **source** (not bash) so the venv stays active in this terminal:

```bash
cd path/to/substack-cartographer
source setup.sh
```

Cartographer supports Python 3.11+ and expects the forked `substack_api` repo as a sibling by default (`../substack_api`). If your checkout uses a different layout, set `SUBSTACK_API_REPO=/path/to/substack_api` before running `source setup.sh`. When editable install metadata resolution fails, `setup.sh` falls back to source-path injection so the sibling repo is still importable from the venv.

That terminal's venv is already active. In **other** terminals, run `source .venv/bin/activate` before using python.

## Daily Workflow

### Working on API Library
```bash
cd path/to/substack_api
# Make changes to substack_api/*.py
# Changes are immediately available (no reinstall needed)
```

### Working on Cartographer
```bash
cd path/to/substack-cartographer
source .venv/bin/activate   # Only in a new terminal
# Make changes to scripts/*.py
python scripts/milestone01/crawl.py  # Default seed
# Or load seeds from the repo config (edit config/seeds.md to add URLs):
python scripts/milestone01/crawl.py --seeds-file config/seeds.md --max-publications 100
```

### Resume crawling

Crawling is resume-friendly: the queue stores pending domains, so you can run the same command again and it continues from the next pending domain.

```bash
cd path/to/substack-cartographer
source .venv/bin/activate
python scripts/milestone01/crawl.py --seeds-file config/seeds.md --max-publications 100
```

- **Seeds each run:** If you pass `--seeds-file`, every run loads the file and adds any domain that is **not already in the queue** (existing rows are left as-is). So seeds are included in the sense that new seeds from the file get added; already-crawled or already-pending domains are not duplicated.
- **No seeds file:** Omit `--seeds-file` to only continue from the current queue (no new seeds added; uses default seed only when the queue is empty).
