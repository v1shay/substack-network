# Comment Backfill

`crawl.py --enable-comments` enriches comments only when a publication is newly crawled. It does not backfill the large set of publications already present in `cartographer.db`.

Use `scripts/comments/comment_backfill.py` for controlled historical comment ingestion. It reads from `publications`, calls the existing `process_comments(...)` pipeline, and records durable status in:

- `comment_ingestion_runs` — one row per batch run.
- `comment_publication_status` — one row per publication with status, attempts, latest stats, and the last error.

## Safe Pilot

Run a read-only target preview first:

```bash
.venv/bin/python scripts/comments/comment_backfill.py --dry-run --limit 50
```

Seed status rows without fetching comments:

```bash
.venv/bin/python scripts/comments/comment_backfill.py --seed-only --limit 50
```

Run the default pilot:

```bash
.venv/bin/python scripts/comments/comment_backfill.py \
  --limit 50 \
  --post-limit 3 \
  --delay 1
```

The default settings are intentionally conservative:

- `--limit 50`
- `--post-limit 3`
- `--delay 1`
- `--max-attempts 3`
- commenter classification off unless `--classify-commenters` is passed

## Resume And Retry

The runner skips publications marked `succeeded`. It selects publications with no status row, `pending`, or `failed`, up to `--max-attempts`.

Useful targeted runs:

```bash
.venv/bin/python scripts/comments/comment_backfill.py --domain paulkrugman --limit 1
.venv/bin/python scripts/comments/comment_backfill.py --domains-file domains.txt --post-limit 1
```

Use `--stop-on-error` for a strict batch that exits on the first failed publication. Without it, the batch records the failure and continues.

## Gates Before Scaling

Before a larger run:

```bash
.venv/bin/python -m unittest discover -s tests -p "test_*.py" -v
.venv/bin/python scripts/comments/db_audit.py --read-only --fail-on-anomaly
```

`--read-only` opens the DB in SQLite read-only mode and does not run migrations. If it reports schema drift, run the normal audit or a small backfill against a copied DB first so the schema upgrade can be inspected before touching the production DB.
