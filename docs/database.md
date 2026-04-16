# Database (cartographer.db)

## How it is created

`cartographer.db` is a **SQLite** database created automatically when you use the crawler or retry script:

- **Script:** `scripts/milestone01/crawl.py` creates the DB (if missing) and the core crawl/comment tables.
- **When:** On first run, `SubstackNetworkCrawler(db_name="cartographer.db")` connects to the file and calls `create_schema()`.
- **unfailed:** The table **unfailed** is created by `scripts/milestone02/retry_failed.py` on first run (that script owns it).
- **Location:** The DB is at **repo root** (`cartographer.db`). Run scripts from repo root; or set `CARTOGRAPHER_ROOT` to the repo root.

## Schema

| Table | Purpose |
|-------|---------|
| **publications** | One row per Substack publication (id, name, domain, description, first_seen) |
| **recommendations** | Edges: source_domain → target_domain (who recommends whom) |
| **queue** | Crawl queue: domain, status ('pending'/'crawled'/'failed'), depth |
| **users** | Commenter/user identities observed during comment ingestion |
| **posts** | Archive posts scanned by comment ingestion |
| **comments** | Normalized comment rows linked to posts and users |
| **comment_ingestion_runs** | Batch-level tracking for historical comment backfills |
| **comment_publication_status** | Per-publication backfill status, attempts, stats, and latest error |
| **semantic_embedding_runs** | Batch-level tracking for semantic embedding jobs |
| **semantic_embeddings** | Model-versioned embeddings for comments, posts, or publications |
| **unfailed** | Domains that were failed and later successfully retried by retry_failed.py (audit) |

**Note on “domain” in publications:** The `domain` column is our unique identifier (Substack subdomain or custom domain). Many Substack publications are author-led, so that identifier is often a person’s handle (e.g. `erictopol`, `cameronrwolfe`) rather than a branded site name. Each row is still one publication (one newsletter); the `name` column, when present, is the display name from the API (publication title or author name).

### Column definitions

**publications**

| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | PRIMARY KEY |
| substack_id | TEXT | UNIQUE (Substack's internal id) |
| name | TEXT | Publication name |
| domain | TEXT | UNIQUE (subdomain or custom domain) |
| description | TEXT | From API hero_text |
| first_seen | TIMESTAMP | When first crawled |

**recommendations**

| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER | PRIMARY KEY |
| source_domain | TEXT | Recommending publication |
| target_domain | TEXT | Recommended publication; UNIQUE(source_domain, target_domain) |

**queue**

| Column | Type | Notes |
|--------|------|-------|
| domain | TEXT | PRIMARY KEY |
| status | TEXT | 'pending', 'crawled', or 'failed' |
| depth | INTEGER | BFS depth when enqueued |

**comment_publication_status**

| Column | Type | Notes |
|--------|------|-------|
| domain | TEXT | PRIMARY KEY; publication domain being backfilled |
| publication_substack_id | TEXT | Publication id copied from publications when available |
| status | TEXT | 'pending', 'running', 'succeeded', or 'failed' |
| attempts | INTEGER | Number of backfill attempts |
| last_attempt_at | TIMESTAMP | Last attempt start time |
| last_success_at | TIMESTAMP | Last successful completion time |
| posts_seen / users_seen / comments_unique | INTEGER | Latest pipeline stats |
| comments_created / comments_updated | INTEGER | Latest write stats |
| last_error | TEXT | Latest failure message |
| updated_at | TIMESTAMP | Last status update |

**semantic_embeddings**

| Column | Type | Notes |
|--------|------|-------|
| source_table | TEXT | 'comments', 'posts', or 'publications' |
| source_id | INTEGER | Row id in the source table |
| source_hash | TEXT | SHA-256 hash of normalized source text |
| model | TEXT | Embedding model name |
| dimensions | INTEGER | Vector dimensions |
| embedding_json | TEXT | JSON-encoded vector |
| embedded_at | TIMESTAMP | When the row was embedded |

**unfailed**

| Column | Type | Notes |
|--------|------|-------|
| domain | TEXT | PRIMARY KEY |
| unfailed_at | TIMESTAMP | When retry succeeded (default CURRENT_TIMESTAMP) |

**Definition of unfailed:** A domain is **unfailed** when it had queue status `'failed'` and a run of `retry_failed.py` then successfully fetched it. On success we: (1) add the publication to **publications**, (2) add its recommendation edges to **recommendations**, (3) enqueue any new recommendation targets in **queue** at depth+1, and (4) set that domain's queue status to `'crawled'`. The **unfailed** table records which domains had that happen (with `unfailed_at`), so you can see which publications were recovered from failure rather than crawled on first try.

For what **depth** means and how the crawler avoids loops, see [bfs.md](./bfs.md).

## Is there data? Quick check

```bash
# From repo root
# Row counts per table
sqlite3 cartographer.db "SELECT 'publications' AS table_name, COUNT(*) AS rows FROM publications
  UNION ALL SELECT 'recommendations', COUNT(*) FROM recommendations
  UNION ALL SELECT 'queue', COUNT(*) FROM queue
  UNION ALL SELECT 'users', COUNT(*) FROM users
  UNION ALL SELECT 'posts', COUNT(*) FROM posts
  UNION ALL SELECT 'comments', COUNT(*) FROM comments
  UNION ALL SELECT 'unfailed', COUNT(*) FROM unfailed;"

# Show all data (if small)
sqlite3 -header -column cartographer.db "SELECT * FROM publications; SELECT * FROM queue;"
```

**Why “heuristic” in the queue?**  
If you run the crawler **without** `--seeds-file`, it uses a single default seed: `heuristic` (heuristic.substack.com) **only when the queue has no pending domains**. If the queue already has pending domains, the run resumes from those and does not add the default seed.

**Why does a domain show as `failed`?**  
The crawler marks a domain as `failed` when it can’t get publication info: direct GET to `https://<domain>/api/v1/publication` fails (e.g. domain doesn’t resolve, server down, or timeout), and the fallback (fetch one post) also fails. Custom domains like `read.racket.news` often fail if DNS doesn’t resolve on your network or the site is unreachable.

## How to inspect it

### Easiest: script in this repo
```bash
# From repo root
python scripts/milestone01/view_db.py
# Or: python scripts/milestone01/view_db.py path/to/cartographer.db
```
Prints all tables in readable column form in the terminal.

### Command line (sqlite3)

```bash
# From repo root
# List tables
sqlite3 cartographer.db ".tables"

# Show full schema
sqlite3 cartographer.db ".schema"

# Query data
sqlite3 cartographer.db "SELECT * FROM publications;"
sqlite3 cartographer.db "SELECT * FROM recommendations LIMIT 10;"
sqlite3 cartographer.db "SELECT * FROM queue;"

# Interactive shell
sqlite3 cartographer.db
# Then: .tables   .schema publications   SELECT * FROM publications;   .quit
```

### From Python

```python
import sqlite3
conn = sqlite3.connect("cartographer.db")  # at repo root
conn.row_factory = sqlite3.Row  # optional: access columns by name
cur = conn.cursor()
cur.execute("SELECT * FROM publications")
for row in cur.fetchall():
    print(dict(row))
conn.close()
```

### GUI / table view

- **DB Browser for SQLite** (free, desktop): https://sqlitebrowser.org/ — open `cartographer.db` (at repo root) to browse and edit in table form.
- **VS Code / Cursor:** Install an extension such as "SQLite Viewer" or "SQLite" to open the `.db` file and view tables.
