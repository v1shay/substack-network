# Failed publications investigator

The **investigator** (`scripts/milestone02/investigate_failed.py`) probes each failed publication URL (from `extract_failed` output), classifies the outcome (Timeout, 404, 200 OK, etc.), and writes an HTML report with a bar chart plus a tab-separated log. Use it to see *why* domains ended up as failed (e.g. site down, SSL error, 404).

**Input:** `data/failed_publications.csv` (produced by `extract_failed.py`; run that first).

**Output:** `data/failed_investigation.html` (report + chart), `data/failed_investigation.log` (TSV: domain, depth, url, status_code, final_url, error, length, reason).

## What happens if I run the investigator again?

**By default it only updates** — it does **not** re-probe every link.

- It reads existing results from `data/failed_investigation.log` (if present).
- For each URL in the CSV: if that URL is already in the log, the old result is reused (no HTTP request).
- Only URLs that are in the CSV but **not** in the log are probed.
- The report and log are then rewritten with the merged results (existing + newly probed).

So running `investigate_failed.py` again is fast when most URLs are already in the log; you only pay for new failed domains.

To **re-probe all links** from scratch, run with `--full` or `--all`:

```bash
python scripts/milestone02/investigate_failed.py --full
# or
python scripts/milestone02/investigate_failed.py --all
```

With `--full` or `--all`, the current report and log are **archived** (renamed to `failed_investigation_YYYYMMDD_HHMMSS.html` and `.log`) so old reports are kept. Then every URL in the CSV is probed again and a new `failed_investigation.html` and `.log` are written.

---

## Usage (from repo root)

```bash
# Default: only new — probe URLs not yet in the log; reuse existing results
python scripts/milestone02/investigate_failed.py

# Re-probe all failed URLs (archives current report)
python scripts/milestone02/investigate_failed.py --full
python scripts/milestone02/investigate_failed.py --all   # same as --full
```

When `update_graph.py` runs the investigator, it uses the default (only new), so only newly failed URLs are probed.

## Lock: only one investigator at a time

The investigator can run for a long time. To avoid starting a second one while the first is still running:

- The investigator creates `.investigator.lock` in the repo root when it starts (and has work to do) and removes it in a `finally` block when it exits. The lock file holds the process PID.
- `update_graph.py` checks that lock before starting the investigator: if the file exists and the PID is still alive, it skips starting a new run and prints that the investigator is already in progress. The script does not use the lock to refuse to run when invoked directly; the lock is there so that update_graph can avoid starting a second instance. Without that check, two investigators could run in parallel and overwrite the same report and log.

---

## How it probes

Each URL is requested with a browser-like User-Agent and a 10s timeout; redirects are followed. Outcomes are grouped into reasons such as: Timeout, ConnectionError, SSL error, 200 OK, 404, 403, 4xx, 5xx. The bar chart in the HTML shows counts per reason; the table and log list every URL with status, final URL, error (if any), and reason.
