#!/usr/bin/env python3
"""
Extract failed publications from the crawl queue (status='failed') and unfailed from the unfailed table.
Writes data/failed_publications.html, data/failed_publications.csv, and data/unfailed_publications.html.

Usage (from repo root):
    python scripts/milestone02/extract_failed.py

Failed list is used by investigate_failed.py. Unfailed list shows domains that were previously failed and successfully retried.
"""

import argparse
import csv
import os
import sqlite3
import sys
from pathlib import Path


def domain_to_url(domain: str) -> str:
    """Homepage URL for a reader."""
    if "." in domain and "substack.com" not in domain:
        return f"https://{domain}"
    clean = domain.replace(".substack.com", "")
    return f"https://{clean}.substack.com"


def _h(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def main() -> None:
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    out_path = root / "data" / "failed_publications.html"
    parser = argparse.ArgumentParser(description="Extract failed publications from queue.")
    parser.add_argument("--db", type=str, default=None, help="Path to cartographer.db")
    args = parser.parse_args()
    if args.db:
        db_path = Path(args.db).resolve()

    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT domain, depth FROM queue WHERE status = 'failed' ORDER BY depth, domain"
    )
    rows = cur.fetchall()
    conn.close()

    # Always write CSV so investigate_failed.py can use it as input.
    csv_path = root / "data" / "failed_publications.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["domain", "depth", "url"])
        for domain, depth in rows:
            w.writerow([domain, depth, domain_to_url(domain)])
    print(f"Wrote {csv_path} ({len(rows)} rows)")

    # HTML
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table_rows = []
    for domain, depth in rows:
        url = domain_to_url(domain)
        table_rows.append(
            f'    <tr><td>{_h(domain)}</td><td>{depth}</td><td><a href="{_h(url)}" target="_blank" rel="noopener">{_h(url)}</a></td></tr>'
        )

    empty_row = '    <tr><td colspan="3">None.</td></tr>'
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Failed publications</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/css/bootstrap.min.css" rel="stylesheet" crossorigin="anonymous">
</head>
<body>
<div class="container mt-4">
  <h1>Failed publications</h1>
  <p><a href="../index.html" id="back-to-graph">← Back to graph</a></p>
  <p class="text-muted small">Domains the crawler marked as failed when it tried to fetch them. Same URL as below: the crawler uses the same base URL but requires <strong>/api/v1/publication</strong> or post metadata to succeed; if that fails (timeout, exception, or API returned nothing), the domain is marked failed. The <a href="failed_investigation.html">investigation report</a> only GETs the <strong>homepage</strong>; 200 OK there means the homepage is up, but the crawler may still have failed on the API or substack_api calls.</p>
  <p><strong>{len(rows)}</strong> failed.</p>
  <table class="table table-striped">
  <thead><tr><th>Domain</th><th>Depth</th><th>URL</th></tr></thead>
  <tbody>
{chr(10).join(table_rows) if table_rows else empty_row}
  </tbody>
  </table>
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/js/bootstrap.bundle.min.js" crossorigin="anonymous"></script>
<script>
  (function() {{
    if (window.parent !== window) {{
      var a = document.getElementById('back-to-graph');
      if (a) a.addEventListener('click', function(e) {{ e.preventDefault(); parent.postMessage('showGraph', '*'); }});
    }}
  }})();
</script>
</body>
</html>
"""
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote {out_path} ({len(rows)} failed)")

    # ---- unfailed_publications.html (from unfailed table, if it exists) ----
    conn2 = sqlite3.connect(db_path)
    cur2 = conn2.cursor()
    try:
        cur2.execute("SELECT domain, unfailed_at FROM unfailed ORDER BY unfailed_at, domain")
        unfailed_rows = cur2.fetchall()
    except sqlite3.OperationalError:
        unfailed_rows = []
    finally:
        conn2.close()

    unfailed_path = root / "data" / "unfailed_publications.html"
    unfailed_table_rows = []
    for domain, unfailed_at in unfailed_rows:
        url = domain_to_url(domain)
        unfailed_table_rows.append(
            f'    <tr><td>{_h(domain)}</td><td>{_h(str(unfailed_at))}</td><td><a href="{_h(url)}" target="_blank" rel="noopener">{_h(url)}</a></td></tr>'
        )
    unfailed_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Unfailed publications</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/css/bootstrap.min.css" rel="stylesheet" crossorigin="anonymous">
</head>
<body>
<div class="container mt-4">
  <h1>Unfailed publications</h1>
  <p><a href="../index.html" id="back-to-graph">← Back to graph</a></p>
  <p class="text-muted small">Domains that were previously in the queue with status <code>failed</code> and were successfully retried by <code>retry_failed.py</code>. They are now in <strong>publications</strong> and <strong>queue</strong> (status <code>crawled</code>). See <a href="failed_publications.html">Failed publications</a> for those still failing.</p>
  <p><strong>{len(unfailed_rows)}</strong> unfailed.</p>
  <table class="table table-striped">
  <thead><tr><th>Domain</th><th>Unfailed at</th><th>URL</th></tr></thead>
  <tbody>
{chr(10).join(unfailed_table_rows) if unfailed_table_rows else empty_row}
  </tbody>
  </table>
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/js/bootstrap.bundle.min.js" crossorigin="anonymous"></script>
<script>
  (function() {{
    if (window.parent !== window) {{
      var a = document.getElementById('back-to-graph');
      if (a) a.addEventListener('click', function(e) {{ e.preventDefault(); parent.postMessage('showGraph', '*'); }});
    }}
  }})();
</script>
</body>
</html>
"""
    unfailed_path.parent.mkdir(parents=True, exist_ok=True)
    unfailed_path.write_text(unfailed_html, encoding="utf-8")
    print(f"Wrote {unfailed_path} ({len(unfailed_rows)} unfailed)")


if __name__ == "__main__":
    main()
