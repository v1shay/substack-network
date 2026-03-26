#!/usr/bin/env python3
"""
Probe failed publication URLs from extract_failed output (data/failed_publications.csv).
Default: only new — probe URLs that are in the CSV but not yet in failed_investigation.log.
--full or --all: re-probe all failed URLs; current report is archived with a timestamp.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    print("pip install requests", file=sys.stderr)
    sys.exit(1)

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.db_runtime import connect_db


LOG_HEADER = [
    "domain",
    "depth",
    "homepage_url",
    "homepage_status",
    "homepage_final_url",
    "archive_status",
    "archive_final_url",
    "publication_status",
    "publication_final_url",
    "error_summary",
    "classification",
]


def _h(value: object) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def domain_to_url(domain: str) -> str:
    if "." in domain and "substack.com" not in domain:
        return f"https://{domain}"
    clean = domain.replace(".substack.com", "")
    return f"https://{clean}.substack.com"


def ensure_failed_csv(root: Path, csv_path: Path) -> bool:
    if csv_path.exists():
        return True

    db_path = root / "cartographer.db"
    if not db_path.exists():
        return False

    conn = connect_db(db_path)
    cur = conn.cursor()
    cur.execute("SELECT domain, depth FROM queue WHERE status = 'failed' ORDER BY depth, domain")
    rows = cur.fetchall()
    conn.close()

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["domain", "depth", "url"])
        for domain, depth in rows:
            writer.writerow([domain, depth, domain_to_url(domain)])

    print(f"CSV missing; generated {csv_path} from queue ({len(rows)} rows).")
    return True


def load_existing_log(log_path: Path) -> dict[str, dict[str, str]]:
    if not log_path.exists():
        return {}
    with open(log_path, encoding="utf-8") as handle:
        lines = [line.rstrip("\n") for line in handle]
    if not lines:
        return {}
    headers = lines[0].split("\t")
    if headers != LOG_HEADER:
        return {}

    existing: dict[str, dict[str, str]] = {}
    for line in lines[1:]:
        if not line:
            continue
        values = line.split("\t")
        if len(values) != len(LOG_HEADER):
            continue
        record = dict(zip(LOG_HEADER, values))
        existing[record["homepage_url"]] = record
    return existing


def probe_endpoint(url: str, headers: dict[str, str], timeout: int) -> dict[str, str]:
    result = {
        "status": "",
        "final_url": url,
        "error": "",
    }
    try:
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        result["status"] = str(response.status_code)
        result["final_url"] = response.url
    except requests.exceptions.Timeout:
        result["error"] = "Timeout"
    except requests.exceptions.SSLError as exc:
        result["error"] = f"SSLError: {exc}"
    except requests.exceptions.ConnectionError as exc:
        result["error"] = f"ConnectionError: {exc}"
    except Exception as exc:  # pragma: no cover - defensive fallback
        result["error"] = str(exc)
    return result


def classify_result(
    *,
    homepage: dict[str, str],
    archive: dict[str, str],
    publication: dict[str, str],
    base_url: str,
) -> str:
    base_host = urlparse(base_url).netloc.lower()
    for candidate in (homepage, archive, publication):
        final_url = candidate.get("final_url") or ""
        final_host = urlparse(final_url).netloc.lower()
        if final_host and final_host != base_host:
            return "redirected_elsewhere"
    if publication.get("status") == "200":
        return "publication_api_ok"
    if archive.get("status") == "200":
        return "archive_ok"
    if homepage.get("status") == "200":
        return "homepage_up"
    if any(candidate.get("error") for candidate in (homepage, archive, publication)):
        return "network_error"
    return "crawler_still_failed"


def build_record(domain: str, depth: int, homepage_url: str, headers: dict[str, str], timeout: int) -> dict[str, str]:
    archive_url = f"{homepage_url}/api/v1/archive?sort=new&offset=0&limit=1"
    publication_url = f"{homepage_url}/api/v1/publication"
    homepage = probe_endpoint(homepage_url, headers, timeout)
    archive = probe_endpoint(archive_url, headers, timeout)
    publication = probe_endpoint(publication_url, headers, timeout)

    errors = [candidate["error"] for candidate in (homepage, archive, publication) if candidate["error"]]
    error_summary = " | ".join(errors)
    classification = classify_result(
        homepage=homepage,
        archive=archive,
        publication=publication,
        base_url=homepage_url,
    )
    return {
        "domain": domain,
        "depth": str(depth),
        "homepage_url": homepage_url,
        "homepage_status": homepage["status"] or "—",
        "homepage_final_url": homepage["final_url"],
        "archive_status": archive["status"] or "—",
        "archive_final_url": archive["final_url"],
        "publication_status": publication["status"] or "—",
        "publication_final_url": publication["final_url"],
        "error_summary": error_summary or "—",
        "classification": classification,
    }


def write_report(results: list[dict[str, str]], out_html: Path, out_log: Path, note: str) -> None:
    classification_counts: Counter[str] = Counter()
    for result in results:
        classification_counts[result["classification"]] += 1

    out_log.parent.mkdir(parents=True, exist_ok=True)
    with open(out_log, "w", encoding="utf-8") as handle:
        handle.write("\t".join(LOG_HEADER) + "\n")
        for result in results:
            handle.write("\t".join(result[key] for key in LOG_HEADER) + "\n")

    sorted_counts = sorted(classification_counts.items(), key=lambda item: (-item[1], item[0]))
    labels_json = json.dumps([label for label, _ in sorted_counts])
    counts_json = json.dumps([count for _, count in sorted_counts])

    rows = []
    for result in results:
        rows.append(
            "    <tr>"
            f"<td>{_h(result['domain'])}</td>"
            f"<td>{_h(result['depth'])}</td>"
            f"<td><a href=\"{_h(result['homepage_url'])}\" target=\"_blank\" rel=\"noopener\">{_h(result['homepage_url'])}</a></td>"
            f"<td>{_h(result['homepage_status'])}</td>"
            f"<td>{_h(result['archive_status'])}</td>"
            f"<td>{_h(result['publication_status'])}</td>"
            f"<td>{_h(result['homepage_final_url'])}</td>"
            f"<td>{_h(result['error_summary'])}</td>"
            f"<td>{_h(result['classification'])}</td>"
            "</tr>"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Failed publications – investigation</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/css/bootstrap.min.css" rel="stylesheet" crossorigin="anonymous">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
<div class="container mt-4">
  <h1>Failed publications – investigation</h1>
  <p><a href="../index.html" id="back-to-graph">← Back to graph</a></p>
  <p class="text-muted small">Input: <code>data/failed_publications.csv</code>. {note} See <code>data/failed_investigation.log</code> for the tab-separated log.</p>
  <p class="text-muted small">This report probes the homepage, <code>/api/v1/archive</code>, and <code>/api/v1/publication</code> separately so it reflects crawler-relevant surfaces, not just homepage reachability.</p>
  <p><strong>{len(results)}</strong> in report.</p>
  <div class="mb-4" style="max-width: 560px; height: 240px;">
    <canvas id="classificationChart"></canvas>
  </div>
  <table class="table table-striped table-sm">
  <thead><tr><th>Domain</th><th>Depth</th><th>Homepage</th><th>Homepage status</th><th>Archive status</th><th>Publication status</th><th>Redirect / final homepage</th><th>Error summary</th><th>Classification</th></tr></thead>
  <tbody>
{chr(10).join(rows)}
  </tbody>
  </table>
</div>
<script>
  (function() {{
    var labels = {labels_json};
    var counts = {counts_json};
    if (labels.length > 0) {{
      new Chart(document.getElementById('classificationChart'), {{
        type: 'bar',
        data: {{
          labels: labels,
          datasets: [{{ label: 'Count', data: counts, backgroundColor: 'rgba(54, 162, 235, 0.6)', borderColor: 'rgb(54, 162, 235)', borderWidth: 1 }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          scales: {{ y: {{ beginAtZero: true, ticks: {{ stepSize: 1 }} }} }},
          plugins: {{ legend: {{ display: false }} }}
        }}
      }});
    }}
    if (window.parent !== window) {{
      var a = document.getElementById('back-to-graph');
      if (a) a.addEventListener('click', function(e) {{ e.preventDefault(); parent.postMessage('showGraph', '*'); }});
    }}
  }})();
</script>
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")
    print(f"Wrote {out_log}")
    print(f"Wrote {out_html}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Probe failed URLs. Default: only new (URLs not yet in log). --full/--all: re-probe all and archive current report."
    )
    parser.add_argument("--full", action="store_true", help="Re-probe all failed URLs; archive current report with timestamp")
    parser.add_argument("--all", action="store_true", dest="full", help="Alias for --full")
    args = parser.parse_args()

    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    csv_path = root / "data" / "failed_publications.csv"
    out_html = root / "data" / "failed_investigation.html"
    out_log = root / "data" / "failed_investigation.log"

    if not ensure_failed_csv(root, csv_path):
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        print("Could not auto-generate failed CSV (missing DB or queue table).", file=sys.stderr)
        sys.exit(1)

    rows: list[tuple[str, int, str]] = []
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append((row["domain"], int(row.get("depth", 0)), row["url"]))

    if not rows:
        print("No failed publications in CSV (empty list).")
        out_html.parent.mkdir(parents=True, exist_ok=True)
        out_html.write_text("<!DOCTYPE html><html><body><p>No failed publications.</p></body></html>", encoding="utf-8")
        return

    lock_path = root / ".investigator.lock"
    lock_path.write_text(str(os.getpid()), encoding="utf-8")
    try:
        if args.full:
            ts = time.strftime("%Y%m%d_%H%M%S")
            archive_html = root / "data" / f"failed_investigation_{ts}.html"
            archive_log = root / "data" / f"failed_investigation_{ts}.log"
            if out_html.exists():
                out_html.rename(archive_html)
                print(f"Archived report to {archive_html.name}")
            if out_log.exists():
                out_log.rename(archive_log)
                print(f"Archived log to {archive_log.name}")
            existing = {}
        else:
            existing = load_existing_log(out_log)
            reused = sum(1 for _, _, url in rows if url in existing)
            if reused:
                print(f"Reusing {reused} URL(s) from log; probing the rest.")

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        timeout = 10
        results: list[dict[str, str]] = []
        probed = 0
        for domain, depth, homepage_url in rows:
            if homepage_url in existing:
                results.append(existing[homepage_url])
                continue
            record = build_record(domain, depth, homepage_url, headers, timeout)
            results.append(record)
            probed += 1
            print(
                "\t".join(
                    (
                        record["domain"],
                        record["depth"],
                        record["homepage_url"],
                        record["homepage_status"],
                        record["archive_status"],
                        record["publication_status"],
                        record["classification"],
                    )
                )
            )
            time.sleep(0.3)

        if probed == 0 and not args.full:
            print("No new URLs to probe; report is up to date.")
        else:
            print(f"\nProbed {probed} new URL(s).")

        note = "Default: only new URLs are probed; existing results reused. Use --full to re-probe all and archive."
        write_report(results, out_html, out_log, note)
    finally:
        lock_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
