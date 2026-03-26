#!/usr/bin/env python3
"""
Fetch the list of recommended publications for a Substack publication.
Uses the same API path as the crawler: archive → publication_id → recommendations/from/{id}.

Usage (from repo root):
    python scripts/get_recommendations.py paulkrugman.substack.com
    python scripts/get_recommendations.py https://paulkrugman.substack.com
    python scripts/get_recommendations.py paulkrugman.substack.com --json
    python scripts/get_recommendations.py paulkrugman.substack.com --raw-json
    python scripts/get_recommendations.py  # Analyzes top 20 ranked publications

Output: one line per recommendation with a best-effort UI guess plus provenance.
  Use --separate for two sections (Publications / People), or --only-publications / --only-people to filter.
  Default: "icon\tui_guess\tprovenance\turl".

Note: The Substack UI shows "follow n people" and "subscribe to m publications". The single API
  /api/v1/recommendations/from/{id} returns one list; we classify by is_personal_mode. On some
  sites (e.g. oldster) the API returns all is_personal_mode=false, so we show all as publications
  while the UI shows "follow 50 people" and "subscribe to 3 publications". Use --as-people to
  treat every item as "person" (follow list) so counts match the UI "follow n people".
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.db_runtime import connect_db

try:
    import networkx as nx
except ImportError:
    nx = None

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}
TIMEOUT = 15

# Icons for API classification (is_personal_mode: true → person, false → publication)
ICON_PERSON = "👤"
ICON_PUBLICATION = "📰"
ICON_BY_KIND = {"person": ICON_PERSON, "publication": ICON_PUBLICATION}


def url_to_base(url_or_domain: str) -> str:
    """Return https://host (no path) for a URL or domain."""
    s = url_or_domain.strip().rstrip("/")
    if not s:
        return ""
    if "://" not in s:
        s = f"https://{s}"
    parsed = urlparse(s)
    host = parsed.netloc or parsed.path.split("/")[0]
    if not host:
        return ""
    return f"https://{host}"


def get_publication_id(base_url: str) -> int | None:
    """Get publication_id from first post in archive. Returns None on failure."""
    endpoint = f"{base_url}/api/v1/archive?sort=new&offset=0&limit=1"
    try:
        r = requests.get(endpoint, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0].get("publication_id")
    except (requests.RequestException, json.JSONDecodeError, KeyError):
        pass
    return None


def get_recommendations(base_url: str) -> list[dict] | None:
    """Fetch recommendations JSON. Returns None on failure."""
    pid = get_publication_id(base_url)
    if not pid:
        return None
    endpoint = f"{base_url}/api/v1/recommendations/from/{pid}"
    try:
        r = requests.get(endpoint, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except (requests.RequestException, json.JSONDecodeError):
        return None


def recommendation_records(
    recommendations: list[dict],
    *,
    force_people: bool = False,
) -> list[dict[str, str | bool]]:
    """Extract recommendation URLs with a best-effort UI guess and provenance."""
    records: list[dict[str, str | bool]] = []
    for rec in recommendations or []:
        pub = rec.get("recommendedPublication", {})
        if force_people:
            ui_guess = "person"
            provenance = "forced via --as-people"
        else:
            ui_guess, provenance = ui_guess_from_publication(pub)
        if pub.get("custom_domain"):
            url = pub["custom_domain"]
            if not url.startswith("http"):
                url = f"https://{url}"
            records.append(
                {
                    "ui_guess": ui_guess,
                    "provenance": provenance,
                    "url": url,
                    "crawl_target": True,
                }
            )
        elif pub.get("subdomain"):
            records.append(
                {
                    "ui_guess": ui_guess,
                    "provenance": provenance,
                    "url": f"https://{pub['subdomain']}.substack.com",
                    "crawl_target": True,
                }
            )
    return records


def ui_guess_from_publication(publication: dict) -> tuple[str, str]:
    is_personal_mode = publication.get("is_personal_mode")
    if is_personal_mode is True:
        return ("person", "best-effort from recommendedPublication.is_personal_mode=true")
    if is_personal_mode is False:
        return ("publication", "best-effort from recommendedPublication.is_personal_mode=false")
    return ("unknown", "no reliable public UI-split signal present")


def redact_sensitive_fields(value):
    if isinstance(value, dict):
        redacted = {}
        for key, nested in value.items():
            lowered = str(key).lower()
            if "token" in lowered or "secret" in lowered or "auth" in lowered:
                redacted[key] = "[REDACTED]"
            else:
                redacted[key] = redact_sensitive_fields(nested)
        return redacted
    if isinstance(value, list):
        return [redact_sensitive_fields(item) for item in value]
    return value


def get_top_ranked_publications(db_path: Path, n: int = 20) -> list[tuple[str, str, float]]:
    """Get top N publications by PageRank. Returns list of (domain, name, pagerank)."""
    if nx is None:
        print("Error: networkx required for ranking. Install with: pip install networkx", file=sys.stderr)
        sys.exit(1)
    
    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    
    conn = connect_db(db_path)
    cur = conn.cursor()
    
    # Build directed graph from recommendations
    cur.execute("SELECT source_domain, target_domain FROM recommendations")
    edges = cur.fetchall()
    G = nx.DiGraph()
    for (src, tgt) in edges:
        if src and tgt:
            G.add_edge(src, tgt)
    
    if G.number_of_nodes() == 0:
        print("No edges in recommendations; nothing to rank.", file=sys.stderr)
        conn.close()
        sys.exit(1)
    
    # PageRank
    pagerank = nx.pagerank(G)
    
    # Domain -> name from publications
    cur.execute("SELECT domain, name FROM publications")
    domain_to_name = {row[0]: (row[1] or "").strip() or row[0] for row in cur.fetchall()}
    conn.close()
    
    # Build rankings: (domain, name, pagerank), sorted by pagerank desc
    rows = []
    for domain in pagerank:
        name = domain_to_name.get(domain, domain)
        rows.append((domain, name, pagerank[domain]))
    rows.sort(key=lambda r: r[2], reverse=True)
    
    return rows[:n]


def analyze_recommendations_report(db_path: Path, top_n: int = 20) -> None:
    """Analyze recommendations for top N publications and write markdown report."""
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    
    print(f"Fetching top {top_n} ranked publications...", file=sys.stderr)
    top_pubs = get_top_ranked_publications(db_path, top_n)
    
    report_lines = []
    report_lines.append("# Recommendations Analysis Report")
    report_lines.append("")
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    report_lines.append(f"Analyzed top {top_n} publications by PageRank.")
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")
    
    total_recommendations = 0
    total_personal_mode_true = 0
    total_personal_mode_false = 0
    total_personal_mode_none = 0
    failed_fetches = 0
    
    for rank, (domain, name, pagerank) in enumerate(top_pubs, 1):
        print(f"[{rank}/{top_n}] Fetching recommendations for {domain}...", file=sys.stderr)
        
        base_url = f"https://{domain}" if not domain.startswith("http") else domain
        recs = get_recommendations(base_url)
        
        report_lines.append(f"## {rank}. {name}")
        report_lines.append("")
        report_lines.append(f"- **Domain:** `{domain}`")
        report_lines.append(f"- **PageRank:** {pagerank:.6f}")
        report_lines.append("")
        
        if recs is None:
            report_lines.append("- **Status:** ❌ Failed to fetch recommendations")
            report_lines.append("")
            failed_fetches += 1
        else:
            total_recommendations += len(recs)
            personal_mode_true = 0
            personal_mode_false = 0
            personal_mode_none = 0
            
            for rec in recs:
                pub = rec.get("recommendedPublication", {})
                is_personal = pub.get("is_personal_mode")
                if is_personal is True:
                    personal_mode_true += 1
                    total_personal_mode_true += 1
                elif is_personal is False:
                    personal_mode_false += 1
                    total_personal_mode_false += 1
                else:
                    personal_mode_none += 1
                    total_personal_mode_none += 1
            
            report_lines.append(f"- **Total recommendations:** {len(recs)}")
            report_lines.append(f"- **`is_personal_mode: true`:** {personal_mode_true}")
            report_lines.append(f"- **`is_personal_mode: false`:** {personal_mode_false}")
            if personal_mode_none > 0:
                report_lines.append(f"- **`is_personal_mode: None/missing`:** {personal_mode_none}")
            report_lines.append("")
            
            # List all recommendations with their is_personal_mode
            if recs:
                report_lines.append("### Recommendations")
                report_lines.append("")
                for rec in recs:
                    pub = rec.get("recommendedPublication", {})
                    rec_url = None
                    if pub.get("custom_domain"):
                        rec_url = pub["custom_domain"]
                        if not rec_url.startswith("http"):
                            rec_url = f"https://{rec_url}"
                    elif pub.get("subdomain"):
                        rec_url = f"https://{pub['subdomain']}.substack.com"
                    
                    if rec_url:
                        ui_guess, provenance = ui_guess_from_publication(pub)
                        if ui_guess == "person":
                            icon = ICON_PERSON
                        elif ui_guess == "publication":
                            icon = ICON_PUBLICATION
                        else:
                            icon = "❓"
                        report_lines.append(
                            f"- {icon} [{rec_url}]({rec_url}) — best-effort `ui_guess`: `{ui_guess}` ({provenance})"
                        )
                report_lines.append("")
        
        report_lines.append("---")
        report_lines.append("")
    
    # Summary
    report_lines.append("## Summary")
    report_lines.append("")
    report_lines.append(f"- **Publications analyzed:** {len(top_pubs)}")
    report_lines.append(f"- **Failed fetches:** {failed_fetches}")
    report_lines.append(f"- **Total recommendations:** {total_recommendations}")
    report_lines.append(f"- **`is_personal_mode: true`:** {total_personal_mode_true}")
    report_lines.append(f"- **`is_personal_mode: false`:** {total_personal_mode_false}")
    if total_personal_mode_none > 0:
        report_lines.append(f"- **`is_personal_mode: None/missing`:** {total_personal_mode_none}")
    
    # Write report
    data_dir = root / "data"
    data_dir.mkdir(exist_ok=True)
    report_path = data_dir / "recommendations_analysis.md"
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    
    print(f"\nReport written to: {report_path}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch recommended publications for a Substack publication. Output: one URL per line (or --json for raw API response). If no URL provided, analyzes top 20 ranked publications and writes report to data/recommendations_analysis.md"
    )
    parser.add_argument(
        "url_or_domain",
        nargs="?",
        default=None,
        help="Publication URL or domain (e.g. paulkrugman.substack.com or https://paulkrugman.substack.com). If omitted, analyzes top 20 ranked publications.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print redacted recommendations JSON instead of formatted output",
    )
    parser.add_argument(
        "--raw-json",
        action="store_true",
        help="Print unredacted recommendations JSON. Use only when raw sensitive fields are required.",
    )
    parser.add_argument(
        "--separate",
        action="store_true",
        help="Print two sections: best-effort Publications then best-effort People",
    )
    parser.add_argument(
        "--only-publications",
        action="store_true",
        help="Print only URLs where the best-effort UI guess is publication",
    )
    parser.add_argument(
        "--only-people",
        action="store_true",
        help="Print only URLs where the best-effort UI guess is person",
    )
    parser.add_argument(
        "--as-people",
        action="store_true",
        help="Treat all items as 'person' (follow list). Use when the UI shows 'follow n people' but the API returns all is_personal_mode=false.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="When no URL provided, analyze top N publications (default: 20)",
    )
    args = parser.parse_args()

    # If no URL provided, run analysis report
    if args.url_or_domain is None:
        root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
        db_path = root / "cartographer.db"
        analyze_recommendations_report(db_path, args.top_n)
        return

    base = url_to_base(args.url_or_domain)
    if not base:
        print("Invalid URL or domain.", file=sys.stderr)
        sys.exit(1)

    recs = get_recommendations(base)
    if recs is None:
        print("Could not fetch recommendations (archive or recommendations API failed).", file=sys.stderr)
        sys.exit(1)

    if args.json and args.raw_json:
        print("Choose only one of --json or --raw-json.", file=sys.stderr)
        sys.exit(1)
    if args.raw_json:
        print(json.dumps(recs, indent=2))
        return
    if args.json:
        print(json.dumps(redact_sensitive_fields(recs), indent=2))
        return

    records = recommendation_records(recs, force_people=args.as_people)
    publications = [record for record in records if record["ui_guess"] == "publication"]
    people = [record for record in records if record["ui_guess"] == "person"]

    if args.only_publications:
        for record in publications:
            print(f"{ICON_PUBLICATION}\t{record['ui_guess']}\t{record['provenance']}\t{record['url']}")
        return
    if args.only_people:
        for record in people:
            print(f"{ICON_PERSON}\t{record['ui_guess']}\t{record['provenance']}\t{record['url']}")
        return
    if args.separate:
        print("=== 📰 Best-Effort Publications ===")
        for record in publications:
            print(f"  {ICON_PUBLICATION}\t{record['ui_guess']}\t{record['provenance']}\t{record['url']}")
        print("\n=== 👤 Best-Effort People ===")
        for record in people:
            print(f"  {ICON_PERSON}\t{record['ui_guess']}\t{record['provenance']}\t{record['url']}")
        return

    for record in records:
        icon = ICON_BY_KIND.get(str(record["ui_guess"]), "❓")
        print(f"{icon}\t{record['ui_guess']}\t{record['provenance']}\t{record['url']}")


if __name__ == "__main__":
    main()
