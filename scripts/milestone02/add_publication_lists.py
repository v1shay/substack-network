#!/usr/bin/env python3
"""
Generate two list pages in data/ and add links to them in index.html (below the graph):
  1. data/graph-publications.html — all publications in the current graph with their PageRank
  2. data/db-publications.html — all publications in the database (with a PageRank distribution chart above the table)

The chart (rank vs PageRank + power-law fit) is produced by pagerank_distribution.py; add_publication_lists runs it and embeds the fragment. Run from repo root after scripts/milestone01/visualize.py so the graph and index.html exist. Uses the same top-N as the default graph (see visualize.py DEFAULT_TOP_N) so the graph list matches what you see.

Usage (from repo root):
    python scripts/milestone02/add_publication_lists.py
    python scripts/milestone02/add_publication_lists.py -n 300
"""

import argparse
import os
import subprocess
import sqlite3
import sys
from pathlib import Path

try:
    import networkx as nx
except ImportError:
    print("❌ networkx required. pip install networkx", file=sys.stderr)
    sys.exit(1)


def domain_to_archive_url(domain: str) -> str:
    if "." in domain and "substack.com" not in domain:
        base = f"https://{domain}"
    else:
        clean = domain.replace(".substack.com", "")
        base = f"https://{clean}.substack.com"
    return f"{base}/archive"


def html_head(title: str, back_href: str = "index.html") -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/css/bootstrap.min.css" rel="stylesheet" crossorigin="anonymous">
</head>
<body>
<div class="container mt-4">
  <h1>{title}</h1>
  <p><a href="{back_href}" id="back-to-graph">← Back to graph</a></p>
"""


def html_foot() -> str:
    return """</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/js/bootstrap.bundle.min.js" crossorigin="anonymous"></script>
<script>
  (function() {
    if (window.parent !== window) {
      var a = document.getElementById('back-to-graph');
      if (a) a.addEventListener('click', function(e) { e.preventDefault(); parent.postMessage('showGraph', '*'); });
    }
  })();
</script>
</body>
</html>
"""


def main() -> None:
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    top_n = 300  # must match scripts/milestone01/visualize.py DEFAULT_TOP_N for graph list to match graph
    parser = argparse.ArgumentParser(description="Add graph + DB publication list pages and links in index.html")
    parser.add_argument("--db", type=str, default=None, help="Path to cartographer.db")
    parser.add_argument("-n", type=int, default=300, metavar="N", help="Top N nodes (must match graph default for graph list)")
    args = parser.parse_args()
    if args.db:
        db_path = Path(args.db).resolve()
    top_n = max(10, min(args.n, 1000))

    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT source_domain, target_domain FROM recommendations")
    edges = cur.fetchall()
    G = nx.DiGraph()
    for (src, tgt) in edges:
        if src and tgt:
            G.add_edge(src, tgt)
    if G.number_of_nodes() == 0:
        print("No edges in recommendations.", file=sys.stderr)
        conn.close()
        sys.exit(1)

    pagerank = nx.pagerank(G)
    cur.execute("SELECT domain, name FROM publications")
    domain_to_name = {row[0]: (row[1] or "").strip() or row[0] for row in cur.fetchall()}

    # Top N by PageRank (same as visualize.py)
    # Same tie-break (domain) as db list so positions match for nodes in both lists
    sorted_nodes = sorted(pagerank.keys(), key=lambda n: (-pagerank[n], n))[:top_n]
    sub = G.subgraph(sorted_nodes)

    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    # ---- graph-publications.html (in data/) ----
    graph_path = data_dir / "graph-publications.html"
    rows = []
    for i, domain in enumerate(sorted_nodes, 1):
        name = domain_to_name.get(domain, domain)
        pr = pagerank[domain]
        indeg = sub.in_degree(domain)
        outdeg = sub.out_degree(domain)
        url = domain_to_archive_url(domain)
        rows.append(f'    <tr><td>{i}</td><td><a href="{_h(url)}" target="_blank" rel="noopener">{_h(name)}</a></td><td>{pr:.6f}</td><td>{indeg}</td><td>{outdeg}</td></tr>')
    graph_html = (
        html_head("Publications in the graph (with PageRank)", back_href="../index.html")
        + """  <p class="text-muted small">PageRank is on the full network; In/Out are edges within this graph only (to/from other nodes in the list).</p>
  <table class="table table-striped">
  <thead><tr><th>#</th><th>Publication</th><th>PageRank</th><th>In</th><th>Out</th></tr></thead>
  <tbody>
"""
        + "\n".join(rows)
        + """
  </tbody>
  </table>
"""
        + html_foot()
    )
    graph_path.write_text(graph_html, encoding="utf-8")
    print(f"Wrote {graph_path}")

    # ---- db-publications.html (all in DB, same columns as graph list, sorted by PageRank) ----
    cur.execute("SELECT domain, name FROM publications ORDER BY domain")
    all_pubs = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM queue WHERE status = 'failed'")
    failed_count = cur.fetchone()[0]
    conn.close()

    # Build (domain, name, pr, indeg, outdeg); use full G for degrees and PageRank
    db_rows = []
    for domain, name in all_pubs:
        name = (name or "").strip() or domain
        if domain in G:
            pr = pagerank[domain]
            indeg = G.in_degree(domain)
            outdeg = G.out_degree(domain)
        else:
            pr = 0.0
            indeg = 0
            outdeg = 0
        db_rows.append((domain, name, pr, indeg, outdeg))
    db_rows.sort(key=lambda x: (-x[2], x[0]))  # PageRank desc, then domain

    db_path_html = data_dir / "db-publications.html"
    rows = []
    for i, (domain, name, pr, indeg, outdeg) in enumerate(db_rows, 1):
        url = domain_to_archive_url(domain)
        rows.append(f'    <tr><td>{i}</td><td><a href="{_h(url)}" target="_blank" rel="noopener">{_h(name)}</a></td><td>{pr:.6f}</td><td>{indeg}</td><td>{outdeg}</td></tr>')

    # PageRank distribution chart (rank vs PageRank + power-law fit) above the table
    chart_fragment = ""
    script_dir = Path(__file__).resolve().parent
    pagerank_distribution_py = script_dir / "pagerank_distribution.py"
    cmd = [sys.executable, str(pagerank_distribution_py), "--html-fragment"]
    if args.db:
        cmd.extend(["--db", str(Path(args.db).resolve())])
    try:
        result = subprocess.run(
            cmd,
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0 and result.stdout.strip():
            chart_fragment = result.stdout.strip() + "\n"
        else:
            if result.stderr:
                print(f"  [pagerank_distribution: {result.stderr.strip()}]", file=sys.stderr)
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        print(f"  [Skipping PageRank chart: {e}]", file=sys.stderr)

    db_html = (
        html_head("All publications in the database", back_href="../index.html")
        + chart_fragment
        + f"""  <p>{len(all_pubs)} publications.</p>
  <table class="table table-striped">
  <thead><tr><th>#</th><th>Publication</th><th>PageRank</th><th>In</th><th>Out</th></tr></thead>
  <tbody>
"""
        + "\n".join(rows)
        + """
  </tbody>
  </table>
"""
        + html_foot()
    )
    db_path_html.write_text(db_html, encoding="utf-8")
    print(f"Wrote {db_path_html}")

    # ---- index.html: thin wrapper that embeds the graph (no duplication of graph data) ----
    index_path = root / "index.html"
    index_html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Substack recommendation graph</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/css/bootstrap.min.css" rel="stylesheet" crossorigin="anonymous">
</head>
<body>
  <div id="graph-panel" class="container-fluid p-0">
    <iframe id="graph-iframe" src="data/substack_graph.html" title="Recommendation graph" style="width:100%; height:720px; border:none;"></iframe>
  </div>
  <div id="list-panel" style="display:none; width:100%; height:720px;">
    <iframe id="list-iframe" title="Publication list" style="width:100%; height:100%; border:none;"></iframe>
  </div>
  <div class="ps-3 pt-3">
    <p class="mb-0"><a href="#" id="link-graph-list">Publications in graph</a> | <a href="#" id="link-db-list">Publications in database</a> | <a href="#" id="link-layer-stats">Layer stats</a> | <a href="#" id="link-failed">Failed publications</a> | <a href="#" id="link-unfailed">Unfailed publications</a></p>
  </div>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/js/bootstrap.bundle.min.js" crossorigin="anonymous"></script>
  <script>
    (function() {
      var graphPanel = document.getElementById('graph-panel');
      var listPanel = document.getElementById('list-panel');
      var listIframe = document.getElementById('list-iframe');
      function showList(src) {
        listIframe.src = src;
        graphPanel.style.display = 'none';
        listPanel.style.display = 'block';
      }
      document.getElementById('link-graph-list').addEventListener('click', function(e) {
        e.preventDefault();
        showList('data/graph-publications.html');
      });
      document.getElementById('link-db-list').addEventListener('click', function(e) {
        e.preventDefault();
        showList('data/db-publications.html');
      });
      document.getElementById('link-layer-stats').addEventListener('click', function(e) {
        e.preventDefault();
        showList('data/layer_stats.html');
      });
      document.getElementById('link-failed').addEventListener('click', function(e) {
        e.preventDefault();
        showList('data/failed_publications.html');
      });
      document.getElementById('link-unfailed').addEventListener('click', function(e) {
        e.preventDefault();
        showList('data/unfailed_publications.html');
      });
      window.addEventListener('message', function(e) {
        if (e.data === 'showGraph') {
          listPanel.style.display = 'none';
          graphPanel.style.display = 'block';
        }
      });
      // Forward Shift to iframe so freeze works without clicking inside the graph first
      window.addEventListener('keydown', function(e) {
        if (e.key === 'Shift' && !e.repeat) {
          var iframe = document.getElementById('graph-iframe');
          if (iframe && iframe.contentWindow) iframe.contentWindow.postMessage('togglePhysics', '*');
        }
      });
    })();
  </script>
</body>
</html>
"""
    index_path.write_text(index_html, encoding="utf-8")
    print(f"Wrote {index_path} (wrapper: iframe → data/substack_graph.html + links).")


def _h(s: str) -> str:
    """Escape for HTML text/attribute."""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


if __name__ == "__main__":
    main()
