#!/usr/bin/env python3
"""
Build an interactive HTML graph of the recommendation network (pyvis).
Uses the same DB and PageRank as milestone02-centrality.py. Limits to top N nodes
by PageRank so the graph stays readable; node size = PageRank, tooltip = name + rank.
Click a node to open its Substack page in a new tab.

Requires: pip install pyvis networkx numpy scipy

Usage (from repo root, cartographer.db at repo root):
    python scripts/milestone01/visualize.py
    python scripts/milestone01/visualize.py -n 300 -o data/substack_graph.html
"""

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

try:
    import networkx as nx
except ImportError:
    print("❌ networkx required. pip install networkx numpy scipy", file=sys.stderr)
    sys.exit(1)
try:
    from pyvis.network import Network
except ImportError:
    print("❌ pyvis required. pip install pyvis", file=sys.stderr)
    sys.exit(1)

# Number of nodes in the graph (top N by PageRank). Must match add_publication_lists.py -n when generating the graph list.
DEFAULT_TOP_N = 300
TOP_N_MAX = 1000


def domain_to_url_for_click(domain: str) -> str:
    """URL to open on node click. Always use /archive so the post list opens instead of the subscribe page."""
    if "." in domain and "substack.com" not in domain:
        base = f"https://{domain}"
    else:
        clean_sub = domain.replace(".substack.com", "")
        base = f"https://{clean_sub}.substack.com"
    return f"{base}/archive"


def main():
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    out_path = root / "data" / "substack_graph.html"
    parser = argparse.ArgumentParser(description="Interactive HTML graph (pyvis) of recommendation network.")
    parser.add_argument("--db", type=str, default=None, help="Path to cartographer.db")
    parser.add_argument("-n", type=int, default=DEFAULT_TOP_N, metavar="N", help=f"Top N nodes by PageRank to show (default: {DEFAULT_TOP_N}, max: {TOP_N_MAX})")
    parser.add_argument("-o", "--output", type=str, default=None, help="Output HTML path (default: data/substack_graph.html)")
    args = parser.parse_args()
    if args.db:
        db_path = Path(args.db).resolve()
    if args.output:
        out_path = Path(args.output).resolve()
    top_nodes = max(10, min(args.n, TOP_N_MAX))

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
    in_degree = dict(G.in_degree())
    cur.execute("SELECT domain, name FROM publications")
    domain_to_name = {row[0]: (row[1] or "").strip() or row[0] for row in cur.fetchall()}
    conn.close()

    # Top N nodes by PageRank; subgraph = only those nodes and edges between them
    # Tie-break by domain so rank matches graph list and db list (add_publication_lists.py)
    sorted_nodes = sorted(pagerank.keys(), key=lambda n: (-pagerank[n], n))[:top_nodes]
    sub = G.subgraph(sorted_nodes).copy()

    # Scale PageRank to node size (e.g. 8–35)
    pr_values = [pagerank[n] for n in sub.nodes()]
    min_pr = min(pr_values)
    max_pr = max(pr_values)
    span = max_pr - min_pr if max_pr > min_pr else 1.0

    def size_for(pr):
        return 8 + 27 * (pr - min_pr) / span

    net = Network(directed=True, height="700px", width="100%", notebook=False)
    net.barnes_hut(gravity=-8000, central_gravity=0.3, spring_length=150, spring_strength=0.001)

    for node in sub.nodes():
        pr = pagerank[node]
        deg = in_degree.get(node, 0)
        name = domain_to_name.get(node, node)
        # Drop "www." from label for cleaner display (tooltip still shows full domain)
        label_text = name[4:].lstrip(".") if name.startswith("www.") else name
        label_text = label_text[:25] + ".." if len(label_text) > 27 else label_text
        rank_approx = sorted_nodes.index(node) + 1 if node in sorted_nodes else 0
        title = f"{name}\ndomain: {node}\nPageRank: {pr:.4f}\nin-degree: {deg}\n(rank ≈ {rank_approx})\nClick to open Substack page"
        net.add_node(
            node,
            label=label_text,
            title=title,
            value=size_for(pr),
            font={"size": 16},
        )
    for u, v in sub.edges():
        net.add_edge(u, v)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    net.save_graph(str(out_path))

    # Inject node id -> URL map and click handler (pyvis has no built-in link on click). URL used only on click, not in graph.
    node_to_url = {node: domain_to_url_for_click(node) for node in sub.nodes()}
    with open(out_path, "r", encoding="utf-8") as f:
        html = f.read()
    # JSON-escape for embedding in JS: node ids are domains, may contain dots etc.
    url_map_js = "var nodeIdToUrl = " + json.dumps(node_to_url) + ";"
    click_js = (
        'network.on("click", function(params) {'
        ' if (params.nodes.length === 1) {'
        " var url = nodeIdToUrl[params.nodes[0]];"
        " if (url) window.open(url, '_blank');"
        " }"
        "});"
    )
    # Press Shift once to freeze nodes (press again to unfreeze).
    # Listen on window for keydown when graph has focus; also listen for postMessage so parent (index.html in iframe) can forward Shift.
    shift_freeze_js = (
        "var physicsFrozen = false;"
        "function togglePhysics() {"
        " physicsFrozen = !physicsFrozen;"
        " network.setOptions({ physics: { enabled: !physicsFrozen } });"
        "}"
        "container.setAttribute('tabindex', 0);"
        "container.addEventListener('click', function() { container.focus(); });"
        "window.addEventListener('keydown', function(e) {"
        " if (e.key === 'Shift' && !e.repeat) { e.preventDefault(); togglePhysics(); }"
        "});"
        "window.addEventListener('message', function(e) { if (e.data === 'togglePhysics') togglePhysics(); });"
    )
    # Insert or update map and handlers right after "network = new vis.Network(container, data, options);"
    old = "network = new vis.Network(container, data, options);"
    if old not in html:
        pass  # pyvis output changed, skip injection
    elif "nodeIdToUrl" not in html:
        injection = (
            "\n                  " + url_map_js + "\n                  " + click_js
            + "\n                  " + shift_freeze_js
        )
        html = html.replace(old, old + injection)
    else:
        # Already injected: update URL map so /archive and node set stay in sync when graph is regenerated
        start = html.find("var nodeIdToUrl = ")
        if start != -1:
            end = html.find(";", start)
            if end != -1:
                html = html[: start + len("var nodeIdToUrl = ")] + json.dumps(node_to_url) + html[end:]
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    # index.html at repo root is a thin wrapper (iframe + links) written by scripts/milestone02/add_publication_lists.py
    print(f"Wrote interactive graph to {out_path}")
    print(f"  Nodes: {sub.number_of_nodes()}, edges: {sub.number_of_edges()}")
    print("  Open in a browser: click a node to open its Substack page.")
    print("  Press Shift to freeze nodes (press again to unfreeze).")


if __name__ == "__main__":
    main()
