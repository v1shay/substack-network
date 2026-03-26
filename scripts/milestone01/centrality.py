#!/usr/bin/env python3
"""
Compute PageRank (and in-degree) on the recommendation graph.
Loads publications + recommendations from cartographer.db, builds a directed graph,
runs PageRank, and outputs a top-N rankings table (and optional CSV to data/).

Requires: pip install networkx

Usage (from repo root, cartographer.db at repo root; or set CARTOGRAPHER_ROOT):
    python scripts/milestone01/centrality.py
    python scripts/milestone01/centrality.py -n 100
    python scripts/milestone01/centrality.py -o data/pagerank.csv
    python scripts/milestone01/centrality.py --db path/to/cartographer.db
"""

import argparse
import csv
import os
import sqlite3
import sys
from pathlib import Path

try:
    import networkx as nx
except ImportError:
    print("❌ networkx not found. Install with: pip install networkx", file=sys.stderr)
    sys.exit(1)


def main():
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    out_csv = None
    top_n = 50
    parser = argparse.ArgumentParser(description="PageRank on recommendation graph (cartographer.db).")
    parser.add_argument("--db", type=str, default=None, help="Path to cartographer.db (default: cwd/cartographer.db)")
    parser.add_argument("-n", type=int, default=50, metavar="N", help="Show and export top N by PageRank (default: 50)")
    parser.add_argument("-o", "--output", type=str, default=None, help="Write rankings to this CSV (e.g. data/pagerank.csv)")
    args = parser.parse_args()
    if args.db:
        db_path = Path(args.db).resolve()
    if args.output:
        out_csv = Path(args.output).resolve()
    top_n = args.n

    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
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

    # PageRank and in-degree
    pagerank = nx.pagerank(G)
    in_degree = dict(G.in_degree())

    # Domain -> name from publications (optional labels)
    cur.execute("SELECT domain, name FROM publications")
    domain_to_name = {row[0]: (row[1] or "").strip() or row[0] for row in cur.fetchall()}
    # Domain -> depth from queue (BFS depth; None if not in queue)
    cur.execute("SELECT domain, depth FROM queue")
    domain_to_depth = {row[0]: row[1] for row in cur.fetchall()}
    conn.close()

    # Build rankings: (domain, name, pagerank, in_degree, depth), sorted by pagerank desc
    rows = []
    for node in pagerank:
        name = domain_to_name.get(node, node)
        depth = domain_to_depth.get(node)
        rows.append((node, name, pagerank[node], in_degree.get(node, 0), depth))
    rows.sort(key=lambda r: r[2], reverse=True)

    # Top N
    top = rows[:top_n]

    # Print table
    print(f"Top {top_n} by PageRank (nodes={G.number_of_nodes()}, edges={G.number_of_edges()})\n")
    print(f"{'rank':<6} {'domain':<35} {'name':<30} {'pagerank':<12} {'in_degree':<10} {'depth':<6}")
    print("-" * 102)
    for i, (domain, name, pr, deg, depth) in enumerate(top, 1):
        name_short = (name[:27] + "..") if len(name) > 29 else name
        domain_short = (domain[:32] + "..") if len(domain) > 34 else domain
        depth_str = str(depth) if depth is not None else "—"
        print(f"{i:<6} {domain_short:<35} {name_short:<30} {pr:<12.6f} {deg:<10} {depth_str:<6}")

    # Optional CSV
    if out_csv:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["rank", "domain", "name", "pagerank", "in_degree", "depth"])
            for i, (domain, name, pr, deg, depth) in enumerate(top, 1):
                w.writerow([i, domain, name, f"{pr:.6f}", deg, depth if depth is not None else ""])
        print(f"\nWrote {len(top)} rows to {out_csv}")


if __name__ == "__main__":
    main()
