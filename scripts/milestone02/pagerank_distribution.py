#!/usr/bin/env python3
"""
PageRank distribution: rank–PageRank plot and power-law fit (Zipf-like).

Loads the recommendation graph from cartographer.db, computes PageRank, sorts publications
by PageRank (rank 1 = highest). Fits a power law PR(r) ≈ c·r^(-α) in log-log space and
reports exponent α and R². Can output JSON and/or an HTML fragment for embedding above
the "All publications in the database" table.

Usage (from repo root):
    python scripts/milestone02/pagerank_distribution.py --json
    python scripts/milestone02/pagerank_distribution.py --html-fragment
    python scripts/milestone02/pagerank_distribution.py --json --html-fragment
"""

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

try:
    import networkx as nx
    import numpy as np
except ImportError as e:
    print(f"❌ {e}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PageRank distribution and power-law fit (rank vs PageRank)."
    )
    parser.add_argument("--db", type=str, default=None, help="Path to cartographer.db")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Write data/pagerank_distribution.json",
    )
    parser.add_argument(
        "--html-fragment",
        action="store_true",
        help="Print HTML fragment (chart + inlined data) to stdout for embedding",
    )
    args = parser.parse_args()

    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db" if args.db is None else Path(args.db).resolve()

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
    cur.execute("SELECT domain FROM publications ORDER BY domain")
    all_domains = [row[0] for row in cur.fetchall()]
    conn.close()

    # (rank, pr) with rank 1 = highest PageRank
    db_rows = []
    for domain in all_domains:
        pr = pagerank.get(domain, 0.0)
        db_rows.append((domain, pr))
    db_rows.sort(key=lambda x: (-x[1], x[0]))
    ranks = list(range(1, len(db_rows) + 1))
    prs = [r[1] for r in db_rows]

    # Power-law fit: PR(r) ≈ c·r^(-α)  =>  log(PR) = log(c) - α·log(r)
    # Use only points with pr > 0
    fit_ranks = []
    fit_prs = []
    for r, pr in zip(ranks, prs):
        if pr > 0:
            fit_ranks.append(r)
            fit_prs.append(pr)
    if len(fit_ranks) < 2:
        c, alpha, r_squared = 0.0, 0.0, 0.0
        fit_line = []
    else:
        log_r = np.log(np.array(fit_ranks, dtype=float))
        log_pr = np.log(np.array(fit_prs, dtype=float))
        coeffs = np.polyfit(log_r, log_pr, 1)
        alpha = -coeffs[0]
        log_c = coeffs[1]
        c = np.exp(log_c)
        pred = log_c + coeffs[0] * log_r
        ss_res = np.sum((log_pr - pred) ** 2)
        ss_tot = np.sum((log_pr - np.mean(log_pr)) ** 2)
        r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
        fit_line = [[int(r), float(c * (r ** (-alpha)))] for r in ranks if r >= 1]

    points = [[int(r), float(pr)] for r, pr in zip(ranks, prs)]
    out = {
        "points": points,
        "power_law": {"c": float(c), "alpha": float(alpha), "r_squared": float(r_squared)},
        "n_publications": len(ranks),
    }

    if args.json:
        data_dir = root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        json_path = data_dir / "pagerank_distribution.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"Wrote {json_path}", file=sys.stderr)

    if args.html_fragment:
        # Inline JSON for Chart.js (escape for script tag: avoid </script>)
        data_js = json.dumps(out).replace("</", "<\\/")
        fragment = f'''  <div class="mb-4">
    <h2 class="h5">PageRank distribution (rank vs PageRank)</h2>
    <p class="text-muted small"><strong>Rank</strong> = position when publications are sorted by PageRank (rank 1 = highest). <strong>Power-law fit</strong> PR(r) ≈ c·r<sup>−α</sup>: <strong>α</strong> = {alpha:.3f} (steepness; Zipf-like when α ≈ 1), <strong>c</strong> = {c:.2e}, <strong>R²</strong> = {r_squared:.4f} (fit quality).</p>
    <div style="height:280px;"><canvas id="pagerank-distribution-chart"></canvas></div>
  </div>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js" crossorigin="anonymous"></script>
  <script>
(function() {{
  var out = {data_js};
  var pts = out.points;
  var fit = out.power_law;
  var scatterData = pts.slice(0, 1000).map(function(p) {{ return {{ x: p[0], y: p[1] }}; }});
  for (var i = 1000; i < pts.length; i++) {{
    if ((i - 1000) % 100 === 0) scatterData.push({{ x: pts[i][0], y: pts[i][1] }});
  }}
  var lineData = [];
  if (fit.alpha > 0 && pts.length > 0) {{
    var maxRank = pts.length;
    for (var i = 0; i <= 250; i++) {{
      var r = 1 + (maxRank - 1) * (i / 250);
      lineData.push({{ x: r, y: fit.c * Math.pow(r, -fit.alpha) }});
    }}
  }}
  var datasets = [
    // Publication data first
    {{ label: 'PageRank (data)', data: scatterData, backgroundColor: 'rgba(0,0,0,0.92)', borderColor: 'rgba(0,0,0,0.92)', pointBorderWidth: 0, pointRadius: 1.5 }},
    // Red fit on top, transparent so points remain visible
    {{ label: 'Power-law fit', data: lineData, type: 'line', borderColor: 'rgba(220,53,69,0.55)', borderWidth: 12, fill: false, pointRadius: 0, tension: 0 }}
  ];
  var ctx = document.getElementById('pagerank-distribution-chart').getContext('2d');
  new Chart(ctx, {{
    type: 'scatter',
    data: {{ datasets: datasets }},
    options: {{
      animation: {{ duration: 0 }},
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        tooltip: {{
          enabled: false,
          external: function(context) {{
            var tooltipEl = document.getElementById('pagerank-chart-tooltip');
            if (!tooltipEl) {{
              tooltipEl = document.createElement('div');
              tooltipEl.id = 'pagerank-chart-tooltip';
              tooltipEl.style.position = 'absolute';
              tooltipEl.style.pointerEvents = 'none';
              tooltipEl.style.background = 'rgba(0,0,0,0.8)';
              tooltipEl.style.color = '#fff';
              tooltipEl.style.borderRadius = '4px';
              tooltipEl.style.padding = '6px 8px';
              tooltipEl.style.fontSize = '12px';
              tooltipEl.style.zIndex = '9999';
              tooltipEl.style.whiteSpace = 'nowrap';
              document.body.appendChild(tooltipEl);
            }}
            var tooltip = context.tooltip;
            if (!tooltip || tooltip.opacity === 0 || !tooltip.dataPoints || tooltip.dataPoints.length === 0) {{
              tooltipEl.style.opacity = 0;
              return;
            }}
            var dp = tooltip.dataPoints[0];
            var rank = dp.parsed.x;
            var pr = dp.parsed.y;
            var label = dp.dataset.label || 'Point';
            tooltipEl.textContent = label + ': PageRank ' + Number(pr).toFixed(6) + ', rank ' + rank;

            var rect = context.chart.canvas.getBoundingClientRect();
            tooltipEl.style.opacity = 1;
            // Fixed top-right offset from hovered anchor.
            tooltipEl.style.left = (window.pageXOffset + rect.left + tooltip.caretX + 24) + 'px';
            tooltipEl.style.top = (window.pageYOffset + rect.top + tooltip.caretY - 42) + 'px';
          }}
        }}
      }},
      scales: {{
        x: {{ title: {{ display: true, text: 'Rank' }}, type: 'linear' }},
        y: {{ title: {{ display: true, text: 'PageRank' }}, type: 'logarithmic' }}
      }}
    }}
  }});
}})();
  </script>
'''
        print(fragment)


if __name__ == "__main__":
    main()
