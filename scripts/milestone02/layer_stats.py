#!/usr/bin/env python3
"""
Compute layer sizes L(d), accumulation A(d), growth ratio r(d), and L'(d) from the crawl queue.
Writes data/layer_stats.html for the UI (table + chart). See docs/bfs.md for definitions.

Usage (from repo root):
    python scripts/milestone02/layer_stats.py
"""

import json
import os
import sqlite3
import sys
from pathlib import Path


def main() -> None:
    root = Path(os.environ.get("CARTOGRAPHER_ROOT", Path.cwd())).resolve()
    db_path = root / "cartographer.db"
    out_path = root / "data" / "layer_stats.html"

    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT depth, COUNT(*) AS L FROM queue GROUP BY depth ORDER BY depth"
    )
    rows = cur.fetchall()  # (depth, L)
    
    # L(d); A(d) = accumulation (cumulative sum of L); L'(d); r(d). Chart hides L', r for last depth.
    layers = []  # (depth, L, A, L_prime, r)
    cum = 0
    for i, (depth, L) in enumerate(rows):
        cum += L
        prev_L = rows[i - 1][1] if i > 0 else None
        L_prime = (L - prev_L) if prev_L is not None else None
        r = (L / prev_L) if prev_L and prev_L > 0 else None
        layers.append((depth, L, cum, L_prime, r))

    # Peak depth d* = argmax L(d)
    if layers:
        peak_depth, peak_L = max(layers, key=lambda x: x[1])[:2]
    else:
        peak_depth = peak_L = 0
    cur.execute("SELECT COUNT(*) FROM publications")
    total_publications = cur.fetchone()[0]

    table_rows = []
    for depth, L, A, L_prime, r in layers:
        Lp_str = f"{L_prime:+,d}" if L_prime is not None else "—"
        r_str = f"{r:.3f}" if r is not None else "—"
        table_rows.append(
            f"    <tr><td>{depth}</td><td>{A}</td><td>{L}</td><td>{Lp_str}</td><td>{r_str}</td></tr>"
        )

    depths_js = json.dumps([x[0] for x in layers])
    L_js = json.dumps([x[1] for x in layers])
    A_js = json.dumps([x[2] for x in layers])
    # Chart: hide L', r only for the last depth (layer still being built)
    n = len(layers)
    L_prime_js = json.dumps([layers[i][3] if i < n - 1 else None for i in range(n)])
    r_js = json.dumps([layers[i][4] if i < n - 1 else None for i in range(n)])

    # Progress line: show on the layer being *crawled* (where pending → crawled/failed),
    # not the layer being *filled* (rightmost = where we enqueue new nodes).
    # "Layer being crawled" = shallowest depth that still has pending nodes (BFS order).
    # Line at y = processed count (crawled + failed); when that layer is done, line at top of bar.
    current_working_index = None
    processed_count = 0
    if layers:
        cur.execute(
            "SELECT MIN(depth) FROM queue WHERE status = 'pending'"
        )
        row = cur.fetchone()
        depth_with_pending = row[0] if row and row[0] is not None else None
        if depth_with_pending is not None:
            # Find index of that depth in our layers list
            for i, (d, _, _, _, _) in enumerate(layers):
                if d == depth_with_pending:
                    current_working_index = i
                    current_working_depth = d
                    break
        if current_working_index is None:
            # No pending anywhere: use last layer (all done; line would be at top)
            current_working_index = len(layers) - 1
            current_working_depth = layers[current_working_index][0]
        cur.execute(
            "SELECT COUNT(*) FROM queue WHERE depth=? AND status IN ('crawled', 'failed')",
            (current_working_depth,),
        )
        processed_count = cur.fetchone()[0]

    conn.close()

    layer6_progress_js = json.dumps(
        [processed_count if i == current_working_index else None for i in range(len(layers))]
    )
    
    table_body = chr(10).join(table_rows) if table_rows else '    <tr><td colspan="5">No queue data yet.</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Layer stats (convergence)</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/css/bootstrap.min.css" rel="stylesheet" crossorigin="anonymous">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3"></script>
</head>
<body>
<div class="container mt-4">
  <h1>Layer stats (convergence)</h1>
  <p><a href="../index.html" id="back-to-graph">← Back to graph</a></p>
  <p class="text-muted small">L(d) = nodes at depth d. A(d) = L(0)+…+L(d) (accumulation). L'(d) = L(d)−L(d−1). r(d) = L(d)/L(d−1). See <a href="../docs/bfs.md">docs/bfs.md</a>.</p>
  <p><strong>Total publications in database:</strong> {total_publications} &nbsp; <strong>Peak depth d*:</strong> {peak_depth} (layer size {peak_L})</p>
  <div class="mb-4" style="max-width: 800px; height: 300px;">
    <canvas id="layerChart"></canvas>
  </div>
  <table class="table table-striped">
  <thead><tr><th>Depth d</th><th>A(d)</th><th>L(d)</th><th>L'(d)</th><th>r(d)</th></tr></thead>
  <tbody>
{table_body}
  </tbody>
  </table>
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.0-beta3/dist/js/bootstrap.bundle.min.js" crossorigin="anonymous"></script>
<script>
  (function() {{
    var depths = {depths_js};
    var L = {L_js};
    var A = {A_js};
    var LPrime = {L_prime_js};
    var r = {r_js};
    var layer6Progress = {layer6_progress_js};
    if (depths.length > 0) {{
      var datasets = [
        {{ label: 'L(d)', data: L, backgroundColor: 'rgba(54, 162, 235, 0.6)', borderColor: 'rgb(54, 162, 235)', borderWidth: 1, yAxisID: 'y' }},
        {{ label: 'A(d)', data: A, type: 'line', borderColor: 'rgb(135, 206, 250)', backgroundColor: 'rgba(135, 206, 250, 0.2)', fill: false, tension: 0.2, yAxisID: 'y' }},
        {{ label: "L'(d)", data: LPrime, type: 'line', borderColor: 'rgb(65, 105, 225)', backgroundColor: 'rgba(65, 105, 225, 0.2)', fill: false, tension: 0.2, yAxisID: 'y' }},
        {{ label: 'r(d)', data: r, type: 'line', borderColor: 'rgb(255, 99, 132)', backgroundColor: 'rgba(255, 99, 132, 0.2)', fill: false, tension: 0.2, yAxisID: 'y1' }}
      ];
      var pluginsConfig = {{
        tooltip: {{ mode: 'index', intersect: false }}
      }};
      // Add horizontal line annotation for current working layer progress (uses L(d) scale)
      if ({current_working_index if current_working_index is not None else 'null'} !== null) {{
        pluginsConfig.annotation = {{
          annotations: {{
            layerProgressLine: {{
              type: 'line',
              xScaleID: 'x',
              yScaleID: 'y',
              xMin: {current_working_index} - 0.4,
              xMax: {current_working_index} + 0.4,
              yMin: {processed_count},
              yMax: {processed_count},
              borderColor: 'rgb(0, 0, 0)',
              borderWidth: 2,
              label: {{
                display: false
              }}
            }}
          }}
        }};
      }}
      var chart = new Chart(document.getElementById('layerChart'), {{
        type: 'bar',
        data: {{
          labels: depths,
          datasets: datasets
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          scales: {{
            y: {{ beginAtZero: false, title: {{ display: true, text: "L(d), A(d), L'(d)" }} }},
            y1: {{ position: 'right', grid: {{ drawOnChartArea: false }}, title: {{ display: true, text: 'r(d)' }} }}
          }},
          plugins: pluginsConfig
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

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote {out_path}")
    print(f"  Total publications: {total_publications}, peak depth: {peak_depth}, peak L: {peak_L}")


if __name__ == "__main__":
    main()
