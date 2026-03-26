# Visualization (interactive HTML graph)

The project builds an **interactive HTML graph** of the recommendation network using **pyvis** (which uses vis.js under the hood). Node size = PageRank, hover shows name/domain/rank, and **clicking a node opens the publication’s Substack archive (post list) in a new tab** so you bypass the subscribe page. This doc explains how that “click to open URL” behavior is implemented.

## What the visualization is

- **Script:** `scripts/milestone01/visualize.py`
- **Input:** `cartographer.db` (publications + recommendations) at repo root. Same graph and PageRank as `scripts/milestone01/centrality.py`.
- **Output:** A single HTML file (default: `data/substack_graph.html`) that you open in a browser. You can zoom, pan, hover for details, and **click a node to open that publication’s archive** (e.g. `https://name.substack.com/archive` or `https://custom.domain/archive`) in a new tab.
- **Scope:** Only the top N nodes by PageRank (default 200, max 500) so the graph stays readable.
- **Layout algorithm:** vis.js **Barnes–Hut** force-directed layout with physics (gravity, central gravity, spring length/strength). Nodes repel and edges act as springs until the layout stabilizes. **Proximity has semantics:** nodes that end up close are linked in the **recommendation graph** (one recommends the other, or they share many common recommenders). Layout reflects “who recommends whom,” not content or topic similarity.
- **Stopping node movement:** Nodes keep moving when you first open the graph. **Press Shift** to freeze them (easier to click); press Shift again to unfreeze.
- **Hover over** a node for details; **click** to open the publication’s archive (list of posts) in a new tab.

## Why you can see an isolated component (e.g. two nodes)

The crawler does BFS from seeds, so the full recommendation graph (all edges in the DB) is one connected component. The visualization, however, shows only the **top N nodes by PageRank** and only **edges between those nodes**. So we draw the *induced subgraph* on the top N.

If two nodes A and B have high PageRank (enough to be in the top N) and they recommend each other, but every other node that links to or from A or B has *lower* PageRank and is not in the top N, then in the subgraph A and B are connected only to each other. They appear as an isolated pair even though in the full graph they are connected to the rest via nodes that were dropped. So isolated components in the view are an artifact of the top‑N cut, not of BFS.

## Click to open URL: limitation and workaround

### Why we need a workaround

**Pyvis does not support “open URL when a node is clicked.”** It generates HTML and JavaScript that draw the network (vis.js), but it does not expose a way to attach a custom action (e.g. open a link) to node clicks. The library’s node options include things like `label`, `title`, `value`, `color`, but there is no built-in `url` or `link` that the graph uses on click.

So we cannot configure “click → open URL” purely through pyvis API calls. We have to change the generated page after pyvis writes it.

### What the script does (in detail)

1. **Build the graph and URLs in Python**  
   The script already has:
   - The set of node IDs (domains) in the visualized subgraph.
   - A function `domain_to_url_for_click(domain)` that turns each domain into the **archive** URL (e.g. `heuristic` → `https://heuristic.substack.com/archive`, `www.example.com` → `https://www.example.com/archive`) so the post list opens instead of the subscribe page.

2. **Build a node‑id → URL map**  
   For every node in the subgraph, we compute its URL and store it in a Python dict:  
   `node_to_url = { node_id: url, ... }`  
   The node IDs are the same strings (domains) that pyvis uses as node IDs in the generated JavaScript.

3. **Let pyvis write the HTML**  
   We call `net.save_graph(str(out_path))` as usual. That produces an HTML file that:
   - Loads vis.js and defines the network data (nodes and edges).
   - Creates the network with `network = new vis.Network(container, data, options);`
   - Registers other handlers (e.g. for stabilization progress).

   At this point the file has **no** click‑to‑open‑URL behavior.

4. **Re-open the HTML and inject our JavaScript**  
   We read the saved HTML file back in, then do a **string replacement** to inject two things **immediately after** the line that creates the network:
   - **A global map from node ID to URL:**  
     `var nodeIdToUrl = { ... };`  
     The `{ ... }` is the JSON-serialized `node_to_url` dict (so node IDs and URLs are properly escaped for embedding in JavaScript).
   - **A click handler on the network:**  
     `network.on("click", function(params) { ... });`  
     In that handler we check that exactly one node was clicked (`params.nodes.length === 1`), read `params.nodes[0]` (the node ID), look up the URL in `nodeIdToUrl`, and if present call `window.open(url, '_blank')` to open the publication’s archive in a new tab.

5. **Write the modified HTML back to the same path**  
   So the file that you open in the browser is the **post-processed** one, with the map and the click handler included.

## How to regenerate

From the repo root (with `cartographer.db` in the current directory):

```bash
python scripts/milestone01/visualize.py
# Optional: -n 200 (nodes), -o data/substack_graph.html, --db path/to/cartographer.db
```

Then open `data/substack_graph.html` in a browser. Click a node to open that publication’s archive in a new tab.

## Publishing the graph (e.g. to Git / GitHub Pages)

- **Can you commit the HTML to git?** Yes. The repo ignores `data/` by default; an exception `!data/substack_graph.html` in `.gitignore` allows that one file to be committed so the rest of `data/` (CSVs, DB) stays ignored.
- **Size:** The file is typically a few hundred KB (e.g. ~190 KB for 200 nodes). That’s fine for git (GitHub warns at 50 MB+).
- **Will it run in the browser when someone opens it?** Yes. The HTML loads vis-network and Bootstrap from CDNs, so it runs entirely in the browser. If someone clones the repo and opens `data/substack_graph.html` in a browser (file or via a local server), or you serve it via **GitHub Pages**, the graph will load and be interactive. On GitHub, “viewing” the file in the repo shows source code, not the live graph; to see the graph they open the file (e.g. raw URL in a new tab, or the GitHub Pages URL if you use that).
- **GitHub Pages:** To share a link that opens the graph, enable GitHub Pages for the repo and point it at the branch that contains `data/substack_graph.html` (or copy the file to the Pages root as `index.html`). The CDN dependencies will load and the graph will run.

## Index wrapper and list panel (back to graph)

The entry point is `index.html` (generated by `scripts/milestone02/add_publication_lists.py`): it embeds the graph in an iframe and shows links to list pages (Publications in graph, Publications in database, Layer stats, Failed publications). Clicking a list link loads that page in a second iframe (the list panel) and hides the graph panel.

All list-type pages (graph list, db list, layer stats, failed publications, investigation report) use the same “back to graph” behavior for conceptual consistency:

- Each page has a “← Back to graph” link with `id="back-to-graph"`.
- When the page is shown inside the list iframe, a script detects `window.parent !== window`, intercepts the link click, and sends `parent.postMessage('showGraph', '*')` instead of navigating. The index page listens for that message and switches back to the graph panel (hides list iframe, shows graph iframe). The graph iframe was never navigated away, so the graph does not reload.
- List and report pages must open in the list iframe (not in a new tab) so that this behavior applies; avoid `target="_blank"` on links from one list page to another (e.g. from Failed publications to Investigation report).

When adding a new list-like or report page, reuse this pattern so “back to graph” works the same everywhere and the graph is not reloaded.
