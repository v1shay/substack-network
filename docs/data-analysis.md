# Analysing the recommendation graph

**Questions to ask of the current data** (publications + recommendations + queue):

- **Influence / in-degree:** Which publications are recommended most often? (Top “authorities”.)
- **Reciprocity:** How often do A and B recommend each other? (Mutual recommendations.)
- **Connectivity:** How many weakly connected components? Is there one giant component?
- **Recommendation balance:** Distribution of “how many publications does each publication recommend?” (out-degree distribution.)
- **Centrality:** Betweenness, PageRank, or other centralities once the graph is large enough to be interesting.

**How big a graph for centrality to be worthwhile?** A few hundred to low thousands of publications often suffice to see clear hubs vs authorities; tens of thousands give more stable rankings.
- **PageRank / in-degree:** Meaningful already at a few hundred nodes; runtimes scale with edges (often under a minute for ~10k nodes, ~100k edges on a laptop).
- **Betweenness:** Cost grows with nodes and edges (e.g. Brandes O(n·e)); exact betweenness is fine up to a few thousand nodes; beyond that use sampling or approximate algorithms.

**How to apply PageRank to the data**
1. **Graph:** Build a directed graph from the `recommendations` table: nodes = domains, edge (A, B) = “A recommends B.” Every row is one edge.
2. **PageRank:** Run PageRank on that graph (e.g. `networkx.pagerank(G)`). The score of a node = “importance” as “being recommended by other important nodes.”
3. **In-degree:** For each node, count incoming edges (how many publications recommend it). Highly correlated with PageRank; useful as a simple baseline.
4. **Join with publications:** Map domain → name (and optional description) from the `publications` table so the output is human-readable (e.g. “Top 50: name, domain, pagerank, in_degree”).
5. **Output:** A rankings table (and optionally CSV in `data/`) plus, later, an export for visualization (pyvis/HTML or matplotlib).

See `scripts/milestone01/centrality.py` for the implementation.

**Depth vs centrality: what does it mean when #1 is at depth 18?**

If the top-ranked node by PageRank (e.g. heathercoxrichardson) is at **depth 18** (18 steps from the seed in BFS), that tells us:

1. **Structure of the network:** The most recommended publications (highest in-degree / PageRank) are not necessarily "near" the seed. They can be many hops away. The graph has **late-discovered hubs**: nodes that the crawl only reaches after a long path, but that accumulate many incoming recommendations once they are in the graph.
2. **Interesting measure:** **Depth vs PageRank** (or depth vs in-degree). Plot or rank by "centrality at high depth": a node that is both far from the seed (high depth) and has high PageRank is a hub that the BFS had to "dig" to find. A simple derived measure: e.g. PageRank × (1 / (1 + depth)) to favor hubs that are closer to the seed, or "rank by PageRank among nodes at depth ≥ d" to see who dominates the "deep" part of the graph.
3. **Seed dependence:** **Yes.** Which nodes are in the graph and at what depth depends entirely on the **seed(s)**. Different seeds discover different regions of the recommendation network. A politics-oriented seed might reach heathercoxrichardson at depth 5; a tech-only seed might never reach her (different component) or reach her at depth 30. So "who is #1" is a property of the **induced subgraph** discovered by BFS from your seeds. Change seeds and you can get a different subgraph and a different top-ranked node. PageRank itself, once the graph is fixed, is a property of that graph; but the graph you have is seed-dependent.

So: a long "time to reach #1" (high depth for the top node) reflects that the crawl had to traverse many recommendation hops before including that hub, and that the network has **central nodes that are not close to the seed**. The ranking is still meaningful for the subgraph you discovered; it is not a global ranking of all of Substack.

Less interesting questions:
- **Depth distribution:** How many publications at depth 0, 1, 2, …? How far does the graph extend from the seeds?
- **Failure and coverage:** What fraction of the queue is `failed` vs `crawled`? Do custom domains fail more than `*.substack.com`?
- **Discovery by seed:** If we tagged which seed first led to each domain, which seeds discover the largest subgraph?
- **Descriptions:** Can we extract topics or keywords from publication descriptions to group or filter (e.g. “tech”, “politics”)?
