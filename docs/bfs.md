# BFS and depth

The crawler explores the recommendation graph with **breadth-first search**. This doc explains what depth means, how we avoid loops, and how to measure convergence.

## What does the queue contain?

The **queue** holds every domain that has ever entered the BFS frontier: one row per domain, with `status` in `pending`, `crawled`, or `failed`. So:

- **Yes:** the queue has a row for everything we have ever considered (seeds + every recommended domain we discovered). Crawled domains are in the queue with `status='crawled'`; the **publications** table has the actual data for those we successfully fetched.
- The queue is the full "discovery history": every node that was ever enqueued, with its BFS depth and current status.

## What does depth mean?

The crawler does **BFS on the recommendation graph** (conceptual graph: nodes = publications, edges = "A recommends B"). Depth is the **BFS depth** in that graph: **Depth 0** = seeds; **Depth 1** = recommended by some seed; **Depth 2** = recommended by something at depth 1; etc. No tree is stored—only the queue and the recommendations table—but depth is the distance from the seeds in this BFS. Seeds are multiple (depth 0); when we process a domain at depth D we enqueue its recommendations with depth D+1. We use `INSERT OR IGNORE` on the queue (domain is primary key), so the **first** path that discovers a domain sets its depth.

## People vs publications

On Substack’s UI you see “follow n people” and “subscribe for free to m publications.” The crawler only follows **publication** recommendations. In the **substack_api** library, `get_recommendations()` calls `/api/v1/recommendations/from/{publication_id}` and only uses items that have a **`recommendedPublication`** object (with `subdomain` or `custom_domain`). Any other shape in the API response (e.g. recommended people) is ignored. So the crawler only ever enqueues publication URLs, not people. Failed domains are publication URLs that failed to fetch (timeout, API error, or `get_publication_info()` returned nothing), not “person” URLs.

## Does BFS avoid loops?

Yes. Each domain appears **at most once** in the queue. When we see a recommendation for a domain that's already in the queue (pending, crawled, or failed), we don't add it again. We only ever crawl domains with status `pending`, then set them to `crawled` or `failed`, so we never crawl the same node twice. Cycles in the recommendation graph (e.g. A recommends B, B recommends A) don't cause infinite loops.

---

## Layers and convergence

### Layer size

The **layer** at depth *d* is the set of nodes that were first discovered at that depth (i.e. that have `depth = d` in the queue). The **layer size** is:

**L(d) = number of nodes in the queue with depth = d** (any status).

In SQL:

```sql
SELECT depth, COUNT(*) AS layer_size
FROM queue
GROUP BY depth
ORDER BY depth;
```

Early in the crawl, layers typically **grow** (expansion): depth 1 has more nodes than depth 0, depth 2 more than depth 1, etc. As the graph gets covered, many recommendations point to domains already in the queue, so fewer *new* domains are added at the next depth. At some point **layers get shorter again** (contraction): L(d) peaks and then decreases. That shrinking is a sign of **convergence**—we are mostly re-discovering already-known nodes.

### Convergence / divergence formulas

1. **Layer size L(d)**  
   - **Expansion:** L(d+1) > L(d) → still discovering a lot of new territory.  
   - **Convergence:** L(d+1) < L(d) → frontier is shrinking; we're past the "bulge."

2. **Peak depth d\***  
   - **d\* = argmax over d of L(d)**  
   - Before d\* the crawl is expanding; after d\* it is contracting. So d\* is the "front" of the wave.

3. **Layer ratio (growth factor)**  
   - **r(d) = L(d) / L(d−1)** (treat L(0) as 1 or the actual seed count).  
   - **r(d) > 1** → divergence (layer growing).  
   - **r(d) < 1** → convergence at that depth (layer shrinking).  
   - The first depth where r(d) < 1 after the peak is where contraction starts.

4. **Accumulation A(d) and change in size L'(d)**  
   - **A(d) = L(0) + L(1) + … + L(d)** (cumulative sum; total nodes discovered up to and including depth d). L(d) is the first difference of A: **L(d) = A(d) − A(d−1)**.  
   - **L'(d) = L(d) − L(d−1)** (discrete first difference of L; L'(0) undefined). L' &lt; 0 means layer shrinking.

**Why L, A, L', and r?**  
L(d) is the raw layer size. **A(d)** is the accumulation (total discovered up to depth d); L is the “derivative” of A. **r(d)** gives "expansion vs contraction" as a single threshold: **r > 1** = growing, **r < 1** = shrinking; r is dimensionless and comparable across depths. **L'(d)** gives the rate of change of L. We display L, A, L', and r on the layer stats page. In the chart, L'(d) and r(d) are shown for all depths except the last (the layer currently being built).

5. **Saturation (per-crawl measure)**  
   - When we crawl a node, we add some number of *new* domains to the queue (INSERT OR IGNORE skips already-present domains).  
   - **Saturation = 1 − (new domains added this crawl) / (recommendations seen this crawl)**.  
   - High saturation (e.g. > 0.9) means most recommendations were already in the queue → we are close to having explored the reachable graph.

You can compute L(d), A(d), L'(d), and r(d) from the queue after a run (or periodically) to see how layer sizes evolve and where convergence kicks in. The layer stats page in the UI shows these.
