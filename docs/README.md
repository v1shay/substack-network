# Documentation Index

## 📚 Documentation Files

1. **[project-ontology.md](./project-ontology.md)** - The complete mapping of Archipelago metaphors to technical terms
2. **[development-plan.md](./development-plan.md)** - Comprehensive development roadmap and next steps
3. **[multiple-repo-workflow.md](./multiple-repo-workflow.md)** - Quick reference for working with cartographer, substack_api, and pAIa
4. **[bfs.md](./bfs.md)** - How the crawler does BFS on the recommendation graph
5. **[database.md](./database.md)** - Schema, inspection, and DB location
6. **[data-analysis.md](./data-analysis.md)** - Analysing the recommendation graph (questions, centrality, scale)
7. **[visualization.md](./visualization.md)** - Interactive HTML graph (pyvis), including how “click to open URL” is implemented
8. **[comment-pipeline.md](./comment-pipeline.md)** - Standalone comment ingestion modules and test entry points
9. **[comment-integration-spec.md](./comment-integration-spec.md)** - Planned crawler integration contract (flags, fail-open behavior, stats logging)
10. **[comment-backfill.md](./comment-backfill.md)** - Durable, resumable backfill for comments on already-crawled publications
11. **[semantic-embeddings.md](./semantic-embeddings.md)** - Optional embedding batches for semantic sorting and future semantic graph artifacts

## 🎯 Quick Start

### Current Status

✅ **Working:** `scripts/milestone01/crawl.py` - Network crawler using the `substack_api` library (BFS, queue, seeds file, cartographer.db at repo root)
✅ **Working:** `scripts/comments/comment_backfill.py` - Controlled historical comment ingestion for publications already in the DB
⚠️ **Scaffolded:** `scripts/comments/semantic_embeddings.py` - Semantic embeddings, pending OpenAI config and downstream graph/clustering UI

### Next Steps

1. **Set up your forked API repository** (see [multiple-repo-workflow.md](./multiple-repo-workflow.md))
2. **Run a small comment backfill pilot** (see [comment-backfill.md](./comment-backfill.md))
3. **Configure semantic embeddings** (see [semantic-embeddings.md](./semantic-embeddings.md))

---
## 🗺️ Project Metaphors

The project uses an "Archipelago" metaphor:
- **Islands** = Publications (newsletters/blogs)
- **Governors** = Authors
- **Trade Routes** = Recommendations
- **Artifacts** = Posts
- **Fossils** = Raw HTML
- **Bones** = Clean text
- **Surveying** = Network mapping
- **Excavating** = Content scraping

See [project-ontology.md](./project-ontology.md) for complete mapping.
