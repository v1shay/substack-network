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

## 🎯 Quick Start

### Current Status

✅ **Working:** `scripts/milestone01/crawl.py` - Network crawler using the `substack_api` library (BFS, queue, seeds file, cartographer.db at repo root)
❌ **Not Started:** Content scraping, analysis pipeline

### Next Steps

1. **Set up your forked API repository** (see [multiple-repo-workflow.md](./multiple-repo-workflow.md))
2. **Expand database schema** (add posts, notes, links tables)
3. **Implement content scraping** (see [development-plan.md](./development-plan.md))

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
