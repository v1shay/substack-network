# Semantic Embeddings

Semantic sorting is separate from the recommendation graph. The existing HTML graph uses recommendation edges; proximity in that graph means recommendation topology, not text similarity.

`scripts/comments/semantic_embeddings.py` adds an optional batch layer for embedding text from:

- `comments`
- `posts`
- `publications`

Embeddings are stored in:

- `semantic_embedding_runs` — one row per embedding batch.
- `semantic_embeddings` — one row per source row and model, with `source_hash`, model name, dimensions, and JSON-encoded vector.

## Setup

Install dependencies through the normal setup path:

```bash
source setup.sh
```

The embedding job requires the `openai` package and `OPENAI_API_KEY` only when making real API calls.

Check configuration without API calls:

```bash
.venv/bin/python scripts/comments/semantic_embeddings.py --check-config --source comments --limit 10
```

Preview candidates without writing or calling OpenAI:

```bash
.venv/bin/python scripts/comments/semantic_embeddings.py --dry-run --source comments --limit 100
```

Run a small batch:

```bash
.venv/bin/python scripts/comments/semantic_embeddings.py --source comments --limit 100
```

## Dedupe Behavior

The job computes a SHA-256 hash of normalized source text. For the same source row and model:

- unchanged text is skipped;
- changed text updates the existing embedding row;
- a different model creates a separate embedding row.

This keeps semantic batches resumable and makes model-version comparisons possible.

## Graph Boundary

Do not interpret the current `data/substack_graph.html` layout as semantic similarity. A future semantic graph should read from `semantic_embeddings` and build a separate nearest-neighbor or clustering artifact, so recommendation topology and text meaning remain auditable as different signals.
