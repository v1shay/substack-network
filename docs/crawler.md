# Crawler and Substack API

How the crawler fetches publications and recommendations, and how the Substack API behaves.

## API endpoints (same host as the publication)

For a publication like `https://paulkrugman.substack.com`:

| Endpoint | Auth required? | Crawler use |
|----------|-----------------|-------------|
| `GET /api/v1/publication` | Yes (returns **403** with only User-Agent) | `get_publication_info()` tries this first; usually fails. |
| `GET /api/v1/archive?sort=new&offset=0&limit=1` | No (**200**) | Fallback: get first post → `publication_id` and metadata. |
| `GET /api/v1/recommendations/from/{publication_id}` | No (**200**) | Get list of recommended publications. |

So the crawler can read recommendations because:

1. **Publication info:** `/api/v1/publication` often returns 403. The crawler then uses the **fallback**: `newsletter.get_posts(limit=1)` → **`/api/v1/archive`** (200). From the first post it gets `publication_id` and can build name/description.
2. **Recommendations:** `get_recommendations()` needs `publication_id` (from search or from that same post). It then calls **`/api/v1/recommendations/from/{publication_id}`** → 200 with only a User-Agent.

The crawler never needs a successful `/api/v1/publication` call; it uses **archive** and **recommendations/from/{id}**.

## People vs publications

On Substack's UI you see "follow n people" and "subscribe for free to m publications." **We don't know how the UI determines this split** — it may use different logic, different API endpoints, or different data than what's available in the public API.

**What we know:**
- The crawler uses `substack_api`'s `get_recommendations()`, which calls `/api/v1/recommendations/from/{publication_id}`.
- This endpoint returns a single list of items, each with a `recommendedPublication` object (with `subdomain` or `custom_domain`).
- The crawler enqueues **all** recommendations that have a `recommendedPublication` object, regardless of any other fields.
- Each `recommendedPublication` has an `is_personal_mode` field (boolean), but **we don't know if this maps to the UI's "people" vs "publications" distinction**.

**What we infer (but don't know for certain):**
- The script `get_recommendations.py` classifies items by `is_personal_mode` (true → person, false → publication) as a heuristic, but this may not match the UI.
- On some sites the API returns *all* items with `is_personal_mode: false`, yet the UI shows different counts. For example:
  - **oldster.substack.com**: UI shows "follow 50 people" and "subscribe to 3 publications", but API returns 50 items all with `is_personal_mode: false`.
  - **exponentialview.co**: UI shows "follow 50 people" and "subscribe to 3 publications", but API returns all recommendations with `is_personal_mode: false`.
  
  This suggests either (1) the UI uses different logic/endpoints, or (2) `is_personal_mode` doesn't correspond to the UI's "people" vs "publications" split.

**Reproduce from the repo root:**

```bash
# Default output: kind (person|publication from is_personal_mode) and URL
python scripts/get_recommendations.py oldster.substack.com
python scripts/get_recommendations.py exponentialview.co

# See the split as two sections (Publications vs People)
python scripts/get_recommendations.py oldster.substack.com --separate
python scripts/get_recommendations.py exponentialview.co --separate

# Raw API response to inspect is_personal_mode per item
python scripts/get_recommendations.py oldster.substack.com --json
python scripts/get_recommendations.py exponentialview.co --json
```

Then compare with the UI: open each site’s recommendations (e.g. “Recommendations” or “Who to follow”) and note “follow n people” vs “subscribe to m publications” — the API may return all items with `is_personal_mode: false` while the UI shows 50 people and 3 publications.

**Workaround:** Use **`--as-people`** in `get_recommendations.py` to treat every item as "person" if you want counts to match the UI's "follow n people" number. See [bfs.md](bfs.md#people-vs-publications).

### The mismatch

Subscribing to Paul Krugman, we see

![](images/2026-02-18-21-03-12.png)

but with the api we get

```
python scripts/get_recommendations.py paulkrugman.substack.com

https://eduardoelreportero.substack.com
https://hboushey.substack.com
https://econjared.substack.com
https://contrarian.substack.com
https://karenattiah.substack.com
https://www.derekthompson.org
https://writing.yaschamounk.com
https://www.the-downballot.com
https://www.programmablemutter.com
https://anntelnaes.substack.com
https://www.hopiumchronicles.com
https://margaretsullivan.substack.com
https://phillipspobrien.substack.com
https://www.briefingbook.info
https://calculatedrisk.substack.com
https://www.apricitas.io
https://arimelber.substack.com
https://stayathomemacro.com
https://gooznews.substack.com
https://braddelong.substack.com
https://www.noahpinion.blog
https://heathercoxrichardson.substack.com
https://www.gelliottmorris.com
```

### Why the mismatch?

The discrepancy between UI counts and API data suggests several possibilities:

1. **Different data sources**: The UI might call a different endpoint (e.g., authenticated or user-specific) that returns different categorization than the public `/api/v1/recommendations/from/{id}` endpoint.

2. **Client-side logic**: The UI might apply additional filtering or categorization logic that isn't reflected in the raw API response. For example, it might use other fields (like `author` information, publication metadata, or user interaction data) to determine "people" vs "publications".

3. **`is_personal_mode` meaning**: The `is_personal_mode` field might indicate something other than "person vs publication" — perhaps it indicates whether a publication is in "personal mode" (single-author focused) vs "publication mode" (multi-author or editorial), which doesn't directly map to "follow people" vs "subscribe to publications".

4. **Separate recommendation lists**: The UI might combine data from multiple sources:
   - The 50 items from `/api/v1/recommendations/from/{id}` might be the "follow people" list (all treated as people regardless of `is_personal_mode`).
   - The "3 publications" might come from a different source (e.g., curated recommendations, trending publications, or a separate API endpoint).

5. **Caching or stale data**: The UI might be showing cached or pre-computed counts that don't match the current API response.

Without access to Substack's frontend code or authenticated API endpoints, we can't definitively determine which explanation is correct. The evidence suggests that **`is_personal_mode` is not a reliable indicator** of how the UI categorizes recommendations.

## Get recommendations from the terminal

Use the script (run from repo root):

```bash
python scripts/get_recommendations.py paulkrugman.substack.com
```

Or with a full URL:

```bash
python scripts/get_recommendations.py https://paulkrugman.substack.com
```

**Separating people vs publications:** The API returns items with `recommendedPublication`; each has **`is_personal_mode`** (boolean). The script uses this as a heuristic (true → person, false → publication), but **this may not match how the UI categorizes them**. Use:

- **Default:** `kind\turl` per line (kind = `person` or `publication`) — e.g. `... | grep '^person'` or `... | grep '^publication'`.
- **`--separate`:** Two sections: "Publications" then "People".
- **`--only-publications`** / **`--only-people`:** Print only those URLs (one per line).
- **`--as-people`:** Treat every item as "person" (so counts match the UI "follow n people" when the API returns all `is_personal_mode: false`).

Note: Some recommendation lists have all `is_personal_mode: false`; then "People" will be empty unless you use `--as-people`. The "subscribe to m publications" list shown on the UI may come from a different source than this endpoint.

### Manual curl (two steps)

**1. Get `publication_id` from the archive (first post):**

```bash
curl -s "https://paulkrugman.substack.com/api/v1/archive?sort=new&offset=0&limit=1" \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
```

In the JSON array, take the first object's **`publication_id`** (e.g. `277517`).

**2. Get recommendations:**

```bash
curl -s "https://paulkrugman.substack.com/api/v1/recommendations/from/277517" \
  -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
```

Replace the host and `277517` with your publication's host and `publication_id`.

## Why "failed" when the homepage is 200?

The **investigator** only GETs the **homepage**. The **crawler** needs **`/api/v1/publication`** (often 403) or the **archive + post metadata** to succeed. So the homepage can be 200 while the crawler still marks the domain failed (e.g. timeout, or archive/post failed). See the Failed publications and investigation report copy in the UI.
