
### **The Substack Archipelago: Translation Matrix**

Use metaphors sparingly in documentation. In particular, as we are also developing the substack api, it is important to use the substack api terminology throughout.

| **Project Metaphor** | **User Vernacular** | **Substack API** | **Standard Engineering** | **Research Definition** |
| :--- | :--- | :--- | :--- | :--- |
| **Archipelago** | "Substack" | `substack.com` | `network_graph` | The total observed network. |
| **Island** | Newsletter / Blog | `publication` | **`publications`** | The distinct website/node (e.g., *Racket News*). |
| **Governor** | Author / Creator | `user` (admin) | **`authors`** | The person who writes/owns the publication. |
| **Voyager** | Subscriber / Reader | `user` (subscriber) | **`subscribers`** | An entity that consumes content and creates traffic. |
| **Trade Route** | "Recommended" / "Reads" | `recommendation` | **`recommendations`** | A directed edge ($A \rightarrow B$) indicating endorsement. |
| **Artifact** | Post / Article | `post` | **`posts`** | A single piece of content (URL). |
| **Fossil** | HTML / Source | `body_html` | **`raw_html`** | The unprocessed HTML fragment (preserves structure). |
| **Bone** | The Text | `body_text` | **`clean_text`** | The stripped string ready for LLM analysis. |
| **Surveying** | Browsing | `crawling` | **`crawler`** | The script that maps the network (metadata only). |
| **Excavating** | Archiving | `scraping` | **`scraper`** | The script that extracts content (HTML/Text). |
| **Survey Log** | N/A | N/A | **`queue`** | The database table managing the crawl state. |

## The Expanded Ontology

(quick and dirty suggestion, needs discussion)

| **Project Metaphor** | **User Vernacular** | **Substack API** | **Standard Engineering** | **Research Definition** |
| :--- | :--- | :--- | :--- | :--- |
| ... | ... | ... | ... | ... |
| **Shard** | Note | `note` | **`notes`** | Short-form content (Twitter-style). Distinct from Artifacts. |
| **Echo** | Comment | `comment` | **`comments`** | User feedback attached to an Artifact or Shard. |
| **Bridge** | Internal Link | `href` (internal) | **`internal_edges`** | A hyperlink within the text pointing to another Substack Island/Artifact. |
| **Horizon** | External Link | `href` (external) | **`external_domains`** | A hyperlink pointing outside the Archipelago (e.g., NYT, Wikipedia). |
| **Manifest** | Metadata | `meta` / `json` | **`metadata`** | Tags, timestamps, and author bylines attached to an Artifact. |


### Why these additions matter (and how to handle the complexity)

Here is how you keep the engineering simple while adding these features:

#### 1. The Shard (Notes)
Substack Notes are essentially "Tweets." They are valuable because they represent the **real-time pulse** of the Archipelago, whereas Artifacts (Posts) are the **historical record**.
*   **Engineering Strategy:** Store them in a separate table (`notes`) linked to the `author_id`. Do not try to scrape full HTML for these; usually, the text content is enough.

#### 2. The Bridge (Internal Links)
*   **Why add it?** "Trade Routes" (Recommendations) show who the author *endorses*. "Bridges" (Internal Links) show who the author is *actually talking about* in their text. This reveals hidden clusters of influence.
*   **Complexity Check:** This is the most computationally expensive addition.
*   **Engineering Strategy:** When `Excavating` (scraping) an Artifact, run a regex to extract all `substack.com` links. Store them in a simple list or a lightweight mapping table.

#### 3. The Horizon (External Links)
*   **Why add it?** This defines the "political leanings" or "source material" of an Island. An Island linking to *Fox News* is distinct from one linking to *NPR*.
*   **Engineering Strategy:** You don't need the full URL. Just extract the **domain** (e.g., `nytimes.com`). Store these as a frequency count (e.g., `{"nytimes.com": 5, "wikipedia.org": 2}`).
