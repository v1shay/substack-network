# Tech-debt agent — design decisions

Design for an agent that monitors technical debt and will eventually suggest reductions. No implementation yet. For background research see **tech-debt-agent-research.md**; for agent rules see **.cursor/rules/tech-debt-agent.mdc**.

## Directory and naming

- **Directory:** `software-quality/`  
  Common industry terms include "code quality", "software quality", "technical debt". We use `software-quality` to cover both quality metrics and debt-oriented monitoring. Alternatives considered: `software_measurements` (narrow), `tech-debt` (debt-only). This directory holds design, research, and (later) agent outputs and config.

- **Artifact naming:** All three core artifacts share the prefix `tech-debt-agent` so they are clearly related:
  - `.cursor/rules/tech-debt-agent.mdc` — agent rules
  - `software-quality/tech-debt-agent-design.md` — this file
  - `software-quality/tech-debt-agent-research.md` — background research

## Agent role and phases

1. **Phase 1 (current):** Monitor and measure. Produce reports/snapshots of selected quality and debt indicators. No automated suggestions.
2. **Phase 2 (later):** Suggest. Use the same measurements to propose concrete refactors or cleanup; suggestions must reference the motivating metric or finding.

The agent does not change code by itself; it informs human decisions.

## Conceptual integrity (Fred Brooks)

We treat **conceptual integrity** (Fred Brooks, *The Design of Design*) as a design goal: the system should reflect one coherent mental model and use the same mechanisms for the same kind of problem, rather than ad-hoc or one-off solutions. That reduces cognitive load and keeps technical debt lower. The agent should favor suggestions that preserve or restore conceptual integrity (e.g. reuse an existing pattern instead of introducing a new one). When measuring or suggesting, ask whether a change would add a special case or duplicate a pattern; if so, prefer the option that keeps the design consistent.

## Measurements we can apply (preliminary)

Guided by [software quality metrics](https://scholar.google.com/scholar?q=software+quality+metrics+maintainability) and [technical debt measurement](https://scholar.google.com/scholar?q=technical+debt+measurement+software) literature:

| Category | Metric / signal | Notes |
|----------|-----------------|--------|
| **Size & structure** | File length (LOC), function length | Simple; long files/functions often correlate with maintainability issues. |
| **Complexity** | Cyclomatic complexity (or similar) | Per function; high values suggest refactor candidates. See [cyclomatic complexity maintainability](https://scholar.google.com/scholar?q=cyclomatic+complexity+maintainability). |
| **Duplication** | Code clones (same or similar blocks) | Tool-dependent; even simple duplicate-block detection helps. |
| **Debt markers** | TODO / FIXME / XXX density | Raw count or per-file; proxy for deferred work. |
| **Dependencies** | Outdated or vulnerable deps | e.g. pip/requirements, npm if used; supports [technical debt principal and interest](https://scholar.google.com/scholar?q=technical+debt+principal+interest) view. |
| **Consistency** | Style drift, naming | Linters and formatters; can track rule violations over time. |
| **Dead code** | Unused exports, unreachable code | Static analysis; reduces noise and confusion. |

We do not adopt a single "technical debt in hours" number without defining how it is derived; any such metric will be documented and justified in this doc or the research note.

## Tooling and automation (design only)

- Prefer existing tools (linters, formatters, static analyzers, clone detectors) over custom metrics where they give comparable signal.
- Outputs (reports, dashboards) should live under `software-quality/` (e.g. `software-quality/reports/`) so they are easy to find and version.
- No code in this repo yet; tool choices and integration will be decided when we implement.

## References

- **tech-debt-agent-research.md** — literature and metrics background
- **.cursor/rules/tech-debt-agent.mdc** — rules for the agent
