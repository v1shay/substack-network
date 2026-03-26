# Tech-debt agent — background research

Background on software quality and technical debt measurements, with an eye on building a monitoring (and later suggestion) agent. Inline links point to Google Scholar queries to find papers that inform design. For design decisions see **tech-debt-agent-design.md**; for agent rules see **.cursor/rules/tech-debt-agent.mdc**.

## Why these links

Each subsection below includes a **Google Scholar query** link. Use them to find recent and classic papers on that topic. The agent design should be informed by this body of work rather than ad-hoc metrics.

## Technical debt: definition and measurement

- **Concept:** Technical debt is the implied cost of rework caused by choosing a quick or suboptimal solution now. See [technical debt software engineering](https://scholar.google.com/scholar?q=technical+debt+software+engineering).
- **Measurement:** Researchers have proposed ways to estimate debt (e.g. principal vs interest, time-to-fix). See [technical debt measurement](https://scholar.google.com/scholar?q=technical+debt+measurement+software) and [technical debt principal interest](https://scholar.google.com/scholar?q=technical+debt+principal+interest).
- **Relevance for agent:** We need measurable proxies (complexity, duplication, TODO density, dependency age) that correlate with maintainability and rework; the agent will use these rather than a single "debt in hours" number unless we adopt a defined method.

## Software quality and maintainability

- **Quality models:** ISO 25010 and others define quality attributes (maintainability, reliability, etc.). See [software quality metrics maintainability](https://scholar.google.com/scholar?q=software+quality+metrics+maintainability).
- **Code-level metrics:** Size (LOC), [cyclomatic complexity](https://scholar.google.com/scholar?q=cyclomatic+complexity+maintainability), coupling and cohesion, and composite indices (e.g. Maintainability Index) are widely used. See also [static analysis code quality](https://scholar.google.com/scholar?q=static+analysis+code+quality).
- **Relevance for agent:** We can apply size and complexity with standard tools; composite indices are optional and should be chosen with a clear definition.

## Code smells and refactoring

- **Code smells:** Symptoms that may indicate design or maintainability problems (long methods, large classes, duplication). See [code smell detection refactoring](https://scholar.google.com/scholar?q=code+smell+detection+refactoring).
- **Refactoring impact:** Empirical work on which refactorings improve quality and which metrics respond. See [refactoring impact maintainability](https://scholar.google.com/scholar?q=refactoring+impact+maintainability).
- **Relevance for agent:** Smell-like signals (long functions, duplicated blocks, TODO density) are good candidates for monitoring; suggestion logic can later map from these to refactoring ideas.

## Duplication and clones

- **Clone detection:** Code clones (exact, near-miss, semantic) and their relation to defects and maintenance. See [code clone detection impact](https://scholar.google.com/scholar?q=code+clone+detection+impact).
- **Relevance for agent:** Even simple clone detection (e.g. duplicate blocks) gives a useful debt indicator; we can add tooling when we implement.

## Dependencies and security

- **Outdated dependencies:** Old packages as a form of debt (security, compatibility). See [dependency technical debt](https://scholar.google.com/scholar?q=dependency+technical+debt+software).
- **Relevance for agent:** Pip/npm dependency checks fit the "debt and interest" view and are easy to automate; we can include them in the measurement set.

## Conceptual integrity and design consistency

- **Fred Brooks (conceptual integrity):** In *The Design of Design* and *The Mythical Man-Month*, Brooks argues that conceptual integrity—a design that hangs together as one coherent idea—is more important than flexibility or feature count. A system that does one thing well with one mental model is easier to understand, change, and keep consistent. See [conceptual integrity software design](https://scholar.google.com/scholar?q=conceptual+integrity+software+design).
- **SWE methods to maintain or increase conceptual integrity:** (1) Reuse one pattern for one concern (e.g. one way to "go back" in the UI). (2) Document the pattern so new code follows it. (3) Refactor special cases so the same problem is solved in one way. (4) Design rules and lint that enforce "do X the same way as in Y" or flag inconsistent naming/structure. (5) Code reviews and checklists that ask "does this preserve our stated patterns?" See [design consistency software engineering](https://scholar.google.com/scholar?q=design+consistency+software+engineering) and [architectural consistency refactoring](https://scholar.google.com/scholar?q=architectural+consistency+refactoring).
- **Relevance for agent:** When suggesting changes, prefer options that preserve or restore a single pattern; flag violations of documented conventions (e.g. a new list page that does not use the back-to-graph postMessage pattern).

## Summary for the agent

- Prefer **evidence-based metrics** (complexity, size, duplication, TODOs, dependencies) with clear definitions.
- Use **Scholar queries** above to check and update our choices as we implement.
- Keep **suggestions** tied to specific measurements and, where possible, to findings from the literature (e.g. "high cyclomatic complexity → consider extracting functions").
- Favor **conceptual integrity**: one pattern per concern, document patterns, and prefer refactors that remove special cases or duplicate mechanisms (see Conceptual integrity section above).

**Related files:** **tech-debt-agent-design.md** (design), **.cursor/rules/tech-debt-agent.mdc** (rules).
