# Proposed Indexes & Materialized Views for M3 Queries

> Planning document. **No indexes or SQL are created in this PR.** Application lands in a later PR once benchmarks (see §8) confirm which of these entries are worth keeping.

- Scope: PostgreSQL (Supabase) backing the NPM supply-chain risk scorer.
- Companion reading: [`docs/PLAN.md` §8](./PLAN.md), [`docs/api-spec.md`](./api-spec.md), `docs/queries/m3-queries.md` (M3 SQL — landing in a sibling PR; queries are also quoted in PLAN §8).

---

## 1. Overview

The CIS 5500 rubric requires **≥2 complex queries whose runtime drops from >15s pre-optimization to <1s post-optimization**, with a before/after timing table in the final report. PLAN §8 has already flagged **Q3, Q7, Q8, and Q10** as the four complex candidates (Q3 has been empirically observed on Supabase to exceed 15s at depth ≥ several). This document inventories every index and materialized view we think is worth trying, maps each one to the M3 queries it helps, and calls out a "top 3 + 1 MV" combination that should land at least the two rubric-qualifying speedups (Q3 + Q7, with Q8 and Q10 as backups). Indexes that look tempting but are not obviously worth the write-amplification / storage cost are explicitly flagged as such rather than rubber-stamped.

---

## 2. Current schema recap

Four base tables populated by `src/clean_data.py` and loaded into Supabase. ER diagram: [`docs/images/er-diagram.png`](./images/er-diagram.png).

- **`packages`** — `id` PK (UUIDv5), natural key `(ecosystem, name)` (unique), columns: `ecosystem`, `name`, `description`, `latest_version`.
- **`versions`** — `id` PK, `package_id` FK → `packages.id`, natural key `(ecosystem, package_name, version)` (unique), columns: `ecosystem`, `package_name`, `version`, `released`, `has_repository`, `github_owner`, `github_repo`.
- **`dependencies`** — `id` PK, `from_version_id` FK → `versions.id`, `to_package_id` FK → `packages.id`, columns: `ecosystem`, `from_package`, `from_version`, `to_package`, `version_spec`, `dep_kind`.
- **`maintainers`** — `id` PK, `package_id` FK → `packages.id`, columns: `ecosystem`, `package_name`, `username`, `name`, `role`, `email`.

Data-type note carried over from `src/clean_data.py`: `versions.released` and `versions.has_repository` are written to CSV as strings (`has_repository` is `'true'` / `'false'` / `''`). If the Supabase schema also stores them as `text`, a handful of optimizations below assume the Supabase table was created with `timestamptz` and `boolean` (or a one-time cast migration). That migration is out of scope for this PR but is a prerequisite for the partial index in row I5 and the `released DESC` sort in row I3.

---

## 3. Candidate indexes

Shorthand: "helps" means the query either joins/filters on the indexed column, groups by it, or sorts by it. Cardinality assumptions come from the top-N seed size we run `collect_data.py` with (expect ~10⁴–10⁵ packages, ~10⁵–10⁶ versions and dependencies, ~10⁵ maintainers after a full scrape).

| # | Table | Columns | Type | Motivated by queries | Rationale | Estimated impact |
|---|-------|---------|------|----------------------|-----------|------------------|
| I1 | `dependencies` | `from_version_id` | btree | Q2, Q3, Q5, Q7, Q8 | This is the primary edge-traversal axis: "given a version, enumerate its out-edges". Every recursive BFS step in Q3/Q8, the fan-out aggregate in Q2, and the dependents counts feeding Q5/Q7 all start here. Without it, every BFS hop is a seq scan over `dependencies`. | **High.** Largest single expected win for Q3/Q8; these are the rubric-qualifying complex queries. |
| I2 | `dependencies` | `to_package_id` | btree | Q3, Q5, Q7, R5 (A5) | Reverse traversal: "given a package, who depends on it?". Drives the Q5 dependents count (ecosystem blast-radius), the Q7 "abandoned popular" filter (needs dependents per candidate), the A5 dependents listing route, and the reverse edge of Q3 when the UI asks for inbound edges. | **High.** Required for Q5 to be ≪1s, and is a precondition for Q7 reaching the <1s target once the last-release filter is pushed down. |
| I3 | `versions` | `(package_id, released DESC)` | btree (composite) | Q4, Q7, Q10, A2 | "Give me the latest release date for this package" is the hot inner subquery for the staleness signal (S2). Sorting by `released DESC` means the MAX is an index-only scan of the first row per package. Also speeds up the `latest_versions` materialized view's `DISTINCT ON (package_id) ... ORDER BY released DESC`. | **High.** Cuts Q7 staleness filter from a full versions scan to an index range scan; also removes the per-package correlated subquery in Q10's staleness input. Requires `versions.released :: timestamptz` (see §2 data-type note). |
| I4 | `maintainers` | `package_id` | btree | Q4, Q6, Q10, A3 | Maintainer-count aggregate (S1 in the risk formula) groups by `package_id`. Q6 ("maintainers with most packages") groups by `username` — I4 does *not* help Q6's outer group-by but does help A3 and the `per-package maintainer_count` subquery that Q4 and Q10 share. | Medium-high. The table is small-ish (~10⁵ rows) so even a seq scan is tolerable today, but this index makes the per-package subquery constant-time, which matters when Q10 runs it for every package row. |
| I5 | `versions` | `package_id` **WHERE** `has_repository IS FALSE` (or `= 'false'` if still stored as text) | partial btree | Q9, A6 (S5 signal) | Q9 and the has-repo signal in Q10 only care about the "no repo" sliver. A partial index is ~O(rows with `has_repository = false`) in size — typically a minority of versions — and converts the filter from a full scan into a narrow range scan. | Medium. Worth it only if the "no repo" fraction is small (say <30% of versions). If most versions lack a repo, a plain index (or no index at all) is cheaper. Revisit after first scrape stats. |
| I6 | `packages` | `(ecosystem, name)` | btree (unique) | all routes accepting `ecosystem=` + name-based lookup, A1, A2 | Per PLAN §8 this is already the unique key for `packages`. The action item here is **confirm that Supabase materialized it as a btree index** (some managed Postgres UIs create a unique constraint without an explicit usable index on the columns). If absent, add it. | Low *new* impact (it already exists as a constraint); high correctness value for route plans that filter `WHERE ecosystem = 'npm'`. |
| I7 | `packages` | `name gin_trgm_ops` | GIN (pg_trgm) | A1 (`/api/packages/search`) | The search route uses substring/prefix matches on `name`. A btree on `(ecosystem, name)` only helps strict prefix (`name LIKE 'lod%'`) when the locale is C; it cannot accelerate substring search (`name ILIKE '%lodash%'`). A trigram GIN index does both. | Medium for A1 only. Not on the critical path for the rubric's complex-query targets. Requires `CREATE EXTENSION pg_trgm` on Supabase (commonly available). |
| I8 | `dependencies` | `(from_version_id, to_package_id)` | btree (composite) | Q3, Q8 | Supports the recursive CTE's `(from_version_id, to_package_id)` visited-set check cheaply (index-only scan for `SELECT 1 FROM dependencies WHERE from_version_id = ? AND to_package_id = ?`). Also helps `DISTINCT` de-duplication of the frontier. | Low-to-medium incremental once I1 exists. Consider only if I1 alone leaves Q3 above 1s post-`latest_versions` MV. Candidate to *defer*. |
| I9 | `dependencies` | `dep_kind` | btree | R3 (graph — optional `depKind` filter), A4 | `dep_kind` has very low cardinality (`dependency`, `peer`, `optional`, possibly empty). On a low-cardinality column, a plain btree is typically *worse* than a seq scan with filter. **Not recommended.** If the Graph Explorer ever defaults to filtering one kind, consider a *partial* index per kind instead. | Negative / skip. Called out so we don't accidentally add it. |
| I10 | `maintainers` | `username` | btree | Q6 | Q6 groups by `username`. Postgres' HashAggregate is already cheap here and Q6's output is `LIMIT 10` on a table of ~10⁵ rows, so a seq scan + HashAggregate is well under 1s without an index. | Low. Skip unless Q6 profiling shows >200ms; not a rubric-qualifying query. |
| I11 | `versions` | `package_id` | btree (plain) | Q1, A2 | Useful for R1 / Package Detail. Partially subsumed by I3's composite, which can also serve `WHERE package_id = ?` queries on the leading column. | Redundant with I3. Skip; listed so reviewers know we considered it. |

---

## 4. Proposed materialized views

### MV1. `latest_versions` — "latest version per package" (used by Q3, Q8, Q10, graph endpoints)

**Purpose.** Q3 (recursive BFS from the latest version of a package), Q8 (depth-bounded walk from the latest version), and the Q10 staleness signal all need to resolve `package → latest version → version_id`. Today this is typically done with `JOIN versions v ON v.package_id = p.id AND v.version = p.latest_version`, which forces a join on a text column (`version`) and cannot use I3. A materialized `DISTINCT ON` view precomputes the mapping and gives us a small table keyed by `package_id`.

**Proposed definition (pseudocode — not applied in this PR):**

```sql
-- PROPOSAL — do not apply in this PR.
CREATE MATERIALIZED VIEW latest_versions AS
SELECT DISTINCT ON (v.package_id)
       v.id          AS version_id,
       v.package_id,
       v.version,
       v.released,
       v.has_repository
FROM   versions v
ORDER  BY v.package_id, v.released DESC NULLS LAST, v.version DESC;

CREATE UNIQUE INDEX latest_versions_pkg_idx ON latest_versions (package_id);
CREATE INDEX        latest_versions_vid_idx ON latest_versions (version_id);
```

Notes on the definition:
- We sort by `released DESC NULLS LAST` first, then `version DESC` as a deterministic tiebreaker, rather than trusting `packages.latest_version` (which is a denormalized text column that can drift). This removes the `::text` join from Q3/Q8.
- The `UNIQUE INDEX ... (package_id)` both enforces the `DISTINCT ON` invariant and lets us use `REFRESH MATERIALIZED VIEW CONCURRENTLY`.
- If `versions.released` is still stored as `text`, the `ORDER BY` is a lexicographic string sort — correct for ISO-8601 strings but brittle. Prefer migrating the column to `timestamptz` before relying on this MV (same prerequisite as I3).

**Refresh strategy.** Scraper runs are batch, not streaming, so stale data for up to one scrape cycle is acceptable.
- After each `collect_data.py` load: `REFRESH MATERIALIZED VIEW CONCURRENTLY latest_versions;`
- In the absence of an orchestrator, a Supabase pg_cron entry refreshing nightly is acceptable for v1. Document in `docs/PLAN.md` §8 once we apply.
- **Not on write-through.** The scraper does not currently emit per-row change events, so incremental maintenance is out of scope.

**Queries it helps.** Q3, Q8, Q10 (staleness input), A6 (risk breakdown for one package), and implicitly every `/api/packages/:packageId/graph` and `/depth-below` call. Turns the "latest version for each package" step from an O(versions) scan with a text-column join into an O(packages) index lookup.

### MV2 (optional, lower priority). `package_dep_stats` — precomputed fan-in / fan-out per package

**Purpose.** Q2 (fan-out), Q5 (fan-in / dependents), and the S3/S4 inputs to Q10. Today these are computed with `GROUP BY` aggregates over `dependencies`.

**Sketch.**
```sql
-- PROPOSAL — consider only if I1 + I2 do not get Q5/Q10 under 1s.
CREATE MATERIALIZED VIEW package_dep_stats AS
SELECT p.id                               AS package_id,
       COALESCE(COUNT(d_out.id), 0)       AS direct_fanout,   -- from latest version
       COALESCE(COUNT(d_in.id),  0)       AS direct_fanin
FROM   packages p
LEFT JOIN latest_versions lv       ON lv.package_id = p.id
LEFT JOIN dependencies    d_out    ON d_out.from_version_id = lv.version_id
LEFT JOIN dependencies    d_in     ON d_in.to_package_id    = p.id
GROUP BY p.id;
```

**Refresh strategy.** Same cadence as MV1, refreshed *after* MV1 so it sees consistent "latest version" mappings.

**When to use it.** Only if post-index benchmarks still show Q5 or Q10 above the 1s target. It meaningfully duplicates data that I1 + I2 + a live aggregate can serve, so we would rather not ship it. Listed for completeness.

---

## 5. Per-query optimization strategy

| Query | Complex? | Index / view changes | Rewrites to consider | >15s→<1s target per rubric? |
|-------|----------|----------------------|----------------------|-----------------------------|
| **Q1** — versions of a package | No | I3 covers `WHERE package_id = ? ORDER BY released DESC`. I11 is redundant with I3 — skip. | None. Already fast enough. | No. |
| **Q2** — direct dependency counts | No | I1 (`from_version_id`) + MV1 (to resolve each package's latest version without a text-column join). | Replace `JOIN versions v ON v.version = p.latest_version` with `JOIN latest_versions lv ON lv.package_id = p.id`. | No, but a free side-beneficiary of MV1 + I1. |
| **Q3** — recursive BFS dependency traversal | **Yes** | I1 is the must-have. MV1 removes the text-join at the BFS root. I8 is an optional booster if I1 alone does not get the median query under 1s. | Use `WITH RECURSIVE ... UNION` (set-dedup) rather than `UNION ALL` to get `CYCLE`-safe de-dup for free. Apply a frontier `LIMIT` (e.g. `LIMIT 10_000`) so a pathological package cannot blow up the response. Remove any `::text` casts in the recursive join — with MV1 the join key is `uuid = uuid`. Cap `maxDepth` at the route layer (already 20 per api-spec R3). | **Yes.** Q3 is the #1 rubric target; empirical >15s observed on Supabase today. |
| **Q4** — low-maintainer stale packages | No | I3 (latest release per package) + I4 (maintainer-count per package). | Push the `maintainer_count ≤ N` filter into a `HAVING` on the `maintainers` group-by so we never scan versions for packages with too many maintainers. | No. |
| **Q5** — most depended-on packages | No | I2 (`to_package_id`). MV2 is a fallback if the live `GROUP BY to_package_id` still misses the target. | Cache the Home-page `LIMIT 10` result via Next.js route-segment cache (PLAN §8 already lists caching). | No, but the caching note matters for the Home page's first paint. |
| **Q6** — maintainers with most packages | No | None required. I10 intentionally *skipped* — see row I10 rationale. | Pre-aggregate with a CTE if the distinct-package count per username is not already part of the query. | No. |
| **Q7** — abandoned but popular | **Yes** | I2 (dependents count) + I3 (max release per package). | Replace the correlated / HAVING-based "last release older than 2y" filter — today written similar to `GROUP BY p.id HAVING MAX(v.released) < NOW() - INTERVAL '2 years'` — with a **windowed or lateral subquery** that materializes "latest release per package" once and joins to it, so the dependents aggregate does not iterate over the whole `versions` table. MV1 makes that subquery a single lookup per package. | **Yes.** Q7 is the second rubric target. Index-backed latest-release lookup + dependents aggregate should land well under 1s. |
| **Q8** — depth-bounded dependency walk | **Yes** | I1 + MV1 (same combo as Q3). I8 optional. | Same recursive-CTE hygiene as Q3: set-dedup, frontier `LIMIT`, `maxDepth` cap. Use `ARRAY` of visited `version_id`s in the CTE to bound path re-exploration. | Backup target. We aim for Q3+Q7 to satisfy the rubric; Q8 is a third complex query that inherits Q3's fixes at no extra cost. |
| **Q9** — packages without repos | No | I5 (partial index `WHERE has_repository IS FALSE`) if that fraction is small; otherwise plain seq scan is fine. | Once the Supabase column is `boolean`, drop `::text` comparisons. Prefer `has_repository IS FALSE` over `has_repository = FALSE` for 3-valued-logic safety (`NULL` → unknown repo state, currently represented as `''` from the cleaner). | No. |
| **Q10** — multi-signal risk scoring | **Yes** | I2 (fan-in), I3 (staleness), I4 (maintainer count), I5 (no-repo), MV1 (latest version), MV2 only as a last resort. | Compute each signal as an independent CTE, then `JOIN` them on `package_id` and apply the weight expression once. Avoid per-row correlated subqueries. Normalize on the fly with a windowed `MIN()/MAX()` rather than a second scan. | Backup target. Hard to hit 1s because Q10 touches every table; more realistically, target <3s here and lean on Q3+Q7 for the rubric's <1s pair. |

---

## 6. Prioritization

**Top 3 indexes to apply first, in order:**

1. **I1 — `dependencies(from_version_id)`**. Single biggest lever for Q3 and Q8. Without it no BFS will ever land under 1s.
2. **I2 — `dependencies(to_package_id)`**. Without it, Q5 is an O(n) aggregate and Q7's dependents-per-package join is unbounded.
3. **I3 — `versions(package_id, released DESC)`**. Unlocks Q7's staleness filter (the second rubric target) and removes the correlated-subquery pattern in Q4/Q10.

**Plus MV1 (`latest_versions`)** — not an index but the other half of the Q3/Q8 fix. Treat it as a co-requisite of I1.

**How this combination satisfies the rubric:**
- Q3 drops from **>15s → <1s** once I1 + MV1 replace the per-BFS-step seq scan and the `v.version = p.latest_version` text join. This is the currently-observed >15s query per PLAN §8, and is our #1 rubric proof point.
- Q7 drops from **>15s → <1s** once I2 + I3 + the windowed rewrite turn the "dependents of packages stale for ≥2y" scan into two index-backed aggregates joined on `package_id`. This is the second rubric proof point.
- Q8 and Q10 become secondary proof points (targeting <1s and <3s respectively) — nice to have, not required for the rubric.

I4, I5, I7 are worth doing but secondary; they improve user-facing routes (A1 search, A6 risk breakdown) and the Q10 composite, not the rubric-qualifying queries.

---

## 7. Risks and open questions

- **Write amplification during scrapes.** `collect_data.py` bulk-loads CSVs into `packages`, `versions`, `dependencies`, `maintainers`. Each new btree index lengthens the insert/update path proportionally to index count × rows inserted. Mitigation for v1 (not in this PR): `DROP INDEX`, bulk-insert, `CREATE INDEX`, `ANALYZE`. The scraper runs are manual and infrequent, so this is acceptable.
- **Storage cost.** Five btree + one partial + one GIN index on a ~10⁶-row `dependencies` table is not huge in absolute terms (single-digit GB at most) but is worth measuring on Supabase after the first post-opt scrape to be sure we do not exceed the project's storage budget.
- **Supabase connection-pool limits.** Supabase pools connections via PgBouncer in transaction mode, which blocks some `prepared statements` patterns. None of the proposed indexes or the MV refresh rely on cross-statement prepared-statement state, so this should be a non-issue, but we should run the first refresh from the Supabase SQL editor (direct connection) rather than the pooled Next.js client.
- **Autovacuum sufficiency.** Default Supabase autovacuum is fine for read-heavy workloads. We should still verify `pg_stat_user_tables.last_autovacuum` after the first big scrape; if bloat creeps in on `dependencies`, tune `autovacuum_vacuum_scale_factor` for that table specifically. Not a blocker for this plan.
- **Materialized view staleness.** MV1 is refreshed on a cadence, not on write. The window between a scraper run finishing and the MV refreshing produces "latest version" results that lag reality. For v1 this is explicitly acceptable — document it in the M5 report.
- **Partial index definition for I5.** Depends on whether Supabase stores `has_repository` as `boolean` or `text`. The cleaner emits `'true'` / `'false'` / `''`. If the table is still `text`, the partial predicate must be `WHERE has_repository = 'false'` and `NULL`-safe logic is effectively shifted to `''`. Migrating to `boolean` is a prerequisite we would add to the index-application PR.
- **`versions.released` type.** Same story: I3's `released DESC` ordering is only well-defined on `timestamptz`. A one-time cast migration is a prerequisite for I3 and MV1's ordering. Scope: not in this PR.
- **pg_trgm extension for I7.** Available on Supabase but requires `CREATE EXTENSION` in the schema. Document in the index-application PR.

---

## 8. Not doing yet

**This PR is documentation only.** No indexes are created, no materialized view is created, no SQL is executed, and no schema migrations are added to the repo. The application PR comes later, after we:

1. Measure the baseline (pre-index) runtime for Q1–Q10 on production-ish Supabase data and record it in `docs/query-optimization-log.md` (to be created).
2. Apply I1, I2, I3 and MV1 first, re-measure, and confirm Q3 and Q7 drop below 1s.
3. Apply I4, I5, I7 as a second batch only if the corresponding user-facing routes need the win.
4. Decide on I8 and MV2 based on whether post-step-2 numbers miss any targets.
5. Skip I9, I10, I11 unless profiling data contradicts the rationale in §3.

Step 1's before/after table is what the final report ultimately submits for the rubric's >15s → <1s evidence.
