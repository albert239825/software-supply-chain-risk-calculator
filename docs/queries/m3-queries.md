# M3 SQL Queries

> Canonical list of the 10 SQL queries submitted for CIS 5500 Milestone 3. Each query backs exactly one route in the API spec and surfaces on one or more pages in the v1 UI.

- See [`../PLAN.md`](../PLAN.md) §8 for the query-to-route-to-page mapping and the optimization plan.
- See [`../api-spec.md`](../api-spec.md) for the detailed request/response contract of routes R1–R10.

## Summary

| Query | Title | Complex | Route (api-spec.md) |
|-------|-------|---------|---------------------|
| Q1 | List all packages and their versions | No | R1 `/api/packages/:packageId/versions` |
| Q2 | Counting dependencies | No | R2 `/api/stats/top-fanout` |
| Q3 | Recursive dependency traversal | Yes | R3 `/api/packages/:packageId/graph` |
| Q4 | High-risk packages (low maintainer count, stale) | No | R4 `/api/risk/stale-low-maintainer` |
| Q5 | Most depended-on packages | No | R5 `/api/stats/most-dependents` |
| Q6 | Maintainers responsible for the most packages | No | R6 `/api/maintainers/top` |
| Q7 | Abandoned but popular packages | Yes | R7 `/api/risk/abandoned-popular` |
| Q8 | Packages with shallow dependency depth | Yes | R8 `/api/stats/depth-below` |
| Q9 | Packages without repositories | No | R9 `/api/packages/no-repo` |
| Q10 | Multi-signal risk scoring | Yes | R10 `/api/risk/ranked` |

Complexity counts: **4 complex** (Q3, Q7, Q8, Q10) — meets the CIS 5500 rubric minimum of ≥4 complex queries. Optimization targets for the complex set are tracked in PLAN §8.

---

### Q1: List all packages and their versions

- **Complex:** No
- **Backs route:** R1 in docs/api-spec.md
- **Surfaces on pages:** Package Detail
- **Optimization target:** fast, no optimization needed

**Description:** Retrieves latest versions of a given package along with their release timestamps, ordered from most recent to oldest. Useful for identifying outdated or stagnant packages.

```sql
SELECT p.name AS package_name, v.version, v.released
FROM packages p
JOIN versions v ON p.id = v.package_id
ORDER BY v.released DESC;
```

---

### Q2: Counting dependencies

- **Complex:** No
- **Backs route:** R2 in docs/api-spec.md
- **Surfaces on pages:** Graph Explorer, Package Detail
- **Optimization target:** fast, no optimization needed

**Description:** Computes the number of direct dependencies associated with each package and returns the top packages with the largest dependency counts. Packages with many dependencies tend to have larger attack surfaces.

```sql
SELECT p.name AS package_name, COUNT(d.to_package_id) AS num_dependencies
FROM packages p
JOIN versions v ON p.id = v.package_id
JOIN dependencies d ON v.id = d.from_version_id
GROUP BY p.name
ORDER BY num_dependencies DESC
LIMIT 10;
```

---

### Q3: Recursive dependency traversal

- **Complex:** Yes
- **Backs route:** R3 in docs/api-spec.md
- **Surfaces on pages:** Graph Explorer
- **Optimization target:** >15s pre / <1s post

**Description:** Recursive BFS traversal of the dependency graph from a given package version, capturing direct and transitive dependencies with their depth in the graph.

```sql
WITH RECURSIVE bfs AS (
   SELECT
       0 AS depth,
       ARRAY['0063402a-1335-5d56-b371-0ac3026e129d'::text] AS frontier,
       ARRAY['0063402a-1335-5d56-b371-0ac3026e129d'::text] AS seen
   UNION ALL
   SELECT
       b.depth + 1,
       nxt.next_frontier,
       b.seen || nxt.next_frontier
   FROM bfs b
   JOIN LATERAL (
       SELECT array_agg(DISTINCT v_next.id::text) AS next_frontier
       FROM unnest(b.frontier) AS cur(from_version_id)
       JOIN dependencies d
         ON d.from_version_id::text = cur.from_version_id
       JOIN packages p
         ON p.id::text = d.to_package_id::text
       JOIN versions v_next
         ON v_next.package_id::text = p.id::text
        AND v_next.version = p.latest_version
       WHERE NOT (v_next.id::text = ANY(b.seen))
   ) AS nxt
     ON cardinality(nxt.next_frontier) > 0
   WHERE b.depth < 20
),
dep_tree AS (
   SELECT
       d.from_version_id,
       d.to_package_id,
       d.from_package,
       d.from_version,
       d.to_package,
       d.version_spec,
       d.dep_kind,
       b.depth + 1 AS depth
   FROM bfs b
   JOIN LATERAL unnest(b.frontier) AS cur(from_version_id) ON TRUE
   JOIN dependencies d
     ON d.from_version_id::text = cur.from_version_id
), graph AS (
       SELECT DISTINCT ON (
       to_package_id,
       dep_kind
       )
       from_version_id,
       to_package_id,
       from_package,
       from_version,
       to_package,
       version_spec,
       dep_kind,
       depth
       FROM dep_tree
       ORDER BY
           to_package_id,
           dep_kind,
           depth
)
SELECT max(g.depth) from graph g;
```

---

### Q4: High-risk packages (low maintainer count, stale)

- **Complex:** No
- **Backs route:** R4 in docs/api-spec.md
- **Surfaces on pages:** Risk Analysis
- **Optimization target:** fast, no optimization needed

**Description:** Identifies packages with ≤2 maintainers and the oldest last-release dates. Few maintainers and stale releases are risk indicators.

```sql
SELECT 
    p.name AS package_name,
    COUNT(m.id) AS maintainer_count,
    MAX(v.released) AS last_release
FROM packages p
JOIN versions v ON p.id = v.package_id
LEFT JOIN maintainers m ON p.id = m.package_id
GROUP BY p.name
HAVING COUNT(m.id) <= 2
ORDER BY last_release ASC;
```

---

### Q5: Most depended-on packages

- **Complex:** No
- **Backs route:** R5 in docs/api-spec.md
- **Surfaces on pages:** Home
- **Optimization target:** fast, no optimization needed

**Description:** Identifies packages that are most frequently depended on, acting as critical infrastructure whose vulnerabilities can propagate widely.

```sql
SELECT 
    p.name AS package_name,
    COUNT(*) AS dependents
FROM dependencies d
JOIN packages p ON d.to_package_id = p.id
GROUP BY p.name
ORDER BY dependents DESC
LIMIT 10;
```

---

### Q6: Maintainers responsible for the most packages

- **Complex:** No
- **Backs route:** R6 in docs/api-spec.md
- **Surfaces on pages:** Package Detail
- **Optimization target:** fast, no optimization needed

**Description:** Finds maintainers who own many packages — a concentration-of-control risk signal.

```sql
SELECT 
    m.username,
    COUNT(DISTINCT m.package_id) AS num_packages
FROM maintainers m
GROUP BY m.username
ORDER BY num_packages DESC
LIMIT 10;
```

---

### Q7: Abandoned but popular packages

- **Complex:** Yes
- **Backs route:** R7 in docs/api-spec.md
- **Surfaces on pages:** Risk Analysis
- **Optimization target:** >15s pre / <1s post

**Description:** Finds packages widely used but not updated in ≥2 years — embedded but unmaintained, high-impact risk.

```sql
SELECT
 p.name AS package_name,
 COUNT(DISTINCT d.from_package) AS dependents,
 MAX(v.released) AS last_release
FROM packages p
JOIN versions v
 ON v.package_id = p.id
JOIN dependencies d
 ON d.to_package_id = p.id
GROUP BY p.id, p.name
HAVING MAX(v.released::timestamp) < (NOW() - INTERVAL '2 years')
ORDER BY dependents DESC;
```

---

### Q8: Packages with shallow dependency depth

- **Complex:** Yes
- **Backs route:** R8 in docs/api-spec.md
- **Surfaces on pages:** Graph Explorer
- **Optimization target:** >15s pre / <1s post

**Description:** Computes packages with maximum transitive dependency depth below a threshold. Shallower trees are easier to audit.

> **Bind parameter:** the literal `n` in `WHERE w.depth < n` is a bind parameter — the maximum recursion depth cap supplied by the caller (route R8 accepts it as a query-string argument; see `docs/api-spec.md`).

```sql
WITH RECURSIVE resolved_edges AS (
    SELECT DISTINCT
        d.from_version_id AS from_version_id,
        v.id              AS to_version_id
    FROM dependencies d
    JOIN packages p_to
      ON p_to.id = d.to_package_id
    JOIN versions v
      ON v.package_id = p_to.id
     AND v.version = p_to.latest_version
    WHERE d.from_version_id IS NOT NULL
      AND d.to_package_id IS NOT NULL
),
walk AS (
    SELECT
        p_from.id   AS root_package_id,
        p_from.name AS root_package,
        e.to_version_id AS current_version_id,
        1 AS depth
    FROM resolved_edges e
    JOIN versions v_from
      ON v_from.id = e.from_version_id
    JOIN packages p_from
      ON p_from.id = v_from.package_id

    UNION ALL

    SELECT
        w.root_package_id,
        w.root_package,
        e.to_version_id AS current_version_id,
        w.depth + 1 AS depth
    FROM walk w
    JOIN resolved_edges e
      ON e.from_version_id = w.current_version_id
    WHERE w.depth < n
),
package_depths AS (
    SELECT
        p.id   AS package_id,
        p.name AS package_name,
        COALESCE(MAX(w.depth), 0) AS max_dependency_depth
    FROM packages p
    LEFT JOIN walk w
      ON w.root_package_id = p.id
    GROUP BY p.id, p.name
)
SELECT
    package_id,
    package_name,
    max_dependency_depth
FROM package_depths
WHERE max_dependency_depth < 3
ORDER BY max_dependency_depth, package_name;
```

---

### Q9: Packages without repositories

- **Complex:** No
- **Backs route:** R9 in docs/api-spec.md
- **Surfaces on pages:** Package Detail
- **Optimization target:** fast, no optimization needed

**Description:** Identifies package versions with no associated source-code repository. Missing repos reduce auditability.

```sql
SELECT p.name AS package_name, v.version
FROM packages p
JOIN versions v ON p.id = v.package_id
WHERE COALESCE(LOWER(v.has_repository::text), '') IN ('false', '0', 'no', '');
```

---

### Q10: Multi-signal risk scoring

- **Complex:** Yes
- **Backs route:** R10 in docs/api-spec.md
- **Surfaces on pages:** Home, Risk Analysis
- **Optimization target:** >15s pre / <1s post

**Description:** Baseline composite risk score combining maintainer count, dependency exposure, and release staleness. Weights are placeholders; see [`../PLAN.md`](../PLAN.md) §7 for the v1 signal list and final weight-ratification plan.

```sql
SELECT 
    p.name AS package_name,
    COUNT(DISTINCT m.id) AS maintainers,
    COUNT(DISTINCT d.to_package_id) AS dependencies,
    MAX(v.released) AS last_release,
    (
        COUNT(DISTINCT d.to_package_id) * 0.4 +
        (2 - COUNT(DISTINCT m.id)) * 0.3 +
        EXTRACT(YEAR FROM AGE(NOW(), MAX(v.released)::timestamptz)) * 0.3
    ) AS risk_score
FROM packages p
JOIN versions v ON p.id = v.package_id
LEFT JOIN dependencies d ON v.id = d.from_version_id
LEFT JOIN maintainers m ON p.id = m.package_id
GROUP BY p.name
ORDER BY risk_score DESC
LIMIT 20;
```
