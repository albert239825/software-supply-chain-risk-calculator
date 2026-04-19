# API Specification — v1

> Spec for the Next.js backend routes that power the Supply Chain Risk Scorer web app. Every non-auxiliary route is backed by one of the 10 M3 SQL queries (per CIS 5500 rubric). Auxiliary routes exist to support page composition and do not necessarily correspond to an M3 query.

- **Base URL (dev):** `http://localhost:3000`
- **Prefix:** `/api`
- **Auth:** none (all endpoints public read-only)
- **Response format:** JSON
- **List endpoints:** return bare JSON arrays (`[...]`). Pagination (`limit`/`offset`) and `?ecosystem=` filtering are not yet implemented; sorting and limits are baked into each query.
- **Package identifier:** the canonical package id is `packages.id` (UUIDv5 string), e.g. `0063402a-1335-5d56-b371-0ac3026e129d`. Routes that take `:packageId` expect this UUID.
- **Errors:** HTTP status codes with body `{ "error": "<message>" }`. No envelope on success responses.
- **DB access:** direct Postgres via `pg.Pool`. Set `SUPABASE_DB_URL` in `web/.env.local` to the Supabase Postgres connection pooler URL (Dashboard → Project Settings → Database → Connection string → Transaction mode).

## Common types

```ts
type Ecosystem = "npm" | "pypi";
type ISODate = string;   // "2025-01-14T09:23:11Z"
type UUID = string;
```

---

## 1. Route index

### Query-backed routes (one per M3 query — rubric requirement)

| # | Route | Method | Backs query | Pages using it |
|---|-------|--------|-------------|----------------|
| R1 | `/api/packages/:packageId/versions` | GET | Q1 | Package Detail |
| R2 | `/api/stats/top-fanout` | GET | Q2 | Home, Graph Explorer |
| R3 | `/api/packages/:packageId/graph` | GET | Q3 | Graph Explorer |
| R4 | `/api/risk/stale-low-maintainer` | GET | Q4 | Risk Analysis |
| R5 | `/api/stats/most-dependents` | GET | Q5 | Home, Package Detail |
| R6 | `/api/maintainers/top` | GET | Q6 | Package Detail |
| R7 | `/api/risk/abandoned-popular` | GET | Q7 | Risk Analysis |
| R8 | `/api/stats/depth-below` | GET | Q8 | Graph Explorer |
| R9 | `/api/packages/no-repo` | GET | Q9 | Package Detail |
| R10 | `/api/risk/ranked` | GET | Q10 | Home, Risk Analysis |

### Auxiliary routes (no dedicated M3 query)

| # | Route | Method | Status | Purpose |
|---|-------|--------|--------|---------|
| A1 | `/api/packages/search` | GET | **planned** | Search/autocomplete by name prefix |
| A2 | `/api/packages/:packageId` | GET | shipped | Single-package metadata + latest version |
| A3 | `/api/packages/:packageId/maintainers` | GET | shipped | Maintainers of a given package |
| A4 | `/api/packages/:packageId/dependencies` | GET | shipped | Direct dependencies of latest version |
| A5 | `/api/packages/:packageId/dependents` | GET | shipped | Packages that directly depend on this one |
| A6 | `/api/packages/:packageId/risk` | GET | **planned** | Composite risk + per-signal breakdown for one package |
| A7 | `/api/stats/counts` | GET | **planned** | Global counts for Home header card |
| A8 | `/api/health` | GET | shipped | Liveness/readiness probe |
| — | `/api/packages/all` | GET | shipped | Flat list of every package × version, newest first (Packages page; not a rubric requirement) |

Complex queries (≥15s pre-opt target): R3, R7, R8, R10.

---

## 2. Query-backed routes (detailed)

### R1. `GET /api/packages/:packageId/versions` — Q1

All versions of a package ordered from newest release to oldest.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `packageId` | path | UUID | yes | `packages.id` |

**Response 200** — bare JSON array. Each element:

```json
{ "package_name": "express", "version": "4.18.2", "released": "2025-01-14T09:23:11Z" }
```

**Other:** `500` on DB error. (404 on unknown `packageId` is planned.)

---

### R2. `GET /api/stats/top-fanout` — Q2

Top 10 packages by direct dependency count (attack-surface proxy). Limit is fixed at 10.

**Request:** no query params.

**Response 200** — bare JSON array (up to 10 items). Each element:

```json
{ "package_name": "webpack", "num_dependencies": 34 }
```

---

### R3. `GET /api/packages/:packageId/graph` — Q3

Recursive transitive dependency traversal from the package's latest version using a BFS CTE. Returns dependency edges suitable for graph rendering.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `packageId` | path | UUID | yes | Root package |
| `maxDepth` | query | int | no | Default 4. Passed directly to SQL. |

**Response 200** — bare JSON array of dependency edges. Each element:

```json
{
  "from_version_id": "...",
  "to_package_id": "...",
  "from_package": "express",
  "from_version": "4.18.2",
  "to_package": "cookie",
  "version_spec": "0.4.2",
  "dep_kind": "dependency",
  "depth": 1
}
```

**Other:** `500` on DB error.

> Complexity note: needs `dependencies(from_version_id)` index + materialized "latest version per package" view for performance targets.

---

### R4. `GET /api/risk/stale-low-maintainer` — Q4

Packages with ≤ 2 maintainers ordered by oldest last-release. Maintainer threshold is fixed at 2.

**Request:** no query params.

**Response 200** — bare JSON array. Each element:

```json
{ "package_name": "some-pkg", "maintainer_count": 1, "last_release": "2022-03-10T00:00:00Z" }
```

---

### R5. `GET /api/stats/most-dependents` — Q5

Top 10 packages most frequently depended on (ecosystem blast-radius ranking). Limit is fixed at 10.

**Request:** no query params.

**Response 200** — bare JSON array (up to 10 items). Each element:

```json
{ "package_name": "tslib", "dependents": 1843 }
```

---

### R6. `GET /api/maintainers/top` — Q6

Top 10 maintainers responsible for the most packages (trust-concentration signal). Limit is fixed at 10.

**Request:** no query params.

**Response 200** — bare JSON array (up to 10 items). Each element:

```json
{ "username": "sindresorhus", "num_packages": 917 }
```

---

### R7. `GET /api/risk/abandoned-popular` — Q7

Packages widely used but not updated in ≥ 2 years. Age threshold is fixed at 2 years.

**Request:** no query params.

**Response 200** — bare JSON array. Each element:

```json
{ "package_name": "some-pkg", "dependents": 412, "last_release": "2020-01-01T00:00:00Z" }
```

> Complexity note: optimized via `versions(package_id, released DESC)` index + dependents aggregation rewrite.

---

### R8. `GET /api/stats/depth-below` — Q8

Packages whose maximum transitive dependency depth is below a threshold.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `n` | query | int | no | Depth threshold. Default 3. Results have `max_dependency_depth < n`. |

**Response 200** — bare JSON array. Each element:

```json
{ "package_id": "...", "package_name": "leftpad", "max_dependency_depth": 1 }
```

> Complexity note: also a >15s candidate; shares the materialized latest-version-per-package view with R3.

---

### R9. `GET /api/packages/no-repo` — Q9

Package versions that have no associated source-code repository (transparency negative signal).

**Request:** no query params.

**Response 200** — bare JSON array. Each element:

```json
{ "package_name": "some-pkg", "version": "0.1.2" }
```

---

### R10. `GET /api/risk/ranked` — Q10

Packages ranked by composite multi-signal risk score (the flagship Home-page list). Returns top 20 packages; limit is fixed. The composite score is computed in TypeScript via `web/lib/risk/score.ts` (`computeComposite`) — SQL materializes the raw per-package signals, and the score is calculated in TS so the future A6 per-package breakdown stays in lockstep. Do not re-inline weights in SQL.

**Request:** no query params.

**Response 200** — bare JSON array (up to 20 items, sorted descending by `risk_score`). Each element:

```json
{
  "package_id": "...",
  "package_name": "some-pkg",
  "maintainers": 1,
  "dependencies": 9,
  "dependents": 412,
  "last_release": "2021-06-10T00:00:00Z",
  "risk_score": 0.82,
  "bucket": "high"
}
```

`risk_score` is the normalized 0..1 composite computed from five signals: maintainer count, staleness (years since last release), direct dependency fanout, dependent count (fan-in), and presence of a source repository. `bucket` is `"low"` | `"medium"` | `"high"`.

---

## 3. Auxiliary routes (detailed)

### A1. `GET /api/packages/search` — planned

Prefix/substring search by package name for the global search bar. Not yet implemented.

---

### A2. `GET /api/packages/:packageId`

Single-package metadata used by the Package Detail and Risk Analysis pages.

**Request**

| Param | In | Type | Required |
|---|---|---|---|
| `packageId` | path | UUID | yes |

**Response 200** — single object:

```json
{
  "id": "...",
  "ecosystem": "npm",
  "name": "express",
  "description": "Fast, unopinionated, minimalist web framework...",
  "latest_version": "4.18.2",
  "latest_released": "2025-01-14T09:23:11Z",
  "has_repository": true,
  "github_owner": "expressjs",
  "github_repo": "express"
}
```

**Other:** `404 { "error": "package not found" }` if `packageId` unknown.

---

### A3. `GET /api/packages/:packageId/maintainers`

Maintainers for a single package.

**Request**

| Param | In | Type | Required |
|---|---|---|---|
| `packageId` | path | UUID | yes |

**Response 200** — bare JSON array. Each element:

```json
{ "id": "...", "username": "dougwilson", "name": "Douglas Wilson", "role": "maintainer", "email": null }
```

---

### A4. `GET /api/packages/:packageId/dependencies`

Direct dependencies of the package's latest version.

**Request**

| Param | In | Type | Required |
|---|---|---|---|
| `packageId` | path | UUID | yes |

**Response 200** — bare JSON array. Each element:

```json
{ "package_id": "...", "package_name": "cookie", "version_spec": "0.4.2", "dep_kind": "dependency" }
```

---

### A5. `GET /api/packages/:packageId/dependents`

Packages that declare this package as a direct dependency.

**Request**

| Param | In | Type | Required |
|---|---|---|---|
| `packageId` | path | UUID | yes |

**Response 200** — bare JSON array. Each element:

```json
{ "package_id": "...", "package_name": "body-parser", "dependent_version": "1.20.0" }
```

---

### A6. `GET /api/packages/:packageId/risk` — planned

Composite risk + per-signal breakdown for a single package. Not yet implemented. Will reuse `web/lib/risk/score.ts` (same module as R10) but scoped to one package, returning the normalized 0..1 composite and per-signal breakdown.

---

### A7. `GET /api/stats/counts` — planned

Global counts for the Home-page header card. Not yet implemented.

---

### A8. `GET /api/health`

Liveness + DB connectivity probe.

**Request:** none.

**Response 200:**

```json
{ "status": "ok", "db": "ok", "time": "2026-04-18T18:30:00Z" }
```

`db` can be `"ok"` (DB reachable) or `"unconfigured"` (Supabase env vars not set — returned as 200 for local dev / CI without credentials).

**Response 503:**

```json
{ "status": "error", "db": "error", "time": "2026-04-18T18:30:00Z", "error": "<message>" }
```

---

## 4. Extra route (not a rubric requirement)

### `GET /api/packages/all`

Flat list of every package × version ordered by release date descending. Used by the Packages page.

**Request:** no query params.

**Response 200** — bare JSON array. Each element:

```json
{ "package_name": "express", "version": "4.18.2", "released": "2025-01-14T09:23:11Z" }
```

---

## 5. Status codes used

| Code | When |
|------|------|
| `200 OK` | Successful GET |
| `404 Not Found` | Unknown `packageId` (A2 only; other routes do not yet return 404) |
| `500 Internal Server Error` | Unhandled exception |
| `503 Service Unavailable` | A8 DB check fails |

---

## 6. Page → route coverage

| Page | Routes used |
|------|-------------|
| Home | R10 (ranked risk), R5 (most dependents), R2 (top fan-out) |
| Graph Explorer | R3 (graph), R8 (depth stats) |
| Risk Analysis | R4 (low-maintainer stale), R7 (abandoned popular), R10 |
| Package Detail | A2 (metadata), R1 (versions), A3 (maintainers), A4 (deps), A5 (dependents), R9 (no-repo) |
| Packages | `/api/packages/all` |
