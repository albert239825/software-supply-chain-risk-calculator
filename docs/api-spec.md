# API Specification — v1

> Spec for the Next.js backend routes that power the Supply Chain Risk Scorer web app. Every non-auxiliary route is backed by one of the 10 M3 SQL queries (per CIS 5500 rubric). Auxiliary routes exist to support page composition and do not necessarily correspond to an M3 query.

- **Base URL (dev):** `http://localhost:3000`
- **Prefix:** `/api`
- **Auth:** none in v1 (all endpoints public read-only)
- **Response format:** JSON
- **Pagination:** `?limit=&offset=` on list endpoints; list responses have the shape `{ items, meta: { total, limit, offset } }`
- **Ecosystem filter:** every route accepts an optional `?ecosystem=` query param. Default is `npm`. Included for PyPI-readiness; PyPI is stretch (see PLAN §10).
- **Package identifier:** the canonical package id is `packages.id` (UUIDv5 string), e.g. `0063402a-1335-5d56-b371-0ac3026e129d`. Routes that take `:packageId` expect this UUID. Name-based lookup goes through the search endpoint.
- **Errors:** HTTP status codes only; body is `{ "error": "<message>" }`. No per-request error envelope on success responses.

## Common types

```ts
type Ecosystem = "npm" | "pypi";
type ISODate = string;      // "2025-01-14T09:23:11Z"
type UUID = string;

type Package = {
  id: UUID;
  ecosystem: Ecosystem;
  name: string;
  description: string;
  latest_version: string;
};

type Version = {
  id: UUID;
  package_id: UUID;
  version: string;
  released: ISODate | null;
  has_repository: boolean | null;
  github_owner: string | null;
  github_repo: string | null;
};

type Maintainer = {
  id: UUID;
  package_id: UUID | null;
  username: string;
  name: string | null;
  role: string | null;
  email: string | null;
};

type RiskBreakdown = {
  composite: number;        // normalized 0..1
  bucket: "low" | "medium" | "high";
  signals: {
    maintainer_count:   { value: number; normalized: number; weight: number };
    staleness_years:    { value: number; normalized: number; weight: number };
    fanout_direct:      { value: number; normalized: number; weight: number };
    fanin_dependents:   { value: number; normalized: number; weight: number };
    has_repository:     { value: boolean; normalized: number; weight: number };
  };
};

type ListMeta = { total: number; limit: number; offset: number };
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

| # | Route | Method | Purpose |
|---|-------|--------|---------|
| A1 | `/api/packages/search` | GET | Search/autocomplete by name prefix |
| A2 | `/api/packages/:packageId` | GET | Single-package metadata + latest version |
| A3 | `/api/packages/:packageId/maintainers` | GET | Maintainers of a given package |
| A4 | `/api/packages/:packageId/dependencies` | GET | Direct dependencies of latest version |
| A5 | `/api/packages/:packageId/dependents` | GET | Packages that directly depend on this one |
| A6 | `/api/packages/:packageId/risk` | GET | Composite risk + per-signal breakdown for one package |
| A7 | `/api/stats/counts` | GET | Global counts (packages, versions, maintainers, edges) for Home header |
| A8 | `/api/health` | GET | Liveness/readiness for the mentor check-in & CI |

Total: **10 query-backed + 8 auxiliary = 18 routes.**

---

## 2. Query-backed routes (detailed)

### R1. `GET /api/packages/:packageId/versions` — Q1

List all versions of a package ordered from newest release to oldest.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `packageId` | path | UUID | yes | `packages.id` |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |
| `limit` | query | int | no | Default 100, max 500 |
| `offset` | query | int | no | Default 0 |

**Response 200**
```json
{
  "items": [
    { "id": "...", "package_id": "...", "version": "1.2.3", "released": "2025-01-14T09:23:11Z", "has_repository": true, "github_owner": "org", "github_repo": "lib" }
  ],
  "meta": { "total": 42, "limit": 100, "offset": 0 }
}
```
**Other:** `404` if `packageId` unknown.

---

### R2. `GET /api/stats/top-fanout` — Q2

Top-N packages by direct dependency count (attack-surface proxy).

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `limit` | query | int | no | Default 10, max 100 |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
```json
{
  "items": [
    { "package": { "id": "...", "name": "webpack", "latest_version": "5.x" }, "num_dependencies": 34 }
  ],
  "meta": { "total": 10, "limit": 10, "offset": 0 }
}
```

---

### R3. `GET /api/packages/:packageId/graph` — Q3

Recursive transitive dependency traversal from a package's latest version. Returns nodes + edges suitable for graph rendering.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `packageId` | path | UUID | yes | Root package |
| `maxDepth` | query | int | no | Default 4, capped at 20. Larger values are slow without optimizations. |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
```json
{
  "root": { "id": "...", "name": "...", "version": "..." },
  "nodes": [
    { "id": "<version_id>", "package_id": "...", "name": "lodash", "version": "4.17.21", "depth": 1 }
  ],
  "edges": [
    { "from_version_id": "...", "to_package_id": "...", "dep_kind": "dependency", "version_spec": "^4.0.0", "depth": 1 }
  ],
  "meta": { "depth_reached": 4, "node_count": 137, "edge_count": 158, "truncated": false }
}
```
**Other:**
- `404` if `packageId` unknown.
- `truncated: true` if the BFS hit `maxDepth` before settling.

> Complexity note: target >15s pre-opt / <1s post-opt per PLAN §8. Needs `dependencies(from_version_id)` index + materialized "latest version per package" view.

---

### R4. `GET /api/risk/stale-low-maintainer` — Q4

Packages with ≤N maintainers and the oldest last-release date.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `maxMaintainers` | query | int | no | Default 2 |
| `limit` | query | int | no | Default 50, max 500 |
| `offset` | query | int | no | Default 0 |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
```json
{
  "items": [
    { "package": { "id": "...", "name": "..." }, "maintainer_count": 1, "last_release": "2022-03-10T00:00:00Z" }
  ],
  "meta": { "total": 312, "limit": 50, "offset": 0 }
}
```

---

### R5. `GET /api/stats/most-dependents` — Q5

Packages most frequently depended on (ecosystem blast-radius ranking).

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `limit` | query | int | no | Default 10, max 100 |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
```json
{
  "items": [
    { "package": { "id": "...", "name": "tslib" }, "dependents": 1843 }
  ],
  "meta": { "total": 10, "limit": 10, "offset": 0 }
}
```

---

### R6. `GET /api/maintainers/top` — Q6

Maintainers responsible for the most packages (trust-concentration signal).

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `limit` | query | int | no | Default 10, max 100 |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
```json
{
  "items": [
    { "username": "sindresorhus", "num_packages": 917 }
  ],
  "meta": { "total": 10, "limit": 10, "offset": 0 }
}
```

---

### R7. `GET /api/risk/abandoned-popular` — Q7

Packages widely used but not updated in ≥N years.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `ageYears` | query | int | no | Default 2 |
| `limit` | query | int | no | Default 50, max 500 |
| `offset` | query | int | no | Default 0 |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
```json
{
  "items": [
    { "package": { "id": "...", "name": "..." }, "dependents": 412, "last_release": "2020-01-01T00:00:00Z" }
  ],
  "meta": { "total": 88, "limit": 50, "offset": 0 }
}
```

> Complexity note: candidate for the second >15s → <1s query. Optimized via `versions(package_id, released DESC)` index + dependents aggregation rewrite.

---

### R8. `GET /api/stats/depth-below` — Q8

Packages whose maximum transitive dependency depth is below a threshold (shallow-tree packages).

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `maxDepth` | query | int | no | Default 3 |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |
| `limit` | query | int | no | Default 100, max 500 |
| `offset` | query | int | no | Default 0 |

**Response 200**
```json
{
  "items": [
    { "package_id": "...", "package_name": "leftpad", "max_dependency_depth": 1 }
  ],
  "meta": { "total": 4821, "limit": 100, "offset": 0 }
}
```

> Complexity note: also a >15s candidate; shares the materialized "latest version per package" view with R3.

---

### R9. `GET /api/packages/no-repo` — Q9

Package versions that have no associated source-code repository (transparency negative signal).

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `limit` | query | int | no | Default 50, max 500 |
| `offset` | query | int | no | Default 0 |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
```json
{
  "items": [
    { "package_name": "some-pkg", "version": "0.1.2", "package_id": "..." }
  ],
  "meta": { "total": 17203, "limit": 50, "offset": 0 }
}
```

---

### R10. `GET /api/risk/ranked` — Q10

Packages ranked by composite multi-signal risk score (the flagship Home-page list).

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `limit` | query | int | no | Default 20, max 100 |
| `offset` | query | int | no | Default 0 |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
```json
{
  "items": [
    {
      "package": { "id": "...", "name": "..." },
      "maintainers": 1,
      "dependencies": 9,
      "last_release": "2021-06-10T00:00:00Z",
      "risk_score": 7.42
    }
  ],
  "meta": { "total": 100, "limit": 20, "offset": 0 }
}
```

> The formula is documented in PLAN §7. This endpoint returns the raw score on the "baseline" scale used in M3 Q10. The per-package breakdown page uses A6, which returns the normalized 0..1 composite.

---

## 3. Auxiliary routes (detailed)

### A1. `GET /api/packages/search`

Prefix/substring search by package name for the global search bar.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `q` | query | string | yes | Min length 1 |
| `limit` | query | int | no | Default 10, max 50 |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
```json
{
  "items": [
    { "id": "...", "ecosystem": "npm", "name": "lodash", "latest_version": "4.17.21" }
  ],
  "meta": { "total": 4, "limit": 10, "offset": 0 }
}
```
**Other:** `400` if `q` missing.

---

### A2. `GET /api/packages/:packageId`

Single-package metadata used by the Package Detail and Risk Analysis pages.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `packageId` | path | UUID | yes | |
| `ecosystem` | query | `Ecosystem` | no | Defaults to `npm` |

**Response 200**
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
**Other:** `404` if `packageId` unknown.

---

### A3. `GET /api/packages/:packageId/maintainers`

Maintainers for a single package (deduped by username).

**Response 200**
```json
{
  "items": [
    { "username": "dougwilson", "name": "Douglas Wilson", "role": "maintainer", "email": null }
  ],
  "meta": { "total": 4, "limit": 100, "offset": 0 }
}
```

---

### A4. `GET /api/packages/:packageId/dependencies`

Direct dependencies of the package's latest version.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `packageId` | path | UUID | yes | |
| `depKind` | query | `"dependency" \| "peer" \| "optional" \| "all"` | no | Default `all` |
| `limit` | query | int | no | Default 100, max 500 |
| `offset` | query | int | no | Default 0 |

**Response 200**
```json
{
  "items": [
    { "to_package": { "id": "...", "name": "cookie" }, "version_spec": "0.4.2", "dep_kind": "dependency" }
  ],
  "meta": { "total": 27, "limit": 100, "offset": 0 }
}
```

---

### A5. `GET /api/packages/:packageId/dependents`

Packages that declare this package as a direct dependency.

**Request**

| Param | In | Type | Required | Description |
|---|---|---|---|---|
| `packageId` | path | UUID | yes | |
| `limit` | query | int | no | Default 50, max 500 |
| `offset` | query | int | no | Default 0 |

**Response 200**
```json
{
  "items": [
    { "from_package": { "id": "...", "name": "body-parser" }, "from_version": "1.20.0", "dep_kind": "dependency" }
  ],
  "meta": { "total": 1843, "limit": 50, "offset": 0 }
}
```

---

### A6. `GET /api/packages/:packageId/risk`

Composite risk + per-signal breakdown for a single package. Drives the Risk Analysis page.

**Response 200**
```json
{
  "package": { "id": "...", "name": "..." },
  "risk": {
    "composite": 0.78,
    "bucket": "high",
    "signals": {
      "maintainer_count":   { "value": 1, "normalized": 1.00, "weight": 0.30 },
      "staleness_years":    { "value": 3.4, "normalized": 0.85, "weight": 0.30 },
      "fanout_direct":      { "value": 9, "normalized": 0.42, "weight": 0.20 },
      "fanin_dependents":   { "value": 412, "normalized": 0.61, "weight": 0.10 },
      "has_repository":     { "value": false, "normalized": 1.00, "weight": 0.10 }
    }
  }
}
```

> Weights match PLAN §7; they are placeholders pending decision D4. This route reuses the same signal SQL as R4/R7/R9/R10 but aggregated for one package.

---

### A7. `GET /api/stats/counts`

Global counts used in the Home-page header card.

**Response 200**
```json
{
  "packages": 41235,
  "versions": 182934,
  "maintainers": 29841,
  "dependencies": 612907,
  "ecosystem": "npm"
}
```

---

### A8. `GET /api/health`

Liveness + DB connectivity probe.

**Response 200**
```json
{ "status": "ok", "db": "ok", "time": "2026-04-18T18:30:00Z" }
```
**Other:** `503` if the Supabase connection check fails.

---

## 4. Status codes used

| Code | When |
|------|------|
| `200 OK` | Successful GET |
| `400 Bad Request` | Missing/invalid query param (e.g. `q` missing on A1, `maxDepth` out of range on R3) |
| `404 Not Found` | Unknown `packageId` |
| `500 Internal Server Error` | Unhandled exception |
| `503 Service Unavailable` | A8 DB check fails |

---

## 5. Page → route coverage

| Page | Routes used |
|------|-------------|
| Home | R10 (ranked risk), R5 (most dependents), R2 (top fan-out), A7 (counts), A1 (search) |
| Graph Explorer | A2 (package metadata), R3 (graph), R8 (depth stats), A6 (per-node risk on hover) |
| Risk Analysis | A2, A6, R4 (low-maintainer stale), R7 (abandoned popular), R10 |
| Package Detail | A2, R1 (versions), A3 (maintainers), A4 (deps), A5 (dependents), R6 (top maintainers), R9 (no-repo) |

Every page consumes at least three distinct routes, and every M3 query is reachable from at least one page — rubric compliant.

---

## 6. Open questions

Track decisions in PLAN §9. Spec-specific opens:

- **Pagination style**: offset/limit chosen for simplicity. Switch to cursor on R3 only if the graph response becomes too large to return in one shot (unlikely at `maxDepth ≤ 6`).
- **Normalization source for A6/R10**: compute normalized signals against all packages in the DB, or against the set of packages present in the user's "project"? v1 assumes global normalization; flag for the team if project-scoped normalization is wanted.
- **R3 truncation policy**: currently set `truncated: true` when `maxDepth` was hit. Alternative: return partial frontier + a `continue_token`. Deferred to v1.1.
- **Caching headers**: no `Cache-Control` in v1; rely on Next.js route-segment caching. Revisit before demo if p95 latency is poor.
