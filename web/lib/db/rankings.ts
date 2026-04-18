import type { SupabaseClient } from "@supabase/supabase-js";

import type { Ecosystem, Package } from "@/types/api";

/**
 * Typed Supabase query helpers for the B-rank route cluster (R2, R5, R6, R10).
 *
 * This module is intentionally separate from the per-table helpers under
 * `web/lib/db/{packages,versions,dependencies,maintainers}.ts`. Those helpers
 * deal with single-table CRUD-ish access; this one backs the ranking /
 * aggregation routes and is owned by the B-rank sub-devin so the two
 * workstreams can land in parallel without overlapping edits.
 *
 * -----------------------------------------------------------------------
 * Required Supabase views
 * -----------------------------------------------------------------------
 *
 * The helpers below read from server-side SQL views so the GROUP BY +
 * join-heavy aggregations happen in Postgres (PostgREST cannot express
 * these shapes directly). Someone on the team must apply the following
 * DDL to Supabase before any of these routes will return real data:
 *
 * -- v_top_fanout (backs R2 / Q2): packages ranked by direct dependency count.
 * CREATE OR REPLACE VIEW v_top_fanout AS
 * SELECT p.ecosystem,
 *        p.name AS package_name,
 *        COUNT(d.to_package_id) AS num_dependencies
 * FROM packages p
 * JOIN versions v ON p.id = v.package_id
 * JOIN dependencies d ON v.id = d.from_version_id
 * GROUP BY p.ecosystem, p.name;
 *
 * -- v_most_dependents (backs R5 / Q5): packages most frequently depended on.
 * CREATE OR REPLACE VIEW v_most_dependents AS
 * SELECT p.ecosystem,
 *        p.name AS package_name,
 *        COUNT(*) AS dependents
 * FROM dependencies d
 * JOIN packages p ON d.to_package_id = p.id
 * GROUP BY p.ecosystem, p.name;
 *
 * -- v_top_maintainers (backs R6 / Q6): maintainers by package count.
 * CREATE OR REPLACE VIEW v_top_maintainers AS
 * SELECT m.ecosystem,
 *        m.username,
 *        COUNT(DISTINCT m.package_id) AS num_packages
 * FROM maintainers m
 * GROUP BY m.ecosystem, m.username;
 *
 * -- v_risk_signals (backs R10 / Q10): one row per package with all the
 * --   numeric risk signals preaggregated so R10 fetches them in a single
 * --   round trip. staleness_years is derived client-side from
 * --   latest_released, so the view only exposes the raw timestamptz.
 * CREATE OR REPLACE VIEW v_risk_signals AS
 * SELECT
 *   p.id AS package_id,
 *   p.name AS package_name,
 *   p.ecosystem,
 *   p.description,
 *   p.latest_version,
 *   COALESCE((
 *     SELECT COUNT(DISTINCT m.id)
 *     FROM maintainers m
 *     WHERE m.package_id = p.id
 *   ), 0) AS maintainer_count,
 *   COALESCE((
 *     SELECT COUNT(DISTINCT d_out.to_package_id)
 *     FROM dependencies d_out
 *     JOIN versions v_out
 *       ON v_out.id = d_out.from_version_id
 *     WHERE v_out.package_id = p.id
 *       AND v_out.version = p.latest_version
 *   ), 0) AS fanout_direct,
 *   COALESCE((
 *     SELECT COUNT(DISTINCT d_in.from_package)
 *     FROM dependencies d_in
 *     WHERE d_in.to_package_id = p.id
 *   ), 0) AS fanin_dependents,
 *   lv.released AS latest_released,
 *   COALESCE(lv.has_repository, FALSE) AS has_repository
 * FROM packages p
 * LEFT JOIN versions lv
 *   ON lv.package_id = p.id
 *  AND lv.version   = p.latest_version;
 *
 * -----------------------------------------------------------------------
 * Optimization note
 * -----------------------------------------------------------------------
 *
 * R10 is a prime optimization candidate (see `docs/proposed-indexes.md`).
 * The Phase 1 plan below fetches every signal row for the ecosystem and
 * computes the composite in the Node process. For a fully-loaded NPM
 * ecosystem that linear scan is O(N) per request. The next step is a
 * materialized view `mv_risk_ranked` that persists the precomputed
 * composite and bucket. See `docs/query-optimization-log.md` for the
 * running benchmark table.
 */

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

export type NumericRange = { min: number; max: number };

export type RiskSignalBundle = {
  maintainerCount: number;
  stalenessYears: number;
  fanoutDirect: number;
  faninDependents: number;
  hasRepository: boolean;
};

export type RiskSignalRow = {
  package: Package;
  signals: RiskSignalBundle;
};

export type TopFanoutRow = { package_name: string; num_dependencies: number };
export type MostDependentsRow = { package_name: string; dependents: number };
export type TopMaintainerRow = { username: string; num_packages: number };

// ---------------------------------------------------------------------------
// Raw view-row shapes (what PostgREST returns before we map to our API types)
// ---------------------------------------------------------------------------

type VTopFanoutRow = {
  package_name: string | null;
  num_dependencies: number | string | null;
};

type VMostDependentsRow = {
  package_name: string | null;
  dependents: number | string | null;
};

type VTopMaintainersRow = {
  username: string | null;
  num_packages: number | string | null;
};

type VRiskSignalsRow = {
  package_id: string | null;
  package_name: string | null;
  ecosystem: Ecosystem | null;
  description: string | null;
  latest_version: string | null;
  maintainer_count: number | string | null;
  fanout_direct: number | string | null;
  fanin_dependents: number | string | null;
  latest_released: string | null;
  has_repository: boolean | null;
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function toNumber(value: number | string | null | undefined): number {
  if (value === null || value === undefined) return 0;
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : 0;
}

const MILLIS_PER_YEAR = 1000 * 60 * 60 * 24 * 365.25;

function stalenessYearsFrom(
  latestReleased: string | null,
  now: Date = new Date(),
): number {
  if (!latestReleased) return 0;
  const ts = Date.parse(latestReleased);
  if (!Number.isFinite(ts)) return 0;
  const diffMs = now.getTime() - ts;
  if (diffMs <= 0) return 0;
  return diffMs / MILLIS_PER_YEAR;
}

// ---------------------------------------------------------------------------
// R2 — top fan-out
// ---------------------------------------------------------------------------

export type TopFanoutArgs = {
  ecosystem: Ecosystem;
  limit: number;
};

export async function topFanout(
  client: SupabaseClient,
  { ecosystem, limit }: TopFanoutArgs,
): Promise<TopFanoutRow[]> {
  const { data, error } = await client
    .from("v_top_fanout")
    .select("package_name, num_dependencies")
    .eq("ecosystem", ecosystem)
    .order("num_dependencies", { ascending: false })
    .limit(limit);
  if (error) {
    throw new Error(`topFanout failed: ${error.message}`);
  }
  const rows = (data ?? []) as unknown as VTopFanoutRow[];
  return rows
    .filter((r): r is VTopFanoutRow & { package_name: string } => !!r.package_name)
    .map((r) => ({
      package_name: r.package_name,
      num_dependencies: toNumber(r.num_dependencies),
    }));
}

// ---------------------------------------------------------------------------
// R5 — most dependents
// ---------------------------------------------------------------------------

export type MostDependentsArgs = {
  ecosystem: Ecosystem;
  limit: number;
};

export async function mostDependents(
  client: SupabaseClient,
  { ecosystem, limit }: MostDependentsArgs,
): Promise<MostDependentsRow[]> {
  const { data, error } = await client
    .from("v_most_dependents")
    .select("package_name, dependents")
    .eq("ecosystem", ecosystem)
    .order("dependents", { ascending: false })
    .limit(limit);
  if (error) {
    throw new Error(`mostDependents failed: ${error.message}`);
  }
  const rows = (data ?? []) as unknown as VMostDependentsRow[];
  return rows
    .filter((r): r is VMostDependentsRow & { package_name: string } => !!r.package_name)
    .map((r) => ({
      package_name: r.package_name,
      dependents: toNumber(r.dependents),
    }));
}

// ---------------------------------------------------------------------------
// R6 — top maintainers
// ---------------------------------------------------------------------------

export type TopMaintainersArgs = {
  ecosystem: Ecosystem;
  limit: number;
};

export async function topMaintainers(
  client: SupabaseClient,
  { ecosystem, limit }: TopMaintainersArgs,
): Promise<TopMaintainerRow[]> {
  const { data, error } = await client
    .from("v_top_maintainers")
    .select("username, num_packages")
    .eq("ecosystem", ecosystem)
    .order("num_packages", { ascending: false })
    .limit(limit);
  if (error) {
    throw new Error(`topMaintainers failed: ${error.message}`);
  }
  const rows = (data ?? []) as unknown as VTopMaintainersRow[];
  return rows
    .filter((r): r is VTopMaintainersRow & { username: string } => !!r.username)
    .map((r) => ({
      username: r.username,
      num_packages: toNumber(r.num_packages),
    }));
}

// ---------------------------------------------------------------------------
// R10 — risk signals (bulk per-ecosystem)
// ---------------------------------------------------------------------------

export type FetchRiskSignalsArgs = {
  ecosystem: Ecosystem;
};

const RISK_SIGNALS_COLUMNS =
  "package_id, package_name, ecosystem, description, latest_version, " +
  "maintainer_count, fanout_direct, fanin_dependents, latest_released, has_repository";

export async function fetchRiskSignalsForEcosystem(
  client: SupabaseClient,
  { ecosystem }: FetchRiskSignalsArgs,
  now: Date = new Date(),
): Promise<RiskSignalRow[]> {
  const { data, error } = await client
    .from("v_risk_signals")
    .select(RISK_SIGNALS_COLUMNS)
    .eq("ecosystem", ecosystem);
  if (error) {
    throw new Error(`fetchRiskSignalsForEcosystem failed: ${error.message}`);
  }
  const rows = (data ?? []) as unknown as VRiskSignalsRow[];
  const out: RiskSignalRow[] = [];
  for (const row of rows) {
    if (!row.package_id || !row.package_name || !row.ecosystem) continue;
    const pkg: Package = {
      id: row.package_id,
      ecosystem: row.ecosystem,
      name: row.package_name,
      description: row.description ?? "",
      latest_version: row.latest_version ?? "",
    };
    out.push({
      package: pkg,
      signals: {
        maintainerCount: toNumber(row.maintainer_count),
        stalenessYears: stalenessYearsFrom(row.latest_released, now),
        fanoutDirect: toNumber(row.fanout_direct),
        faninDependents: toNumber(row.fanin_dependents),
        hasRepository: Boolean(row.has_repository),
      },
    });
  }
  return out;
}

// ---------------------------------------------------------------------------
// R10 — ranges for min-max normalization
// ---------------------------------------------------------------------------

export type FetchRiskRangesArgs = {
  ecosystem: Ecosystem;
};

export type RiskRanges = {
  maintainerCount: NumericRange;
  stalenessYears: NumericRange;
  fanoutDirect: NumericRange;
  faninDependents: NumericRange;
};

const EMPTY_RANGES: RiskRanges = {
  maintainerCount: { min: 0, max: 0 },
  stalenessYears: { min: 0, max: 0 },
  fanoutDirect: { min: 0, max: 0 },
  faninDependents: { min: 0, max: 0 },
};

/**
 * Fetch the ecosystem-wide MIN/MAX of each numeric signal. Implemented by
 * reading the signal columns from `v_risk_signals` and reducing in TS,
 * which keeps the view surface minimal and lets us reuse the same
 * timestamp -> staleness conversion used by the per-row fetcher.
 *
 * Rationale: PostgREST's aggregate support for `min()`/`max()` is
 * awkward to compose with the ecosystem filter, and the resulting
 * payload (one numeric column per package) is small — O(N) numbers on
 * the wire.
 */
export async function fetchRiskRangesForEcosystem(
  client: SupabaseClient,
  { ecosystem }: FetchRiskRangesArgs,
  now: Date = new Date(),
): Promise<RiskRanges> {
  const { data, error } = await client
    .from("v_risk_signals")
    .select(
      "maintainer_count, fanout_direct, fanin_dependents, latest_released",
    )
    .eq("ecosystem", ecosystem);
  if (error) {
    throw new Error(`fetchRiskRangesForEcosystem failed: ${error.message}`);
  }
  const rows = (data ?? []) as unknown as Array<
    Pick<
      VRiskSignalsRow,
      "maintainer_count" | "fanout_direct" | "fanin_dependents" | "latest_released"
    >
  >;
  if (rows.length === 0) {
    return EMPTY_RANGES;
  }

  let mcMin = Number.POSITIVE_INFINITY;
  let mcMax = Number.NEGATIVE_INFINITY;
  let syMin = Number.POSITIVE_INFINITY;
  let syMax = Number.NEGATIVE_INFINITY;
  let foMin = Number.POSITIVE_INFINITY;
  let foMax = Number.NEGATIVE_INFINITY;
  let fiMin = Number.POSITIVE_INFINITY;
  let fiMax = Number.NEGATIVE_INFINITY;

  for (const row of rows) {
    const mc = toNumber(row.maintainer_count);
    const sy = stalenessYearsFrom(row.latest_released, now);
    const fo = toNumber(row.fanout_direct);
    const fi = toNumber(row.fanin_dependents);
    if (mc < mcMin) mcMin = mc;
    if (mc > mcMax) mcMax = mc;
    if (sy < syMin) syMin = sy;
    if (sy > syMax) syMax = sy;
    if (fo < foMin) foMin = fo;
    if (fo > foMax) foMax = fo;
    if (fi < fiMin) fiMin = fi;
    if (fi > fiMax) fiMax = fi;
  }

  return {
    maintainerCount: { min: mcMin, max: mcMax },
    stalenessYears: { min: syMin, max: syMax },
    fanoutDirect: { min: foMin, max: foMax },
    faninDependents: { min: fiMin, max: fiMax },
  };
}
