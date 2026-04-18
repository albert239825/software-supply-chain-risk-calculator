import type { SupabaseClient } from "@supabase/supabase-js";

import type { Ecosystem, UUID } from "@/types/api";

import { getLatestVersionRecord } from "./versions";

/**
 * Typed Supabase query helpers for the `dependencies` table.
 *
 * Column names match the clean Supabase schema documented in
 * `src/clean_data.py`: `id, ecosystem, from_package, from_version,
 * to_package, version_spec, dep_kind, from_version_id, to_package_id`.
 */

export type CountByEcosystemArgs = { ecosystem: Ecosystem };

export async function countDependencies(
  client: SupabaseClient,
  { ecosystem }: CountByEcosystemArgs,
): Promise<number> {
  const { count, error } = await client
    .from("dependencies")
    .select("id", { count: "exact", head: true })
    .eq("ecosystem", ecosystem);
  if (error) {
    throw new Error(`countDependencies failed: ${error.message}`);
  }
  return count ?? 0;
}

export type DepKind = "dependency" | "peer" | "optional" | "all";

export type DirectDependencyRow = {
  to_package_id: UUID;
  to_package: string;
  version_spec: string;
  dep_kind: string;
};

export type ListDirectDependenciesArgs = {
  packageId: UUID;
  ecosystem: Ecosystem;
  depKind: DepKind;
  limit: number;
  offset: number;
  /**
   * When the caller already resolved the package's latest `versions.id`
   * (e.g. via `getLatestVersionRecord` for a 404 check), pass it here to
   * skip the extra round trip.
   */
  fromVersionId?: UUID;
};

/**
 * Backs A4 `GET /api/packages/:packageId/dependencies`. Resolves the
 * package's latest version first (`getLatestVersionRecord`) and then reads
 * rows of `dependencies` where `from_version_id` matches. Applies an
 * optional `dep_kind` filter (`'all'` means no filter).
 *
 * Returns `{ items: [], total: 0 }` when the package has no recorded latest
 * version — the route handler surfaces a 404 for that case separately.
 */
export async function listDirectDependencies(
  client: SupabaseClient,
  {
    packageId,
    ecosystem,
    depKind,
    limit,
    offset,
    fromVersionId,
  }: ListDirectDependenciesArgs,
): Promise<{ items: DirectDependencyRow[]; total: number }> {
  let versionId = fromVersionId;
  if (versionId === undefined) {
    const latest = await getLatestVersionRecord(client, {
      packageId,
      ecosystem,
    });
    if (!latest) return { items: [], total: 0 };
    versionId = latest.id;
  }

  let query = client
    .from("dependencies")
    .select("to_package_id, to_package, version_spec, dep_kind", {
      count: "exact",
    })
    .eq("from_version_id", versionId)
    .eq("ecosystem", ecosystem);

  if (depKind !== "all") {
    query = query.eq("dep_kind", depKind);
  }

  const { data, count, error } = await query
    .order("to_package", { ascending: true })
    .range(offset, offset + limit - 1);
  if (error) {
    throw new Error(`listDirectDependencies failed: ${error.message}`);
  }
  return {
    items: (data ?? []) as DirectDependencyRow[],
    total: count ?? 0,
  };
}

export type DependentRow = {
  from_package_id: UUID | null;
  from_package: string;
  from_version: string;
  dep_kind: string;
};

export type ListDependentsArgs = {
  packageId: UUID;
  ecosystem: Ecosystem;
  limit: number;
  offset: number;
};

/**
 * Backs A5 `GET /api/packages/:packageId/dependents`. Identical edges from
 * the same `(from_package, from_version, dep_kind)` that happen to span
 * multiple `from_version_id`s are deduped in-memory on the returned page —
 * PostgREST does not expose `SELECT DISTINCT ON (...)`. The raw row count
 * (pre-dedupe) is returned as `total`, matching the behavior of every other
 * list helper in this module.
 */
export async function listDependents(
  client: SupabaseClient,
  { packageId, ecosystem, limit, offset }: ListDependentsArgs,
): Promise<{ items: DependentRow[]; total: number }> {
  const { data, count, error } = await client
    .from("dependencies")
    .select("from_package_id, from_package, from_version, dep_kind", {
      count: "exact",
    })
    .eq("to_package_id", packageId)
    .eq("ecosystem", ecosystem)
    .order("from_package", { ascending: true })
    .order("from_version", { ascending: true })
    .order("dep_kind", { ascending: true })
    .range(offset, offset + limit - 1);
  if (error) {
    throw new Error(`listDependents failed: ${error.message}`);
  }

  const rows = (data ?? []) as DependentRow[];
  const seen = new Set<string>();
  const deduped: DependentRow[] = [];
  for (const row of rows) {
    const key = `${row.from_package}\u001f${row.from_version}\u001f${row.dep_kind}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(row);
  }

  return { items: deduped, total: count ?? 0 };
}
