import type { SupabaseClient } from "@supabase/supabase-js";

import type { Ecosystem, UUID } from "@/types/api";

/**
 * Typed Supabase query helpers for the `dependencies` table.
 *
 * Phase 0 provides `countDependencies`. `listDirectDependencies` and
 * `listDependents` are reserved as not-implemented stubs for Phase 1
 * (A4, A5, Q2, Q5).
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
};

/**
 * Phase 1 stub — backs A4 `GET /api/packages/:packageId/dependencies`. Joins
 * `packages -> versions (latest) -> dependencies` and applies the optional
 * `depKind` filter.
 */
export async function listDirectDependencies(
  _client: SupabaseClient,
  _args: ListDirectDependenciesArgs,
): Promise<{ items: DirectDependencyRow[]; total: number }> {
  throw new Error("not implemented");
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
 * Phase 1 stub — backs A5 `GET /api/packages/:packageId/dependents` and
 * reuses Q5's aggregation for R5.
 */
export async function listDependents(
  _client: SupabaseClient,
  _args: ListDependentsArgs,
): Promise<{ items: DependentRow[]; total: number }> {
  throw new Error("not implemented");
}
