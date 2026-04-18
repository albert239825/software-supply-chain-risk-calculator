import type { SupabaseClient } from "@supabase/supabase-js";

import type { Ecosystem, UUID, Version } from "@/types/api";

/**
 * Typed Supabase query helpers for the `versions` table.
 *
 * Phase 0 provides `countVersions` only. `listVersionsByPackage` and
 * `getLatestVersionRecord` are exported as not-implemented stubs with
 * finalized signatures so Phase 1 sub-devins can implement them without
 * reshaping the module's imports.
 */

export type CountByEcosystemArgs = { ecosystem: Ecosystem };

export async function countVersions(
  client: SupabaseClient,
  { ecosystem }: CountByEcosystemArgs,
): Promise<number> {
  const { count, error } = await client
    .from("versions")
    .select("id", { count: "exact", head: true })
    .eq("ecosystem", ecosystem);
  if (error) {
    throw new Error(`countVersions failed: ${error.message}`);
  }
  return count ?? 0;
}

export type ListVersionsByPackageArgs = {
  packageId: UUID;
  ecosystem: Ecosystem;
  limit: number;
  offset: number;
};

/**
 * Phase 1 stub — backs R1 `GET /api/packages/:packageId/versions` (Q1).
 * Must return versions ordered by `released DESC NULLS LAST`.
 */
export async function listVersionsByPackage(
  _client: SupabaseClient,
  _args: ListVersionsByPackageArgs,
): Promise<{ items: Version[]; total: number }> {
  throw new Error("not implemented");
}

export type GetLatestVersionRecordArgs = {
  packageId: UUID;
  ecosystem: Ecosystem;
};

/**
 * Phase 1 stub — returns the `versions` row whose `version` matches the
 * owning package's `latest_version`. Needed by A2 and the risk-signal
 * extractor feeding A6/R10.
 */
export async function getLatestVersionRecord(
  _client: SupabaseClient,
  _args: GetLatestVersionRecordArgs,
): Promise<Version | null> {
  throw new Error("not implemented");
}
