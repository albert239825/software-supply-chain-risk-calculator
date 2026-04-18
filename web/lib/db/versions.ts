import type { SupabaseClient } from "@supabase/supabase-js";

import type { Ecosystem, UUID, Version } from "@/types/api";

import { getPackageById } from "./packages";

/**
 * Typed Supabase query helpers for the `versions` table.
 *
 * Column names match the clean Supabase schema documented in
 * `src/clean_data.py`: `id, package_id, ecosystem, package_name, version,
 * released, has_repository, github_owner, github_repo`.
 */

const VERSION_COLUMNS =
  "id, package_id, version, released, has_repository, github_owner, github_repo";

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
 * Backs R1 `GET /api/packages/:packageId/versions` (Q1). Orders by
 * `released DESC NULLS LAST` and returns the ecosystem-wide `total` so the
 * caller can populate `ListMeta.total` in a single round trip via PostgREST's
 * `count: 'exact'`.
 */
export async function listVersionsByPackage(
  client: SupabaseClient,
  { packageId, ecosystem, limit, offset }: ListVersionsByPackageArgs,
): Promise<{ items: Version[]; total: number }> {
  const { data, count, error } = await client
    .from("versions")
    .select(VERSION_COLUMNS, { count: "exact" })
    .eq("package_id", packageId)
    .eq("ecosystem", ecosystem)
    .order("released", { ascending: false, nullsFirst: false })
    .range(offset, offset + limit - 1);
  if (error) {
    throw new Error(`listVersionsByPackage failed: ${error.message}`);
  }
  return {
    items: (data ?? []) as Version[],
    total: count ?? 0,
  };
}

export type GetLatestVersionRecordArgs = {
  packageId: UUID;
  ecosystem: Ecosystem;
  /**
   * When the caller already has the owning `Package`'s `latest_version`
   * string, pass it here to skip the extra round trip to `packages`.
   */
  latestVersion?: string;
};

/**
 * Returns the `versions` row whose `version` matches the owning package's
 * `latest_version`. Returns `null` when either the package has no recorded
 * latest version or no matching version row exists. Needed by A2 and the
 * risk-signal extractor feeding A6/R10.
 */
export async function getLatestVersionRecord(
  client: SupabaseClient,
  { packageId, ecosystem, latestVersion }: GetLatestVersionRecordArgs,
): Promise<Version | null> {
  let version = latestVersion;
  if (version === undefined) {
    const pkg = await getPackageById(client, { id: packageId, ecosystem });
    if (!pkg) return null;
    version = pkg.latest_version;
  }
  if (!version) return null;

  const { data, error } = await client
    .from("versions")
    .select(VERSION_COLUMNS)
    .eq("package_id", packageId)
    .eq("ecosystem", ecosystem)
    .eq("version", version)
    .maybeSingle();
  if (error) {
    throw new Error(`getLatestVersionRecord failed: ${error.message}`);
  }
  return (data as Version | null) ?? null;
}

/**
 * Simple row shape returned by `listVersionsMissingRepo` — matches Q9's
 * output (`SELECT p.name AS package_name, v.version`). Not a `Version`.
 */
export type VersionMissingRepoRow = {
  package_name: string;
  version: string;
};

export type ListVersionsMissingRepoArgs = {
  ecosystem: Ecosystem;
  limit: number;
  offset: number;
};

/**
 * Backs R9 `GET /api/packages/no-repo` (Q9). The filter mirrors the SQL
 * exactly — any version whose `has_repository` is null/empty or lowercases
 * to one of `'false' | '0' | 'no'` is considered missing a repository.
 */
export async function listVersionsMissingRepo(
  client: SupabaseClient,
  { ecosystem, limit, offset }: ListVersionsMissingRepoArgs,
): Promise<{ items: VersionMissingRepoRow[]; total: number }> {
  // PostgREST `or()` accepts a comma-separated list of conditions and we OR
  // them together. We enumerate the common casings clean_data.py could
  // produce (`true`/`false` lowercased) plus the other literals Q9 treats
  // as "missing". `has_repository.eq.` matches the empty string; `.is.null`
  // matches SQL NULL.
  const missingRepoFilter = [
    "has_repository.is.null",
    "has_repository.eq.",
    "has_repository.eq.false",
    "has_repository.eq.False",
    "has_repository.eq.FALSE",
    "has_repository.eq.0",
    "has_repository.eq.no",
    "has_repository.eq.No",
    "has_repository.eq.NO",
  ].join(",");

  const { data, count, error } = await client
    .from("versions")
    .select("package_name, version", { count: "exact" })
    .eq("ecosystem", ecosystem)
    .or(missingRepoFilter)
    .order("package_name", { ascending: true })
    .order("version", { ascending: true })
    .range(offset, offset + limit - 1);
  if (error) {
    throw new Error(`listVersionsMissingRepo failed: ${error.message}`);
  }
  return {
    items: (data ?? []) as VersionMissingRepoRow[],
    total: count ?? 0,
  };
}
