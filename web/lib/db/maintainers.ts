import type { SupabaseClient } from "@supabase/supabase-js";

import type { Ecosystem, Maintainer, UUID } from "@/types/api";

/**
 * Typed Supabase query helpers for the `maintainers` table.
 *
 * Column names match the clean Supabase schema documented in
 * `src/clean_data.py`: `id, ecosystem, package_name, package_id, username,
 * name, role, email`.
 */

const MAINTAINER_COLUMNS = "id, package_id, username, name, role, email";

export type CountByEcosystemArgs = { ecosystem: Ecosystem };

export async function countMaintainers(
  client: SupabaseClient,
  { ecosystem }: CountByEcosystemArgs,
): Promise<number> {
  const { count, error } = await client
    .from("maintainers")
    .select("id", { count: "exact", head: true })
    .eq("ecosystem", ecosystem);
  if (error) {
    throw new Error(`countMaintainers failed: ${error.message}`);
  }
  return count ?? 0;
}

export type ListMaintainersForPackageArgs = {
  packageId: UUID;
  ecosystem: Ecosystem;
  limit: number;
  offset: number;
};

/**
 * Backs A3 `GET /api/packages/:packageId/maintainers`. Dedupes by
 * `username` within the package (keeping the first row per username),
 * since PostgREST does not expose `SELECT DISTINCT ON (...)`. Maintainer
 * lists per package are small in practice, so we fetch the full set, dedupe
 * in-memory, and then slice to the requested page. `total` is the deduped
 * row count so pagination meta is consistent with the returned items.
 */
export async function listMaintainersForPackage(
  client: SupabaseClient,
  { packageId, ecosystem, limit, offset }: ListMaintainersForPackageArgs,
): Promise<{ items: Maintainer[]; total: number }> {
  const { data, error } = await client
    .from("maintainers")
    .select(MAINTAINER_COLUMNS)
    .eq("package_id", packageId)
    .eq("ecosystem", ecosystem)
    .order("username", { ascending: true });
  if (error) {
    throw new Error(`listMaintainersForPackage failed: ${error.message}`);
  }

  const rows = (data ?? []) as Maintainer[];
  const seen = new Set<string>();
  const deduped: Maintainer[] = [];
  for (const row of rows) {
    if (seen.has(row.username)) continue;
    seen.add(row.username);
    deduped.push(row);
  }

  const page = deduped.slice(offset, offset + limit);
  return { items: page, total: deduped.length };
}
