import type { SupabaseClient } from "@supabase/supabase-js";

import type { Ecosystem, Package, UUID } from "@/types/api";

/**
 * Typed Supabase query helpers for the `packages` table.
 *
 * Every function takes the server client returned by
 * `createSupabaseServerClient()` (see `web/lib/supabase.ts`) as its first
 * argument so it is testable without module-level side effects.
 *
 * Column names match the clean Supabase schema documented in
 * `src/clean_data.py`: `id, ecosystem, name, description, latest_version`.
 */

const PACKAGE_COLUMNS = "id, ecosystem, name, description, latest_version";

export type SearchPackagesByNameArgs = {
  q: string;
  ecosystem: Ecosystem;
  limit: number;
};

/**
 * A1 search helper. Case-insensitive substring match against `packages.name`,
 * limited to the given ecosystem. Results ordered by name ascending for a
 * stable autocomplete ordering.
 */
export async function searchPackagesByName(
  client: SupabaseClient,
  { q, ecosystem, limit }: SearchPackagesByNameArgs,
): Promise<Package[]> {
  const pattern = `%${escapeLikePattern(q)}%`;
  const { data, error } = await client
    .from("packages")
    .select(PACKAGE_COLUMNS)
    .eq("ecosystem", ecosystem)
    .ilike("name", pattern)
    .order("name", { ascending: true })
    .limit(limit);
  if (error) {
    throw new Error(`searchPackagesByName failed: ${error.message}`);
  }
  return (data ?? []) as Package[];
}

export type GetPackageByIdArgs = {
  id: UUID;
  ecosystem: Ecosystem;
};

/**
 * Fetch a single package row by its canonical UUID id. Returns `null` when
 * the row does not exist. Other errors are re-thrown so the route handler
 * can map them to an HTTP error response.
 */
export async function getPackageById(
  client: SupabaseClient,
  { id, ecosystem }: GetPackageByIdArgs,
): Promise<Package | null> {
  const { data, error } = await client
    .from("packages")
    .select(PACKAGE_COLUMNS)
    .eq("id", id)
    .eq("ecosystem", ecosystem)
    .maybeSingle();
  if (error) {
    throw new Error(`getPackageById failed: ${error.message}`);
  }
  return (data as Package | null) ?? null;
}

export type CountByEcosystemArgs = { ecosystem: Ecosystem };

/**
 * Exact-count helper using PostgREST's `count: 'exact', head: true` pattern,
 * which avoids transferring the row bodies over the wire.
 */
export async function countPackages(
  client: SupabaseClient,
  { ecosystem }: CountByEcosystemArgs,
): Promise<number> {
  const { count, error } = await client
    .from("packages")
    .select("id", { count: "exact", head: true })
    .eq("ecosystem", ecosystem);
  if (error) {
    throw new Error(`countPackages failed: ${error.message}`);
  }
  return count ?? 0;
}

/**
 * Escape the ILIKE wildcard characters `%` and `_` so user-provided search
 * text is treated as a literal substring.
 */
function escapeLikePattern(input: string): string {
  return input.replace(/[\\%_]/g, (ch) => `\\${ch}`);
}
