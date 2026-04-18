import type { SupabaseClient } from "@supabase/supabase-js";

import type { Ecosystem, Maintainer, UUID } from "@/types/api";

/**
 * Typed Supabase query helpers for the `maintainers` table.
 *
 * Phase 0 provides `countMaintainers`. `listMaintainersForPackage` is a
 * Phase 1 stub (backs A3).
 */

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
 * Phase 1 stub — backs A3 `GET /api/packages/:packageId/maintainers`. Should
 * dedupe by `username` within the package.
 */
export async function listMaintainersForPackage(
  _client: SupabaseClient,
  _args: ListMaintainersForPackageArgs,
): Promise<{ items: Maintainer[]; total: number }> {
  throw new Error("not implemented");
}
