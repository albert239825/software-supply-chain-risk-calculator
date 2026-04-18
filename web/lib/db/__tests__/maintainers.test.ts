import { describe, expect, it } from "vitest";

import { listMaintainersForPackage } from "@/lib/db/maintainers";
import type { Maintainer } from "@/types/api";

import { createFakeSupabase } from "./_fake-supabase";

const PKG_ID = "11111111-1111-1111-1111-111111111111";

function m(partial: Partial<Maintainer>): Maintainer {
  return {
    id: partial.id ?? "id",
    package_id: partial.package_id ?? PKG_ID,
    username: partial.username ?? "u",
    name: partial.name ?? null,
    role: partial.role ?? null,
    email: partial.email ?? null,
  };
}

describe("listMaintainersForPackage", () => {
  it("dedupes by username (keeping first occurrence) and returns a sliced page", async () => {
    const rows: Maintainer[] = [
      m({ id: "a1", username: "alice", role: "owner" }),
      m({ id: "b1", username: "bob", role: "maintainer" }),
      m({ id: "a2", username: "alice", role: "maintainer" }),
      m({ id: "c1", username: "carol" }),
    ];
    const { client, calls } = createFakeSupabase(() => ({ data: rows }));

    const result = await listMaintainersForPackage(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
      limit: 100,
      offset: 0,
    });

    // alice, bob, carol in order; first alice kept (role: owner)
    expect(result.items.map((row) => row.username)).toEqual(["alice", "bob", "carol"]);
    expect(result.items[0].role).toBe("owner");
    expect(result.total).toBe(3);
    expect(calls[0].table).toBe("maintainers");
    const eqOps = calls[0].ops.filter((op) => op[0] === "eq");
    expect(eqOps).toContainEqual(["eq", "package_id", PKG_ID]);
    expect(eqOps).toContainEqual(["eq", "ecosystem", "npm"]);
  });

  it("respects pagination offset + limit after dedupe", async () => {
    const rows: Maintainer[] = [
      m({ id: "1", username: "a" }),
      m({ id: "2", username: "b" }),
      m({ id: "3", username: "c" }),
      m({ id: "4", username: "d" }),
    ];
    const { client } = createFakeSupabase(() => ({ data: rows }));

    const result = await listMaintainersForPackage(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
      limit: 2,
      offset: 1,
    });

    expect(result.items.map((row) => row.username)).toEqual(["b", "c"]);
    expect(result.total).toBe(4);
  });

  it("throws a labeled error when PostgREST fails", async () => {
    const { client } = createFakeSupabase(() => ({
      error: { message: "sad" },
    }));
    await expect(
      listMaintainersForPackage(client, {
        packageId: PKG_ID,
        ecosystem: "npm",
        limit: 100,
        offset: 0,
      }),
    ).rejects.toThrow(/listMaintainersForPackage failed: sad/);
  });
});
