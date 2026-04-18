import { describe, expect, it } from "vitest";

import {
  listDependents,
  listDirectDependencies,
  type DependentRow,
  type DirectDependencyRow,
} from "@/lib/db/dependencies";

import {
  createFakeSupabase,
  createFakeSupabaseSequence,
} from "./_fake-supabase";

const PKG_ID = "11111111-1111-1111-1111-111111111111";
const VERSION_ID = "22222222-2222-2222-2222-222222222222";

const DIRECT_ROWS: DirectDependencyRow[] = [
  { to_package_id: "to-1", to_package: "cookie", version_spec: "0.4.2", dep_kind: "dependency" },
  { to_package_id: "to-2", to_package: "debug", version_spec: "^4", dep_kind: "peer" },
];

describe("listDirectDependencies", () => {
  it("skips the getLatestVersionRecord roundtrip when fromVersionId is provided", async () => {
    const { client, calls } = createFakeSupabase(() => ({
      data: DIRECT_ROWS,
      count: 2,
    }));

    const result = await listDirectDependencies(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
      depKind: "all",
      limit: 100,
      offset: 0,
      fromVersionId: VERSION_ID,
    });

    expect(result).toEqual({ items: DIRECT_ROWS, total: 2 });
    expect(calls).toHaveLength(1);
    expect(calls[0].table).toBe("dependencies");
    // no dep_kind filter when depKind = 'all'
    const eqOps = calls[0].ops.filter((op) => op[0] === "eq");
    expect(eqOps.some((op) => op[1] === "dep_kind")).toBe(false);
    expect(eqOps.some((op) => op[1] === "from_version_id" && op[2] === VERSION_ID)).toBe(
      true,
    );
  });

  it("applies the dep_kind filter when a specific kind is requested", async () => {
    const { client, calls } = createFakeSupabase(() => ({
      data: [DIRECT_ROWS[0]],
      count: 1,
    }));

    await listDirectDependencies(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
      depKind: "peer",
      limit: 100,
      offset: 0,
      fromVersionId: VERSION_ID,
    });

    const eqOps = calls[0].ops.filter((op) => op[0] === "eq");
    expect(eqOps).toContainEqual(["eq", "dep_kind", "peer"]);
  });

  it("returns an empty page when the package has no latest version row", async () => {
    // packages returns row, versions returns nothing -> getLatestVersionRecord -> null
    const { client } = createFakeSupabaseSequence([
      { data: { id: PKG_ID, ecosystem: "npm", name: "p", description: "", latest_version: "1.0.0" } },
      { data: null },
    ]);

    const result = await listDirectDependencies(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
      depKind: "all",
      limit: 100,
      offset: 0,
    });
    expect(result).toEqual({ items: [], total: 0 });
  });

  it("throws a labeled error when PostgREST fails", async () => {
    const { client } = createFakeSupabase(() => ({
      error: { message: "boom" },
    }));
    await expect(
      listDirectDependencies(client, {
        packageId: PKG_ID,
        ecosystem: "npm",
        depKind: "all",
        limit: 100,
        offset: 0,
        fromVersionId: VERSION_ID,
      }),
    ).rejects.toThrow(/listDirectDependencies failed: boom/);
  });
});

describe("listDependents", () => {
  it("dedupes identical (from_package, from_version, dep_kind) edges", async () => {
    const rows: DependentRow[] = [
      { from_package_id: "a", from_package: "body-parser", from_version: "1.20.0", dep_kind: "dependency" },
      { from_package_id: "a", from_package: "body-parser", from_version: "1.20.0", dep_kind: "dependency" },
      { from_package_id: "b", from_package: "connect", from_version: "3.7.0", dep_kind: "dependency" },
    ];
    const { client, calls } = createFakeSupabase(() => ({
      data: rows,
      count: rows.length,
    }));

    const result = await listDependents(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
      limit: 100,
      offset: 0,
    });

    expect(result.items).toHaveLength(2);
    // total reflects the raw row count (pre-dedupe), matching helper docs
    expect(result.total).toBe(3);
    expect(calls[0].table).toBe("dependencies");
    const eqOps = calls[0].ops.filter((op) => op[0] === "eq");
    expect(eqOps).toContainEqual(["eq", "to_package_id", PKG_ID]);
    expect(eqOps).toContainEqual(["eq", "ecosystem", "npm"]);
  });

  it("throws a labeled error when PostgREST fails", async () => {
    const { client } = createFakeSupabase(() => ({
      error: { message: "bust" },
    }));
    await expect(
      listDependents(client, {
        packageId: PKG_ID,
        ecosystem: "npm",
        limit: 100,
        offset: 0,
      }),
    ).rejects.toThrow(/listDependents failed: bust/);
  });
});
