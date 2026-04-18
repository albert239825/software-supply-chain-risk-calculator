import { describe, expect, it } from "vitest";

import {
  getLatestVersionRecord,
  listVersionsByPackage,
  listVersionsMissingRepo,
} from "@/lib/db/versions";
import type { Version } from "@/types/api";

import {
  createFakeSupabase,
  createFakeSupabaseSequence,
} from "./_fake-supabase";

const PKG_ID = "11111111-1111-1111-1111-111111111111";

const VERSION_ROW: Version = {
  id: "22222222-2222-2222-2222-222222222222",
  package_id: PKG_ID,
  version: "1.2.3",
  released: "2025-01-14T09:23:11Z",
  has_repository: true,
  github_owner: "owner",
  github_repo: "repo",
};

describe("listVersionsByPackage", () => {
  it("returns items + ecosystem-wide total and records the expected method chain", async () => {
    const { client, calls } = createFakeSupabase(() => ({
      data: [VERSION_ROW],
      count: 42,
    }));

    const result = await listVersionsByPackage(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
      limit: 50,
      offset: 0,
    });

    expect(result).toEqual({ items: [VERSION_ROW], total: 42 });
    expect(calls).toHaveLength(1);
    const ops = calls[0].ops.map((op) => op[0]);
    expect(calls[0].table).toBe("versions");
    expect(ops).toContain("select");
    expect(ops).toContain("eq");
    expect(ops).toContain("order");
    expect(ops).toContain("range");
    // `count: 'exact'` signals PostgREST to return the total
    const selectOp = calls[0].ops.find((op) => op[0] === "select");
    expect(selectOp?.[2]).toEqual({ count: "exact" });
    // `released DESC NULLS LAST`
    const orderOp = calls[0].ops.find((op) => op[0] === "order");
    expect(orderOp?.[1]).toBe("released");
    expect(orderOp?.[2]).toMatchObject({ ascending: false, nullsFirst: false });
  });

  it("throws a labeled error when PostgREST returns one", async () => {
    const { client } = createFakeSupabase(() => ({
      error: { message: "boom" },
    }));
    await expect(
      listVersionsByPackage(client, {
        packageId: PKG_ID,
        ecosystem: "npm",
        limit: 50,
        offset: 0,
      }),
    ).rejects.toThrow(/listVersionsByPackage failed: boom/);
  });
});

describe("getLatestVersionRecord", () => {
  it("uses the provided latestVersion and skips the packages round trip", async () => {
    const { client, calls } = createFakeSupabase(() => ({ data: VERSION_ROW }));

    const result = await getLatestVersionRecord(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
      latestVersion: "1.2.3",
    });

    expect(result).toEqual(VERSION_ROW);
    expect(calls).toHaveLength(1);
    expect(calls[0].table).toBe("versions");
    const ops = calls[0].ops.map((op) => op[0]);
    expect(ops).toContain("maybeSingle");
  });

  it("falls back to a packages lookup when latestVersion is not provided", async () => {
    const { client, calls } = createFakeSupabaseSequence([
      { data: { id: PKG_ID, ecosystem: "npm", name: "p", description: "", latest_version: "9.9.9" } },
      { data: { ...VERSION_ROW, version: "9.9.9" } },
    ]);

    const result = await getLatestVersionRecord(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
    });

    expect(result?.version).toBe("9.9.9");
    expect(calls.map((c) => c.table)).toEqual(["packages", "versions"]);
  });

  it("returns null when the package does not exist", async () => {
    const { client } = createFakeSupabaseSequence([{ data: null }]);

    const result = await getLatestVersionRecord(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
    });
    expect(result).toBeNull();
  });

  it("returns null when the package has an empty latest_version string", async () => {
    const { client, calls } = createFakeSupabase(() => ({ data: null }));

    const result = await getLatestVersionRecord(client, {
      packageId: PKG_ID,
      ecosystem: "npm",
      latestVersion: "",
    });
    expect(result).toBeNull();
    expect(calls).toHaveLength(0); // short-circuits without a DB roundtrip
  });

  it("throws a labeled error when PostgREST fails", async () => {
    const { client } = createFakeSupabase(() => ({
      error: { message: "kaboom" },
    }));
    await expect(
      getLatestVersionRecord(client, {
        packageId: PKG_ID,
        ecosystem: "npm",
        latestVersion: "1.0.0",
      }),
    ).rejects.toThrow(/getLatestVersionRecord failed: kaboom/);
  });
});

describe("listVersionsMissingRepo", () => {
  it("returns simple { package_name, version } rows and applies the Q9 filter", async () => {
    const rows = [
      { package_name: "a", version: "1.0.0" },
      { package_name: "b", version: "0.1.2" },
    ];
    const { client, calls } = createFakeSupabase(() => ({
      data: rows,
      count: 17203,
    }));

    const result = await listVersionsMissingRepo(client, {
      ecosystem: "npm",
      limit: 100,
      offset: 0,
    });

    expect(result).toEqual({ items: rows, total: 17203 });
    expect(calls).toHaveLength(1);
    const orOp = calls[0].ops.find((op) => op[0] === "or");
    const orFilter = orOp?.[1] as string;
    // Q9 alternates
    expect(orFilter).toContain("has_repository.is.null");
    expect(orFilter).toContain("has_repository.eq.false");
    expect(orFilter).toContain("has_repository.eq.0");
    expect(orFilter).toContain("has_repository.eq.no");
    expect(orFilter).toContain("has_repository.eq.");
  });

  it("throws a labeled error when PostgREST fails", async () => {
    const { client } = createFakeSupabase(() => ({
      error: { message: "nope" },
    }));
    await expect(
      listVersionsMissingRepo(client, {
        ecosystem: "npm",
        limit: 100,
        offset: 0,
      }),
    ).rejects.toThrow(/listVersionsMissingRepo failed: nope/);
  });
});
