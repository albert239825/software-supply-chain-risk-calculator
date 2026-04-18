import type { SupabaseClient } from "@supabase/supabase-js";
import { describe, expect, it } from "vitest";

import {
  fetchRiskRangesForEcosystem,
  fetchRiskSignalsForEcosystem,
  mostDependents,
  topFanout,
  topMaintainers,
} from "@/lib/db/rankings";

type RecordedCall = [string, ...unknown[]];

type FakeResponse = {
  data: unknown;
  error: { message: string } | null;
};

/**
 * A minimal fake Supabase query builder that records the chained method
 * calls and resolves to a prebuilt `{ data, error }` when awaited. Casting
 * through `unknown` avoids committing to the real `SupabaseClient` type
 * surface in tests — helpers only ever use `.from().select().eq()...` so
 * the subset below is sufficient.
 */
function fakeClient(response: FakeResponse): {
  client: SupabaseClient;
  calls: RecordedCall[];
} {
  const calls: RecordedCall[] = [];
  const chain = {
    from(table: string) {
      calls.push(["from", table]);
      return this;
    },
    select(columns: string) {
      calls.push(["select", columns]);
      return this;
    },
    eq(column: string, value: unknown) {
      calls.push(["eq", column, value]);
      return this;
    },
    order(column: string, opts: unknown) {
      calls.push(["order", column, opts]);
      return this;
    },
    limit(n: number) {
      calls.push(["limit", n]);
      return this;
    },
    then<R1, R2>(
      onFulfilled: (value: FakeResponse) => R1,
      onRejected?: (reason: unknown) => R2,
    ) {
      return Promise.resolve(response).then(onFulfilled, onRejected);
    },
  };
  return { client: chain as unknown as SupabaseClient, calls };
}

describe("topFanout", () => {
  it("issues the expected PostgREST call and maps rows", async () => {
    const { client, calls } = fakeClient({
      data: [
        { package_name: "webpack", num_dependencies: 34 },
        { package_name: "next", num_dependencies: "21" },
      ],
      error: null,
    });

    const rows = await topFanout(client, { ecosystem: "npm", limit: 10 });

    expect(rows).toEqual([
      { package_name: "webpack", num_dependencies: 34 },
      { package_name: "next", num_dependencies: 21 },
    ]);
    expect(calls).toEqual([
      ["from", "v_top_fanout"],
      ["select", "package_name, num_dependencies"],
      ["eq", "ecosystem", "npm"],
      ["order", "num_dependencies", { ascending: false }],
      ["limit", 10],
    ]);
  });

  it("throws a labelled error on PostgREST failure", async () => {
    const { client } = fakeClient({
      data: null,
      error: { message: "relation v_top_fanout does not exist" },
    });
    await expect(
      topFanout(client, { ecosystem: "npm", limit: 10 }),
    ).rejects.toThrow(/topFanout failed: relation v_top_fanout/);
  });
});

describe("mostDependents", () => {
  it("reads from v_most_dependents ordered by dependents DESC", async () => {
    const { client, calls } = fakeClient({
      data: [
        { package_name: "tslib", dependents: 1843 },
        { package_name: "react", dependents: 900 },
      ],
      error: null,
    });

    const rows = await mostDependents(client, { ecosystem: "npm", limit: 5 });

    expect(rows).toEqual([
      { package_name: "tslib", dependents: 1843 },
      { package_name: "react", dependents: 900 },
    ]);
    expect(calls[0]).toEqual(["from", "v_most_dependents"]);
    expect(calls).toContainEqual([
      "order",
      "dependents",
      { ascending: false },
    ]);
    expect(calls).toContainEqual(["limit", 5]);
  });

  it("throws with the helper name on error", async () => {
    const { client } = fakeClient({
      data: null,
      error: { message: "timeout" },
    });
    await expect(
      mostDependents(client, { ecosystem: "npm", limit: 5 }),
    ).rejects.toThrow(/mostDependents failed: timeout/);
  });
});

describe("topMaintainers", () => {
  it("reads from v_top_maintainers ordered by num_packages DESC", async () => {
    const { client, calls } = fakeClient({
      data: [
        { username: "sindresorhus", num_packages: 917 },
        { username: "alice", num_packages: 12 },
      ],
      error: null,
    });

    const rows = await topMaintainers(client, {
      ecosystem: "npm",
      limit: 10,
    });

    expect(rows).toEqual([
      { username: "sindresorhus", num_packages: 917 },
      { username: "alice", num_packages: 12 },
    ]);
    expect(calls[0]).toEqual(["from", "v_top_maintainers"]);
    expect(calls).toContainEqual([
      "order",
      "num_packages",
      { ascending: false },
    ]);
  });

  it("throws with the helper name on error", async () => {
    const { client } = fakeClient({
      data: null,
      error: { message: "permission denied for view v_top_maintainers" },
    });
    await expect(
      topMaintainers(client, { ecosystem: "npm", limit: 10 }),
    ).rejects.toThrow(/topMaintainers failed: permission denied/);
  });
});

describe("fetchRiskSignalsForEcosystem", () => {
  it("maps view rows to Package + signals including staleness", async () => {
    const now = new Date("2026-01-01T00:00:00Z");
    const released = new Date("2024-01-01T00:00:00Z").toISOString();
    const { client, calls } = fakeClient({
      data: [
        {
          package_id: "id-1",
          package_name: "alpha",
          ecosystem: "npm",
          description: "first",
          latest_version: "1.0.0",
          maintainer_count: 3,
          fanout_direct: 5,
          fanin_dependents: 100,
          latest_released: released,
          has_repository: true,
        },
        {
          package_id: "id-2",
          package_name: "beta",
          ecosystem: "npm",
          description: null,
          latest_version: null,
          maintainer_count: "1",
          fanout_direct: "0",
          fanin_dependents: "0",
          latest_released: null,
          has_repository: false,
        },
      ],
      error: null,
    });

    const rows = await fetchRiskSignalsForEcosystem(
      client,
      { ecosystem: "npm" },
      now,
    );

    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({
      package: {
        id: "id-1",
        ecosystem: "npm",
        name: "alpha",
        description: "first",
        latest_version: "1.0.0",
      },
      signals: {
        maintainerCount: 3,
        stalenessYears: expect.any(Number),
        fanoutDirect: 5,
        faninDependents: 100,
        hasRepository: true,
      },
    });
    // ~2 years between 2024-01-01 and 2026-01-01
    expect(rows[0].signals.stalenessYears).toBeCloseTo(2, 1);
    // Empty `latest_released` yields 0 staleness
    expect(rows[1].signals.stalenessYears).toBe(0);
    // Numeric strings from PostgREST are coerced
    expect(rows[1].signals.maintainerCount).toBe(1);
    expect(rows[1].package.description).toBe("");
    expect(calls[0]).toEqual(["from", "v_risk_signals"]);
    expect(calls).toContainEqual(["eq", "ecosystem", "npm"]);
  });

  it("skips rows with missing package_id/name/ecosystem", async () => {
    const { client } = fakeClient({
      data: [
        {
          package_id: null,
          package_name: "ghost",
          ecosystem: "npm",
          description: "",
          latest_version: "",
          maintainer_count: 1,
          fanout_direct: 0,
          fanin_dependents: 0,
          latest_released: null,
          has_repository: false,
        },
      ],
      error: null,
    });

    const rows = await fetchRiskSignalsForEcosystem(client, {
      ecosystem: "npm",
    });
    expect(rows).toEqual([]);
  });

  it("throws a labelled error on PostgREST failure", async () => {
    const { client } = fakeClient({
      data: null,
      error: { message: "boom" },
    });
    await expect(
      fetchRiskSignalsForEcosystem(client, { ecosystem: "npm" }),
    ).rejects.toThrow(/fetchRiskSignalsForEcosystem failed: boom/);
  });
});

describe("fetchRiskRangesForEcosystem", () => {
  it("reduces rows to min/max per numeric signal", async () => {
    const now = new Date("2026-01-01T00:00:00Z");
    const oneYearAgo = new Date("2025-01-01T00:00:00Z").toISOString();
    const fiveYearsAgo = new Date("2021-01-01T00:00:00Z").toISOString();
    const { client } = fakeClient({
      data: [
        {
          maintainer_count: 1,
          fanout_direct: 0,
          fanin_dependents: 10,
          latest_released: oneYearAgo,
        },
        {
          maintainer_count: 8,
          fanout_direct: 50,
          fanin_dependents: 500,
          latest_released: fiveYearsAgo,
        },
      ],
      error: null,
    });

    const ranges = await fetchRiskRangesForEcosystem(
      client,
      { ecosystem: "npm" },
      now,
    );

    expect(ranges.maintainerCount).toEqual({ min: 1, max: 8 });
    expect(ranges.fanoutDirect).toEqual({ min: 0, max: 50 });
    expect(ranges.faninDependents).toEqual({ min: 10, max: 500 });
    expect(ranges.stalenessYears.min).toBeCloseTo(1, 1);
    expect(ranges.stalenessYears.max).toBeCloseTo(5, 1);
  });

  it("returns zero ranges when the ecosystem has no rows", async () => {
    const { client } = fakeClient({ data: [], error: null });
    const ranges = await fetchRiskRangesForEcosystem(client, {
      ecosystem: "pypi",
    });
    expect(ranges).toEqual({
      maintainerCount: { min: 0, max: 0 },
      stalenessYears: { min: 0, max: 0 },
      fanoutDirect: { min: 0, max: 0 },
      faninDependents: { min: 0, max: 0 },
    });
  });

  it("throws a labelled error on PostgREST failure", async () => {
    const { client } = fakeClient({
      data: null,
      error: { message: "no view" },
    });
    await expect(
      fetchRiskRangesForEcosystem(client, { ecosystem: "npm" }),
    ).rejects.toThrow(/fetchRiskRangesForEcosystem failed: no view/);
  });
});
