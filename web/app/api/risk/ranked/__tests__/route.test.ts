import { NextRequest } from "next/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Package } from "@/types/api";

vi.mock("@/lib/supabase", () => ({
  createSupabaseServerClient: vi.fn(() => ({})),
}));

vi.mock("@/lib/db/rankings", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/db/rankings")>();
  return {
    ...actual,
    fetchRiskSignalsForEcosystem: vi.fn(),
    fetchRiskRangesForEcosystem: vi.fn(),
  };
});

import {
  fetchRiskRangesForEcosystem,
  fetchRiskSignalsForEcosystem,
} from "@/lib/db/rankings";
import { GET } from "@/app/api/risk/ranked/route";

const mockedSignals = vi.mocked(fetchRiskSignalsForEcosystem);
const mockedRanges = vi.mocked(fetchRiskRangesForEcosystem);

function pkg(id: string, name: string): Package {
  return {
    id,
    ecosystem: "npm",
    name,
    description: "",
    latest_version: "1.0.0",
  };
}

describe("GET /api/risk/ranked", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("ranks packages by composite DESC and exposes bucket but not full breakdown", async () => {
    // Two packages with deliberately hand-chosen signals.
    // Ranges are [1..10] maintainer, [0..10] staleness, [0..100] fanout,
    // [0..1000] fanin. Weights: S1=0.3, S2=0.3, S3=0.2, S4=0.1, S5=0.1.
    //
    // Package "healthy" (maintainers=10, staleness=0, fanout=0, fanin=0, hasRepo=true)
    //   => all normalized signals = 0, composite = 0 (bucket "low").
    // Package "risky" (maintainers=1, staleness=10, fanout=100, fanin=1000, hasRepo=false)
    //   => all normalized signals = 1, composite = 1 (bucket "high").
    mockedSignals.mockResolvedValueOnce([
      {
        package: pkg("id-healthy", "healthy"),
        signals: {
          maintainerCount: 10,
          stalenessYears: 0,
          fanoutDirect: 0,
          faninDependents: 0,
          hasRepository: true,
        },
      },
      {
        package: pkg("id-risky", "risky"),
        signals: {
          maintainerCount: 1,
          stalenessYears: 10,
          fanoutDirect: 100,
          faninDependents: 1000,
          hasRepository: false,
        },
      },
    ]);
    mockedRanges.mockResolvedValueOnce({
      maintainerCount: { min: 1, max: 10 },
      stalenessYears: { min: 0, max: 10 },
      fanoutDirect: { min: 0, max: 100 },
      faninDependents: { min: 0, max: 1000 },
    });

    const req = new NextRequest(
      "http://localhost/api/risk/ranked?ecosystem=npm",
    );
    const res = await GET(req);
    expect(res.status).toBe(200);
    const body = await res.json();

    // Risky first, healthy second. Total=2, default limit=20, offset=0.
    expect(body.items).toHaveLength(2);
    expect(body.items[0].package.name).toBe("risky");
    expect(body.items[0].bucket).toBe("high");
    expect(body.items[0].composite).toBeCloseTo(1, 5);
    expect(body.items[1].package.name).toBe("healthy");
    expect(body.items[1].bucket).toBe("low");
    expect(body.items[1].composite).toBeCloseTo(0, 5);
    expect(body.meta).toEqual({ total: 2, limit: 20, offset: 0 });

    // R10 must NOT return the full per-signal breakdown (that's A6's job).
    expect(body.items[0]).not.toHaveProperty("signals");
    expect(body.items[0]).not.toHaveProperty("risk");
  });

  it("paginates via limit/offset against the sorted ranking", async () => {
    mockedSignals.mockResolvedValueOnce([
      {
        package: pkg("id-a", "alpha"),
        signals: {
          maintainerCount: 5,
          stalenessYears: 5,
          fanoutDirect: 50,
          faninDependents: 500,
          hasRepository: true,
        },
      },
      {
        package: pkg("id-b", "beta"),
        signals: {
          maintainerCount: 1,
          stalenessYears: 10,
          fanoutDirect: 100,
          faninDependents: 1000,
          hasRepository: false,
        },
      },
      {
        package: pkg("id-c", "gamma"),
        signals: {
          maintainerCount: 10,
          stalenessYears: 0,
          fanoutDirect: 0,
          faninDependents: 0,
          hasRepository: true,
        },
      },
    ]);
    mockedRanges.mockResolvedValueOnce({
      maintainerCount: { min: 1, max: 10 },
      stalenessYears: { min: 0, max: 10 },
      fanoutDirect: { min: 0, max: 100 },
      faninDependents: { min: 0, max: 1000 },
    });

    const req = new NextRequest(
      "http://localhost/api/risk/ranked?ecosystem=npm&limit=1&offset=1",
    );
    const res = await GET(req);
    const body = await res.json();

    // Sort DESC by composite -> beta (1.0), alpha (~0.5), gamma (0).
    // offset=1 limit=1 -> alpha.
    expect(body.items).toHaveLength(1);
    expect(body.items[0].package.name).toBe("alpha");
    expect(body.meta).toEqual({ total: 3, limit: 1, offset: 1 });
  });

  it("rejects limit above the 500 hard cap with 400", async () => {
    const req = new NextRequest(
      "http://localhost/api/risk/ranked?limit=9999",
    );
    const res = await GET(req);
    expect(res.status).toBe(400);
    expect(mockedSignals).not.toHaveBeenCalled();
    expect(mockedRanges).not.toHaveBeenCalled();
  });
});
