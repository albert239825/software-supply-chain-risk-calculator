import { describe, expect, it } from "vitest";

import {
  BUCKET_THRESHOLDS,
  RISK_WEIGHTS,
  bucketForComposite,
  computeComposite,
  normalize,
  type RiskSignalRanges,
  type RiskSignals,
} from "@/lib/risk/score";

describe("normalize", () => {
  it("maps the interval endpoints to 0 and 1", () => {
    expect(normalize(0, 0, 10)).toBe(0);
    expect(normalize(10, 0, 10)).toBe(1);
  });

  it("linearly interpolates interior values", () => {
    expect(normalize(5, 0, 10)).toBeCloseTo(0.5, 5);
    expect(normalize(2, 0, 8)).toBeCloseTo(0.25, 5);
  });

  it("clamps values outside the range", () => {
    expect(normalize(-5, 0, 10)).toBe(0);
    expect(normalize(42, 0, 10)).toBe(1);
  });

  it("returns 0 for a degenerate range", () => {
    expect(normalize(5, 10, 10)).toBe(0);
    expect(normalize(5, 10, 1)).toBe(0);
  });

  it("handles non-finite inputs defensively", () => {
    expect(normalize(Number.NaN, 0, 10)).toBe(0);
    expect(normalize(5, Number.POSITIVE_INFINITY, 10)).toBe(0);
  });
});

describe("bucketForComposite", () => {
  it("returns 'low' below the low/medium threshold", () => {
    expect(bucketForComposite(0)).toBe("low");
    expect(bucketForComposite(BUCKET_THRESHOLDS.lowMax - 0.01)).toBe("low");
  });

  it("returns 'medium' between the two thresholds", () => {
    expect(bucketForComposite(BUCKET_THRESHOLDS.lowMax)).toBe("medium");
    expect(bucketForComposite(BUCKET_THRESHOLDS.mediumMax - 0.01)).toBe("medium");
  });

  it("returns 'high' at and above the medium/high threshold", () => {
    expect(bucketForComposite(BUCKET_THRESHOLDS.mediumMax)).toBe("high");
    expect(bucketForComposite(1)).toBe("high");
  });

  it("defaults non-finite composites to 'low'", () => {
    expect(bucketForComposite(Number.NaN)).toBe("low");
  });
});

describe("computeComposite", () => {
  const ranges: RiskSignalRanges = {
    maintainerCount: { min: 1, max: 10 },
    stalenessYears: { min: 0, max: 10 },
    fanoutDirect: { min: 0, max: 100 },
    faninDependents: { min: 0, max: 1000 },
  };

  it("returns a fully-formed breakdown with correct weights", () => {
    const signals: RiskSignals = {
      maintainerCount: 10,
      stalenessYears: 0,
      fanoutDirect: 0,
      faninDependents: 0,
      hasRepository: true,
    };
    const result = computeComposite(signals, ranges);
    expect(result.signals.maintainer_count.weight).toBe(
      RISK_WEIGHTS.maintainer_count,
    );
    expect(result.signals.staleness_years.weight).toBe(
      RISK_WEIGHTS.staleness_years,
    );
    expect(result.signals.fanout_direct.weight).toBe(RISK_WEIGHTS.fanout_direct);
    expect(result.signals.fanin_dependents.weight).toBe(
      RISK_WEIGHTS.fanin_dependents,
    );
    expect(result.signals.has_repository.weight).toBe(
      RISK_WEIGHTS.has_repository,
    );
  });

  it("assigns low risk to a healthy package", () => {
    const signals: RiskSignals = {
      maintainerCount: 10,
      stalenessYears: 0,
      fanoutDirect: 0,
      faninDependents: 0,
      hasRepository: true,
    };
    const result = computeComposite(signals, ranges);
    expect(result.composite).toBeCloseTo(0, 5);
    expect(result.bucket).toBe("low");
  });

  it("assigns high risk to a single-maintainer abandoned package without a repo", () => {
    const signals: RiskSignals = {
      maintainerCount: 1,
      stalenessYears: 10,
      fanoutDirect: 100,
      faninDependents: 1000,
      hasRepository: false,
    };
    const result = computeComposite(signals, ranges);
    expect(result.composite).toBeCloseTo(1, 5);
    expect(result.bucket).toBe("high");
  });

  it("inverts maintainer_count so fewer maintainers -> higher risk", () => {
    const fewMaintainers = computeComposite(
      {
        maintainerCount: 1,
        stalenessYears: 0,
        fanoutDirect: 0,
        faninDependents: 0,
        hasRepository: true,
      },
      ranges,
    );
    const manyMaintainers = computeComposite(
      {
        maintainerCount: 10,
        stalenessYears: 0,
        fanoutDirect: 0,
        faninDependents: 0,
        hasRepository: true,
      },
      ranges,
    );
    expect(fewMaintainers.signals.maintainer_count.normalized).toBeGreaterThan(
      manyMaintainers.signals.maintainer_count.normalized,
    );
    expect(fewMaintainers.composite).toBeGreaterThan(manyMaintainers.composite);
  });

  it("gives has_repository=false a normalized contribution of 1", () => {
    const signals: RiskSignals = {
      maintainerCount: 10,
      stalenessYears: 0,
      fanoutDirect: 0,
      faninDependents: 0,
      hasRepository: false,
    };
    const result = computeComposite(signals, ranges);
    expect(result.signals.has_repository.normalized).toBe(1);
    expect(result.composite).toBeCloseTo(RISK_WEIGHTS.has_repository, 5);
  });

  it("echoes the raw signal values back unchanged", () => {
    const signals: RiskSignals = {
      maintainerCount: 3,
      stalenessYears: 2.5,
      fanoutDirect: 42,
      faninDependents: 200,
      hasRepository: true,
    };
    const result = computeComposite(signals, ranges);
    expect(result.signals.maintainer_count.value).toBe(3);
    expect(result.signals.staleness_years.value).toBe(2.5);
    expect(result.signals.fanout_direct.value).toBe(42);
    expect(result.signals.fanin_dependents.value).toBe(200);
    expect(result.signals.has_repository.value).toBe(true);
  });
});
