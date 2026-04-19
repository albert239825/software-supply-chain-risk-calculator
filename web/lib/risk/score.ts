/**
 * Pure risk-scoring library for the composite signal formula in
 * `docs/PLAN.md` §7. This module has no DB import — it is unit-testable
 * and is consumed by R10 (`/api/risk/ranked`) and A6
 * (`/api/packages/:packageId/risk`, future).
 */

export type RiskBreakdown = {
  composite: number;
  bucket: "low" | "medium" | "high";
  signals: {
    maintainer_count: { value: number; normalized: number; weight: number };
    staleness_years: { value: number; normalized: number; weight: number };
    fanout_direct: { value: number; normalized: number; weight: number };
    fanin_dependents: { value: number; normalized: number; weight: number };
    has_repository: { value: boolean; normalized: number; weight: number };
  };
};

/**
 * Signal weights for the composite risk score.
 *
 * PLACEHOLDER VALUES pending decision D4 in `docs/PLAN.md` §9. The five
 * weights sum to 1.0 so the composite is directly comparable across
 * packages. When the team ratifies the final weights, update both this
 * constant and the documentation in §7.
 */
export const RISK_WEIGHTS = {
  maintainer_count: 0.3,
  staleness_years: 0.3,
  fanout_direct: 0.2,
  fanin_dependents: 0.1,
  has_repository: 0.1,
} as const;

export type RiskSignals = {
  maintainerCount: number;
  stalenessYears: number;
  fanoutDirect: number;
  faninDependents: number;
  hasRepository: boolean;
};

export type NormalizationRange = { min: number; max: number };

/**
 * Four numeric signals are min-max normalized against ecosystem-wide
 * ranges supplied by the caller. `has_repository` is a boolean so no range
 * is required for it.
 */
export type RiskSignalRanges = {
  maintainerCount: NormalizationRange;
  stalenessYears: NormalizationRange;
  fanoutDirect: NormalizationRange;
  faninDependents: NormalizationRange;
};

/**
 * Min-max normalize `value` into `[0, 1]` using the closed interval
 * `[min, max]`. Values outside the range are clamped. When `max === min`
 * the range is degenerate and the function returns 0 so the signal does
 * not spuriously contribute to the composite.
 */
export function normalize(value: number, min: number, max: number): number {
  if (!Number.isFinite(value) || !Number.isFinite(min) || !Number.isFinite(max)) {
    return 0;
  }
  if (max <= min) {
    return 0;
  }
  if (value <= min) {
    return 0;
  }
  if (value >= max) {
    return 1;
  }
  return (value - min) / (max - min);
}

/**
 * Fixed bucket thresholds mapped onto the normalized composite score. These
 * are documented in `docs/PLAN.md` §7 and are intentionally NOT learned:
 * reviewers need to reproduce them from the formula alone.
 *
 *   composite < 0.34  -> "low"
 *   composite < 0.67  -> "medium"
 *   composite >= 0.67 -> "high"
 */
export const BUCKET_THRESHOLDS = {
  lowMax: 0.34,
  mediumMax: 0.67,
} as const;

export function bucketForComposite(composite: number): "low" | "medium" | "high" {
  if (!Number.isFinite(composite)) {
    return "low";
  }
  if (composite < BUCKET_THRESHOLDS.lowMax) {
    return "low";
  }
  if (composite < BUCKET_THRESHOLDS.mediumMax) {
    return "medium";
  }
  return "high";
}

/**
 * Compute the composite risk score and the per-signal breakdown used by A6.
 *
 * Signal direction reminders (see PLAN §7):
 *   - maintainer_count   : LOWER value  -> higher risk (invert before normalize)
 *   - staleness_years    : HIGHER value -> higher risk
 *   - fanout_direct      : HIGHER value -> higher risk
 *   - fanin_dependents   : HIGHER value -> higher risk
 *   - has_repository     : FALSE        -> higher risk
 */
export function computeComposite(
  signals: RiskSignals,
  ranges: RiskSignalRanges,
): RiskBreakdown {
  const maintainerInverted = invertWithinRange(
    signals.maintainerCount,
    ranges.maintainerCount,
  );
  const maintainerNormalized = normalize(
    maintainerInverted,
    ranges.maintainerCount.min,
    ranges.maintainerCount.max,
  );
  const stalenessNormalized = normalize(
    signals.stalenessYears,
    ranges.stalenessYears.min,
    ranges.stalenessYears.max,
  );
  const fanoutNormalized = normalize(
    signals.fanoutDirect,
    ranges.fanoutDirect.min,
    ranges.fanoutDirect.max,
  );
  const faninNormalized = normalize(
    signals.faninDependents,
    ranges.faninDependents.min,
    ranges.faninDependents.max,
  );
  const hasRepoNormalized = signals.hasRepository ? 0 : 1;

  const composite =
    RISK_WEIGHTS.maintainer_count * maintainerNormalized +
    RISK_WEIGHTS.staleness_years * stalenessNormalized +
    RISK_WEIGHTS.fanout_direct * fanoutNormalized +
    RISK_WEIGHTS.fanin_dependents * faninNormalized +
    RISK_WEIGHTS.has_repository * hasRepoNormalized;

  return {
    composite,
    bucket: bucketForComposite(composite),
    signals: {
      maintainer_count: {
        value: signals.maintainerCount,
        normalized: maintainerNormalized,
        weight: RISK_WEIGHTS.maintainer_count,
      },
      staleness_years: {
        value: signals.stalenessYears,
        normalized: stalenessNormalized,
        weight: RISK_WEIGHTS.staleness_years,
      },
      fanout_direct: {
        value: signals.fanoutDirect,
        normalized: fanoutNormalized,
        weight: RISK_WEIGHTS.fanout_direct,
      },
      fanin_dependents: {
        value: signals.faninDependents,
        normalized: faninNormalized,
        weight: RISK_WEIGHTS.fanin_dependents,
      },
      has_repository: {
        value: signals.hasRepository,
        normalized: hasRepoNormalized,
        weight: RISK_WEIGHTS.has_repository,
      },
    },
  };
}

function invertWithinRange(value: number, range: NormalizationRange): number {
  if (range.max <= range.min) {
    return range.min;
  }
  const clamped = Math.min(Math.max(value, range.min), range.max);
  return range.max - (clamped - range.min);
}
