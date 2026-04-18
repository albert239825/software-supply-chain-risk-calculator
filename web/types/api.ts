/**
 * Shared API types for the Supply Chain Risk Scorer backend.
 *
 * These mirror the "Common types" section of `docs/api-spec.md` §0 exactly.
 * Every route handler under `web/app/api/**` and every helper under
 * `web/lib/db/**` must use these types — do not redeclare overlapping shapes.
 */

export type Ecosystem = "npm" | "pypi";

export type ISODate = string;

export type UUID = string;

export type Package = {
  id: UUID;
  ecosystem: Ecosystem;
  name: string;
  description: string;
  latest_version: string;
};

export type Version = {
  id: UUID;
  package_id: UUID;
  version: string;
  released: ISODate | null;
  has_repository: boolean | null;
  github_owner: string | null;
  github_repo: string | null;
};

export type Maintainer = {
  id: UUID;
  package_id: UUID | null;
  username: string;
  name: string | null;
  role: string | null;
  email: string | null;
};

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

export type ListMeta = { total: number; limit: number; offset: number };

export type ListResponse<T> = { items: T[]; meta: ListMeta };

export type ApiError = { error: string };
