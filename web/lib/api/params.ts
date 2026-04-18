import { NextResponse } from "next/server";
import { z, type ZodType } from "zod";

import type { ApiError, Ecosystem } from "@/types/api";

/**
 * Request-parsing and response helpers shared by every route handler under
 * `web/app/api/**`. Routes must use these instead of reading
 * `url.searchParams` directly so validation errors and error bodies stay
 * consistent with the contract in `docs/api-spec.md`.
 */

/**
 * Pagination: `limit` defaults vary per route (overridden with
 * `paginationSchema.extend(...)` when needed). The hard ceiling of 500
 * matches the max declared in the api-spec for list endpoints.
 */
export const paginationSchema = z.object({
  limit: z.coerce.number().int().positive().max(500).default(100),
  offset: z.coerce.number().int().min(0).default(0),
});

export type PaginationParams = z.infer<typeof paginationSchema>;

export const ecosystemSchema: ZodType<Ecosystem> = z.enum(["npm", "pypi"]).default("npm");

/**
 * Shared `depKind` validator for A4 and any future route that filters
 * dependency rows by kind. `'all'` (the default) means "no filter" and is
 * translated to "omit the `.eq('dep_kind', ...)` clause" inside the DB
 * helper.
 */
export const depKindSchema = z
  .enum(["dependency", "peer", "optional", "all"])
  .default("all");

export type DepKindParam = z.infer<typeof depKindSchema>;

export type ParseQueryResult<T> =
  | { data: T; error?: undefined }
  | { data?: undefined; error: NextResponse };

/**
 * Parse a `URL`'s query string against the given Zod schema. On success
 * returns `{ data }`. On failure returns a prebuilt 400 `NextResponse`
 * with an `ApiError` body so the route can early-return.
 */
export function parseQuery<T>(
  url: URL,
  schema: ZodType<T>,
): ParseQueryResult<T> {
  const raw: Record<string, string> = {};
  url.searchParams.forEach((value, key) => {
    raw[key] = value;
  });
  const result = schema.safeParse(raw);
  if (!result.success) {
    return { error: jsonError(400, formatZodError(result.error)) };
  }
  return { data: result.data };
}

/**
 * Shorthand for building the `{ error }` body used across every error
 * response in `docs/api-spec.md`.
 */
export function jsonError(status: number, message: string): NextResponse {
  const body: ApiError = { error: message };
  return NextResponse.json(body, { status });
}

function formatZodError(error: z.ZodError): string {
  return error.issues
    .map((issue) => {
      const path = issue.path.join(".");
      return path ? `${path}: ${issue.message}` : issue.message;
    })
    .join("; ");
}
