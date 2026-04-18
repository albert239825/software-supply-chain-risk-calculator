import type { SupabaseClient } from "@supabase/supabase-js";

/**
 * Minimal fake Supabase client used across DB-helper and route-handler
 * tests. The fake records the full method chain per `.from(table)` call
 * and resolves each awaited query with a handler-provided response.
 *
 * Intentionally narrow: we only implement the query-builder methods that
 * the B-pkg helpers actually use. If future helpers reach for a method
 * that's not defined here, add it to the list in `makeBuilder`.
 *
 * NOTE: this file deliberately lives under __tests__/ but does NOT end in
 * .test.ts, so the Vitest glob (see vitest.config.ts) does not collect it
 * as a test file. It is imported as a helper.
 */

export type FakeOp = [string, ...unknown[]];
export type FakeQueryCall = { table: string; ops: FakeOp[] };
export type FakeResponse = {
  data?: unknown;
  count?: number | null;
  error?: { message: string } | null;
};

export type FakeHandler = (call: FakeQueryCall) => FakeResponse;

type ResolveResult = {
  data: unknown;
  count: number | null;
  error: { message: string } | null;
};

function resolveCall(handler: FakeHandler, call: FakeQueryCall): ResolveResult {
  const res = handler(call);
  return {
    data: res.data ?? null,
    count: res.count ?? null,
    error: res.error ?? null,
  };
}

type FakeBuilder = {
  select: (...args: unknown[]) => FakeBuilder;
  eq: (...args: unknown[]) => FakeBuilder;
  neq: (...args: unknown[]) => FakeBuilder;
  ilike: (...args: unknown[]) => FakeBuilder;
  or: (...args: unknown[]) => FakeBuilder;
  in: (...args: unknown[]) => FakeBuilder;
  is: (...args: unknown[]) => FakeBuilder;
  order: (...args: unknown[]) => FakeBuilder;
  range: (...args: unknown[]) => FakeBuilder;
  limit: (...args: unknown[]) => FakeBuilder;
  maybeSingle: () => Promise<{ data: unknown; error: { message: string } | null }>;
  then: <TResult1 = ResolveResult, TResult2 = never>(
    onFulfilled?:
      | ((value: ResolveResult) => TResult1 | PromiseLike<TResult1>)
      | null,
    onRejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ) => Promise<TResult1 | TResult2>;
};

/**
 * Build a fake Supabase client and capture its call log.
 *
 * The handler is called once per `await` on a query builder (or per
 * `maybeSingle()`). Return the full response you want that specific call to
 * see. Handlers are free to inspect `call.ops` to vary behavior by query —
 * the test suite uses this to e.g. return different rows for `packages`
 * vs. `versions` tables in multi-step helpers like
 * `getLatestVersionRecord`.
 */
export function createFakeSupabase(handler: FakeHandler): {
  client: SupabaseClient;
  calls: FakeQueryCall[];
} {
  const calls: FakeQueryCall[] = [];

  function makeBuilder(current: FakeQueryCall): FakeBuilder {
    const noop = (method: string) =>
      ((...args: unknown[]) => {
        current.ops.push([method, ...args]);
        return builder;
      }) as FakeBuilder[keyof FakeBuilder];

    const builder: FakeBuilder = {
      select: noop("select") as FakeBuilder["select"],
      eq: noop("eq") as FakeBuilder["eq"],
      neq: noop("neq") as FakeBuilder["neq"],
      ilike: noop("ilike") as FakeBuilder["ilike"],
      or: noop("or") as FakeBuilder["or"],
      in: noop("in") as FakeBuilder["in"],
      is: noop("is") as FakeBuilder["is"],
      order: noop("order") as FakeBuilder["order"],
      range: noop("range") as FakeBuilder["range"],
      limit: noop("limit") as FakeBuilder["limit"],
      maybeSingle: () => {
        current.ops.push(["maybeSingle"]);
        const res = resolveCall(handler, current);
        const single = Array.isArray(res.data)
          ? (res.data[0] ?? null)
          : res.data;
        return Promise.resolve({ data: single, error: res.error });
      },
      then: (onFulfilled, onRejected) => {
        const res = resolveCall(handler, current);
        return Promise.resolve(res).then(
          onFulfilled ?? undefined,
          onRejected ?? undefined,
        );
      },
    };
    return builder;
  }

  const client = {
    from(table: string) {
      const call: FakeQueryCall = { table, ops: [] };
      calls.push(call);
      return makeBuilder(call);
    },
  } as unknown as SupabaseClient;

  return { client, calls };
}

/**
 * Build a fake Supabase client that returns a different canned response per
 * `.from(table)` call, in the order the helpers invoke them. Convenient for
 * multi-step helpers where a single handler function would otherwise need
 * to count invocations itself.
 */
export function createFakeSupabaseSequence(responses: FakeResponse[]): {
  client: SupabaseClient;
  calls: FakeQueryCall[];
} {
  let i = 0;
  return createFakeSupabase(() => {
    const res = responses[i] ?? { data: null };
    i += 1;
    return res;
  });
}
