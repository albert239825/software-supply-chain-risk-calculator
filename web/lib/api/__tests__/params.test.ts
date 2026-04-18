import { describe, expect, it } from "vitest";
import { z } from "zod";

import {
  ecosystemSchema,
  jsonError,
  paginationSchema,
  parseQuery,
} from "@/lib/api/params";

function makeUrl(query: string): URL {
  return new URL(`http://localhost${query}`);
}

describe("paginationSchema", () => {
  it("applies defaults when fields are omitted", () => {
    const result = parseQuery(makeUrl("/"), paginationSchema);
    expect(result.error).toBeUndefined();
    expect(result.data).toEqual({ limit: 100, offset: 0 });
  });

  it("coerces numeric strings", () => {
    const result = parseQuery(
      makeUrl("/?limit=25&offset=50"),
      paginationSchema,
    );
    expect(result.data).toEqual({ limit: 25, offset: 50 });
  });

  it("rejects non-numeric input with a 400", async () => {
    const result = parseQuery(makeUrl("/?limit=foo"), paginationSchema);
    expect(result.data).toBeUndefined();
    expect(result.error?.status).toBe(400);
    const body = await result.error?.json();
    expect(body).toMatchObject({ error: expect.stringContaining("limit") });
  });

  it("rejects limit above the hard ceiling of 500", async () => {
    const result = parseQuery(makeUrl("/?limit=1000"), paginationSchema);
    expect(result.error?.status).toBe(400);
  });

  it("rejects negative offset", async () => {
    const result = parseQuery(makeUrl("/?offset=-1"), paginationSchema);
    expect(result.error?.status).toBe(400);
  });
});

describe("ecosystemSchema", () => {
  const wrapper = z.object({ ecosystem: ecosystemSchema });

  it("defaults to npm when omitted", () => {
    const result = parseQuery(makeUrl("/"), wrapper);
    expect(result.data).toEqual({ ecosystem: "npm" });
  });

  it("accepts the two valid values", () => {
    expect(parseQuery(makeUrl("/?ecosystem=npm"), wrapper).data).toEqual({
      ecosystem: "npm",
    });
    expect(parseQuery(makeUrl("/?ecosystem=pypi"), wrapper).data).toEqual({
      ecosystem: "pypi",
    });
  });

  it("rejects unknown ecosystems with a 400", () => {
    const result = parseQuery(makeUrl("/?ecosystem=maven"), wrapper);
    expect(result.data).toBeUndefined();
    expect(result.error?.status).toBe(400);
  });
});

describe("jsonError", () => {
  it("builds a NextResponse with an { error } body and the given status", async () => {
    const response = jsonError(418, "I'm a teapot");
    expect(response.status).toBe(418);
    expect(await response.json()).toEqual({ error: "I'm a teapot" });
  });
});
