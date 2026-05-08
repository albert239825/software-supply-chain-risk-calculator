import { afterEach, describe, expect, it } from "vitest";
import { NextRequest } from "next/server";

describe("GET /api/auth/:provider", () => {
  const env = { ...process.env };

  afterEach(() => {
    process.env = { ...env };
  });

  it("404 for unsupported provider", async () => {
    const { GET } = await import("@/app/api/auth/[provider]/route");
    const res = await GET(new NextRequest("http://localhost/x"), {
      params: Promise.resolve({ provider: "facebook" }),
    });
    expect(res.status).toBe(404);
  });

  it("500 when Google client id is not configured", async () => {
    delete process.env.GOOGLE_CLIENT_ID;
    const { GET } = await import("@/app/api/auth/[provider]/route");
    const res = await GET(new NextRequest("http://localhost/x"), {
      params: Promise.resolve({ provider: "google" }),
    });
    expect(res.status).toBe(500);
  });

  it("302 redirects to Google when configured", async () => {
    process.env.GOOGLE_CLIENT_ID = "cid";
    const { GET } = await import("@/app/api/auth/[provider]/route");
    const res = await GET(new NextRequest("http://localhost:3000/x"), {
      params: Promise.resolve({ provider: "google" }),
    });
    expect([302, 307]).toContain(res.status);
    expect(res.headers.get("location")).toContain("accounts.google.com");
    expect(res.cookies.get("ssc_oauth_state_google")?.value?.length).toBeGreaterThan(
      8,
    );
  });
});
