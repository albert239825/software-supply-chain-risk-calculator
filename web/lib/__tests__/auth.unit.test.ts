import { describe, expect, it, afterEach } from "vitest";
import { NextRequest, NextResponse } from "next/server";

import {
  SESSION_COOKIE,
  applySessionCookie,
  clearSessionCookie,
  createOpaqueToken,
  getBaseUrl,
  getCallbackUrl,
  getOAuthStateCookie,
  hashToken,
  readCookie,
} from "@/lib/auth";

describe("auth helpers (no database)", () => {
  const envSnapshot = { ...process.env };

  afterEach(() => {
    process.env = { ...envSnapshot };
  });

  it("readCookie parses and decodes", () => {
    const req = new Request("http://x", {
      headers: { cookie: "a=1; b=two%20three" },
    });
    expect(readCookie(req, "b")).toBe("two three");
    expect(readCookie(req, "missing")).toBeNull();
  });

  it("readCookie returns null when header absent", () => {
    expect(readCookie(new Request("http://x"), "a")).toBeNull();
  });

  it("getBaseUrl prefers AUTH_REDIRECT_BASE_URL", () => {
    process.env.AUTH_REDIRECT_BASE_URL = "https://app.example";
    const req = new NextRequest("http://localhost:3000/path");
    expect(getBaseUrl(req)).toBe("https://app.example");
  });

  it("getCallbackUrl builds /api/auth/callback/:provider", () => {
    process.env.AUTH_REDIRECT_BASE_URL = "https://app.example";
    const req = new NextRequest("http://localhost:3000/x");
    expect(getCallbackUrl(req, "google")).toBe(
      "https://app.example/api/auth/callback/google",
    );
  });

  it("getOAuthStateCookie is namespaced per provider", () => {
    expect(getOAuthStateCookie("google")).toContain("google");
    expect(getOAuthStateCookie("github")).toContain("github");
  });

  it("createOpaqueToken and hashToken are stable length", () => {
    const t = createOpaqueToken();
    expect(t.length).toBeGreaterThan(20);
    const h = hashToken(t);
    expect(h).toMatch(/^[a-f0-9]{64}$/);
    expect(hashToken("x")).toBe(hashToken("x"));
  });

  it("applySessionCookie and clearSessionCookie touch SSC cookie", () => {
    const res = NextResponse.json({ ok: true });
    applySessionCookie(res, "tok");
    expect(res.cookies.get(SESSION_COOKIE)?.value).toBe("tok");
    clearSessionCookie(res);
    expect(res.cookies.get(SESSION_COOKIE)?.value).toBe("");
  });
});
