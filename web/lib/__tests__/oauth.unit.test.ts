import { afterEach, describe, expect, it } from "vitest";
import { NextRequest } from "next/server";

import { getOAuthStateCookie } from "@/lib/auth";
import { assertValidState, buildOAuthUrl } from "@/lib/oauth";

describe("oauth helpers", () => {
  const envSnapshot = { ...process.env };

  afterEach(() => {
    process.env = { ...envSnapshot };
  });

  it("buildOAuthUrl builds a Google authorize URL", () => {
    process.env.GOOGLE_CLIENT_ID = "google-client";
    const req = new NextRequest("http://localhost:3000/api/auth/google");
    const url = buildOAuthUrl(req, "google", "state-token");
    expect(url).toContain("accounts.google.com");
    expect(url).toContain("client_id=google-client");
    expect(url).toContain("state=state-token");
  });

  it("buildOAuthUrl builds a GitHub authorize URL", () => {
    process.env.GITHUB_CLIENT_ID = "gh-client";
    const req = new NextRequest("http://localhost:3000/api/auth/github");
    const url = buildOAuthUrl(req, "github", "xyz");
    expect(url).toContain("github.com/login/oauth/authorize");
    expect(url).toContain("client_id=gh-client");
  });

  it("assertValidState passes when cookie matches query state", () => {
    const provider = "google";
    const state = "matching";
    const req = new NextRequest("http://localhost/cb", {
      headers: {
        cookie: `${getOAuthStateCookie(provider)}=${encodeURIComponent(state)}`,
      },
    });
    expect(() => assertValidState(req, provider, state)).not.toThrow();
  });

  it("assertValidState throws when state mismatches", () => {
    const provider = "github";
    const req = new NextRequest("http://localhost/cb", {
      headers: {
        cookie: `${getOAuthStateCookie(provider)}=expected`,
      },
    });
    expect(() => assertValidState(req, provider, "wrong")).toThrow(
      /state did not match/,
    );
  });
});
