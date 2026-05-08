import { afterEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

import { exchangeCodeForProfile } from "@/lib/oauth";

describe("exchangeCodeForProfile", () => {
  const env = { ...process.env };

  afterEach(() => {
    process.env = { ...env };
    vi.unstubAllGlobals();
  });

  it("returns a Google profile when token + userinfo succeed", async () => {
    process.env.GOOGLE_CLIENT_ID = "c";
    process.env.GOOGLE_CLIENT_SECRET = "s";

    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ access_token: "at" }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          sub: "sub-1",
          email: "u@gmail.com",
          name: "U",
          picture: "http://pic",
        }),
      } as Response);

    vi.stubGlobal("fetch", fetchMock);

    const req = new NextRequest("http://localhost/cb");
    const profile = await exchangeCodeForProfile(req, "google", "auth-code");
    expect(profile).toMatchObject({
      provider: "google",
      providerUserId: "sub-1",
      email: "u@gmail.com",
    });
  });

  it("throws when Google userinfo cannot be fetched", async () => {
    process.env.GOOGLE_CLIENT_ID = "c";
    process.env.GOOGLE_CLIENT_SECRET = "s";

    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ access_token: "at" }),
        } as Response)
        .mockResolvedValueOnce({
          ok: false,
          json: async () => ({}),
        } as Response),
    );

    const req = new NextRequest("http://localhost/cb");
    await expect(
      exchangeCodeForProfile(req, "google", "code"),
    ).rejects.toThrow(/Could not read Google profile/);
  });

  it("throws when GitHub token exchange fails", async () => {
    process.env.GITHUB_CLIENT_ID = "c";
    process.env.GITHUB_CLIENT_SECRET = "s";

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValueOnce({
        ok: false,
        json: async () => ({ error: "bad_code" }),
      } as Response),
    );

    const req = new NextRequest("http://localhost/cb");
    await expect(
      exchangeCodeForProfile(req, "github", "x"),
    ).rejects.toThrow(/bad_code|GitHub login failed/);
  });

  it("returns a GitHub profile using the primary verified email endpoint", async () => {
    process.env.GITHUB_CLIENT_ID = "c";
    process.env.GITHUB_CLIENT_SECRET = "s";

    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ access_token: "ghat", scope: "read:user" }),
        } as Response)
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            id: 42,
            login: "octocat",
            email: null,
            name: "Octo",
            avatar_url: "http://av",
            html_url: "http://github",
          }),
        } as Response)
        .mockResolvedValueOnce({
          ok: true,
          json: async () => [
            { email: "octo@users.noreply.github.com", primary: true, verified: true },
          ],
        } as Response),
    );

    const req = new NextRequest("http://localhost/cb");
    const profile = await exchangeCodeForProfile(req, "github", "abc");
    expect(profile).toMatchObject({
      provider: "github",
      providerUserId: "42",
      email: "octo@users.noreply.github.com",
      displayName: "Octo",
    });
    expect(profile.accessToken).toBe("ghat");
  });
});
