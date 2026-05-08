import { afterEach, describe, expect, it } from "vitest";

describe("GET /api/auth/providers", () => {
  const env = { ...process.env };

  afterEach(() => {
    process.env = { ...env };
  });

  it("reflects which OAuth env vars are set", async () => {
    delete process.env.GOOGLE_CLIENT_ID;
    delete process.env.GOOGLE_CLIENT_SECRET;
    delete process.env.GITHUB_CLIENT_ID;
    delete process.env.GITHUB_CLIENT_SECRET;
    delete process.env.AUTH_REDIRECT_BASE_URL;

    const { GET } = await import("@/app/api/auth/providers/route");
    const res = await GET();
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      providers: {
        google: { configured: boolean };
        github: { configured: boolean };
      };
      redirectBaseUrl: { configured: boolean; value: string };
    };
    expect(body.providers.google.configured).toBe(false);
    expect(body.providers.github.configured).toBe(false);
    expect(body.redirectBaseUrl.configured).toBe(false);
    expect(body.redirectBaseUrl.value).toBe("http://localhost:3000");
  });
});
