import { describe, expect, it } from "vitest";
import { NextRequest } from "next/server";

import { POST } from "@/app/api/auth/login/route";

function jsonReq(body: unknown) {
  return new NextRequest("http://localhost/api/auth/login", {
    method: "POST",
    body: JSON.stringify(body),
    headers: { "content-type": "application/json" },
  });
}

describe("POST /api/auth/login", () => {
  it("400 when email missing", async () => {
    const res = await POST(jsonReq({ githubUsername: "octocat" }));
    expect(res.status).toBe(400);
  });

  it("400 when body is malformed JSON", async () => {
    const res = await POST(
      new NextRequest("http://localhost/api/auth/login", {
        method: "POST",
        body: "{",
        headers: { "content-type": "application/json" },
      }),
    );
    expect(res.status).toBe(400);
  });

  it("400 when body is not a JSON object", async () => {
    const res = await POST(jsonReq(["me@gmail.com", "octocat"]));
    expect(res.status).toBe(400);
  });

  it("400 when email invalid", async () => {
    const res = await POST(
      jsonReq({ email: "not-an-email", githubUsername: "octocat" }),
    );
    expect(res.status).toBe(400);
  });

  it("400 when email is not Gmail", async () => {
    const res = await POST(
      jsonReq({ email: "a@company.com", githubUsername: "octocat" }),
    );
    expect(res.status).toBe(400);
  });

  it("400 when GitHub username invalid", async () => {
    const res = await POST(
      jsonReq({ email: "me@gmail.com", githubUsername: "bad@name" }),
    );
    expect(res.status).toBe(400);
  });
});
