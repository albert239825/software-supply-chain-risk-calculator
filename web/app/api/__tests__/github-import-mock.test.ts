import { beforeEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";

vi.mock("@/lib/db", () => ({
  default: { query: vi.fn() },
}));

vi.mock("@/lib/auth", () => ({
  getCurrentUser: vi.fn(),
}));

vi.mock("@/lib/github", () => ({
  createDependencySets: vi.fn(() => ({ npm: new Set<string>(), pypi: new Set<string>() })),
  extractPackageJsonDependencies: vi.fn(() => []),
  extractPackageLockDependencies: vi.fn(() => []),
  extractPyprojectDependencies: vi.fn(() => []),
  extractRequirementsDependencies: vi.fn(() => []),
  findDependencyManifestPaths: vi.fn(() => []),
  fetchPackageJson: vi.fn(),
  fetchRepoFileText: vi.fn(),
  getGitHubAccessToken: vi.fn(),
}));

import { POST } from "@/app/api/github/repos/import/route";
import { getCurrentUser } from "@/lib/auth";
import pool from "@/lib/db";
import { findDependencyManifestPaths, getGitHubAccessToken } from "@/lib/github";

function req(body: string) {
  return new NextRequest("http://x/api/github/repos/import", {
    method: "POST",
    body,
    headers: { "content-type": "application/json" },
  });
}

describe("POST /api/github/repos/import", () => {
  beforeEach(() => {
    vi.mocked(getCurrentUser).mockReset();
    vi.mocked(getGitHubAccessToken).mockReset();
    vi.mocked(findDependencyManifestPaths).mockClear();
    vi.mocked(pool.query).mockReset();
  });

  it("401 when anonymous", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue(null);
    const res = await POST(req(JSON.stringify({ fullName: "owner/repo" })));
    expect(res.status).toBe(401);
  });

  it("400 when body is malformed JSON", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue({
      id: "u1",
      email: null,
      displayName: null,
      avatarUrl: null,
    });
    vi.mocked(getGitHubAccessToken).mockResolvedValue("token");

    const res = await POST(req("{"));

    expect(res.status).toBe(400);
    expect(findDependencyManifestPaths).not.toHaveBeenCalled();
    expect(pool.query).not.toHaveBeenCalled();
  });

  it("400 when fullName is invalid", async () => {
    vi.mocked(getCurrentUser).mockResolvedValue({
      id: "u1",
      email: null,
      displayName: null,
      avatarUrl: null,
    });
    vi.mocked(getGitHubAccessToken).mockResolvedValue("token");

    const res = await POST(req(JSON.stringify({ fullName: "not-a-full-name" })));

    expect(res.status).toBe(400);
    expect(findDependencyManifestPaths).not.toHaveBeenCalled();
    expect(pool.query).not.toHaveBeenCalled();
  });
});
