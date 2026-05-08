import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useParams: () => ({ packageId: "pkg-1" }),
}));

import PackageDetailPage from "@/app/packages/[packageId]/page";

const fetchMock = vi.fn();

describe("PackageDetailPage", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  it("renders JSON payload for versions", async () => {
    fetchMock
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [{ version: "1.0.0", released: "2026-01-01T00:00:00Z" }],
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          id: "pkg-1",
          ecosystem: "npm",
          name: "left-pad",
          description: "pads strings",
          latest_version: "1.0.0",
          latest_released: "2026-01-01T00:00:00Z",
          has_repository: true,
          github_owner: "owner",
          github_repo: "repo",
        }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [
          { username: "", name: "Alice", role: null, email: "alice@example.com" },
        ],
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [
          { package_id: "dep", package_name: null, version_spec: null, dep_kind: null },
        ],
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [
          { package_id: "parent", package_name: "parent-pkg", dependent_version: null },
        ],
      } as Response);

    render(<PackageDetailPage />);
    await waitFor(() => {
      const pre = document.querySelector("pre");
      expect(pre?.textContent).toContain("1.0.0");
    });
    expect(screen.getByRole("heading", { name: "left-pad" })).toBeInTheDocument();
    expect(screen.getByText("Present")).toBeInTheDocument();
    expect(screen.getByText("Alice")).toBeInTheDocument();
    expect(screen.getByText("parent-pkg")).toBeInTheDocument();
    expect(screen.getAllByText("Unknown").length).toBeGreaterThan(0);
    expect(screen.getByText("-")).toBeInTheDocument();
    expect(screen.getByText("dependency")).toBeInTheDocument();
  });

  it("uses fallback metadata and empty table rows when optional requests fail", async () => {
    fetchMock
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [{ version: "2.0.0", released: null }],
      } as Response)
      .mockResolvedValueOnce({
        ok: false,
        status: 404,
        json: async () => ({ error: "missing package" }),
      } as Response)
      .mockRejectedValueOnce(new Error("maintainers failed"))
      .mockRejectedValueOnce(new Error("dependencies failed"))
      .mockRejectedValueOnce(new Error("dependents failed"));

    render(<PackageDetailPage />);

    expect(await screen.findByRole("heading", { name: "pkg-1" })).toBeInTheDocument();
    expect(screen.getByText("No package description available.")).toBeInTheDocument();
    expect(screen.getByText("Missing")).toBeInTheDocument();
    expect(screen.getByText("No maintainers found.")).toBeInTheDocument();
    expect(screen.getByText("No direct dependencies found.")).toBeInTheDocument();
    expect(screen.getByText("No direct dependents found.")).toBeInTheDocument();
  });

  it("shows an error when versions cannot be loaded", async () => {
    fetchMock
      .mockResolvedValueOnce({
        ok: false,
        status: 500,
        json: async () => ({ error: "versions unavailable" }),
      } as Response)
      .mockResolvedValue({ ok: true, json: async () => [] } as Response);

    render(<PackageDetailPage />);

    expect(await screen.findByText("versions unavailable")).toBeInTheDocument();
  });
});
