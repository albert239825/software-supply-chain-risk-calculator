import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import PackagesPage from "@/app/packages/page";
import StatsPage from "@/app/stats/page";
import RiskAnalysisPage from "@/app/risk/page";
import AbandonedPage from "@/app/abandoned/page";
import NoRepoPage from "@/app/no-repo/page";
import MaintainersPage from "@/app/maintainers/page";

const fetchMock = vi.fn();

describe("data-driven pages", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  it("PackagesPage lists rows from /api/packages/all", async () => {
    fetchMock.mockResolvedValueOnce({
      json: async () => [
        {
          package_name: "left-pad",
          version: "1.0.0",
          released: "2016-01-01",
        },
      ],
    } as Response);

    render(<PackagesPage />);
    expect(await screen.findByText("left-pad")).toBeInTheDocument();
  });

  it("StatsPage loads both statistics endpoints", async () => {
    fetchMock
      .mockResolvedValueOnce({ json: async () => [{ package_name: "x", num_dependencies: 1 }] } as Response)
      .mockResolvedValueOnce({ json: async () => [{ package_name: "y", dependents: 2 }] } as Response);

    render(<StatsPage />);
    expect(await screen.findByText("x")).toBeInTheDocument();
    expect(screen.getByText("y")).toBeInTheDocument();
  });

  it("RiskAnalysisPage renders ranked rows", async () => {
    fetchMock.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        items: [
          {
            package_id: "pid",
            package_name: "lodash",
            maintainers: 2,
            dependencies: 10,
            last_release: "2024-01-01",
            risk_score: 0.42,
            bucket: "medium",
          },
        ],
        total: 1,
        limit: 30,
        offset: 0,
        hasMore: false,
      }),
    } as Response);

    render(<RiskAnalysisPage />);
    expect(await screen.findByText("lodash")).toBeInTheDocument();
    expect(screen.getByText(/medium/i)).toBeInTheDocument();
  });

  it("AbandonedPage renders API rows", async () => {
    fetchMock.mockResolvedValueOnce({
      json: async () => [
        { package_name: "ghost", dependents: 9, last_release: "2019-01-01" },
      ],
    } as Response);
    render(<AbandonedPage />);
    expect(await screen.findByText("ghost")).toBeInTheDocument();
  });

  it("NoRepoPage renders API rows", async () => {
    fetchMock.mockResolvedValueOnce({
      json: async () => [{ package_name: "naked", version: "0.1.0" }],
    } as Response);
    render(<NoRepoPage />);
    expect(await screen.findByText("naked")).toBeInTheDocument();
  });

  it("MaintainersPage renders API rows", async () => {
    fetchMock.mockResolvedValueOnce({
      json: async () => [{ username: "alice", num_packages: 12 }],
    } as Response);
    render(<MaintainersPage />);
    expect(await screen.findByText("alice")).toBeInTheDocument();
  });
});
