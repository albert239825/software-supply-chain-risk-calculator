import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

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

  afterEach(() => {
    vi.useRealTimers();
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

  it("RiskAnalysisPage loads additional rows and bucket variants", async () => {
    fetchMock
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          items: [
            {
              package_id: "high",
              package_name: "risky",
              maintainers: 1,
              dependencies: 1000,
              last_release: null,
              risk_score: 0.91,
              bucket: "high",
            },
          ],
          total: 2,
          limit: 30,
          offset: 0,
          hasMore: true,
        }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          items: [
            {
              package_id: "low",
              package_name: "steady",
              maintainers: 8,
              dependencies: 1,
              last_release: "2026-01-01T00:00:00Z",
              risk_score: 0.12,
              bucket: "low",
            },
          ],
          total: 2,
          limit: 30,
          offset: 1,
          hasMore: false,
        }),
      } as Response);

    render(<RiskAnalysisPage />);

    expect(await screen.findByText("risky")).toBeInTheDocument();
    expect(screen.getByText("high")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /load more/i }));
    expect(await screen.findByText("steady")).toBeInTheDocument();
    expect(screen.getByText("low")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenLastCalledWith("/api/risk/ranked?limit=30&offset=1");
  });

  it("RiskAnalysisPage renders API errors and empty states", async () => {
    fetchMock.mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: async () => ({ error: "database unavailable" }),
    } as Response);

    const { unmount } = render(<RiskAnalysisPage />);
    expect(await screen.findByText("database unavailable")).toBeInTheDocument();
    unmount();

    fetchMock.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        items: [],
        total: 0,
        limit: 30,
        offset: 0,
        hasMore: false,
      }),
    } as Response);

    render(<RiskAnalysisPage />);
    expect(await screen.findByText(/No packages returned/i)).toBeInTheDocument();
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
