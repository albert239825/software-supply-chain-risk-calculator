import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import TrackPage from "@/app/track/page";

const fetchMock = vi.fn();

describe("TrackPage", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    fetchMock.mockResolvedValue({
      json: async () => ({ user: null }),
    } as Response);
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("shows the sign-in panel when logged out", async () => {
    render(<TrackPage />);
    expect(await screen.findByText(/^log in$/i)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /continue with gmail/i }),
    ).toBeInTheDocument();
  });

  it("shows the tracked workspace after auth succeeds", async () => {
    fetchMock.mockImplementation((input: RequestInfo) => {
      const u = typeof input === "string" ? input : (input as Request).url;
      if (u.includes("/api/auth/me")) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            user: {
              id: "u1",
              email: "me@gmail.com",
              displayName: "Me",
              avatarUrl: null,
            },
          }),
        } as Response);
      }
      if (u.includes("/api/tracked-dependencies")) {
        return Promise.resolve({
          ok: true,
          json: async () => [],
        } as Response);
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({}),
      } as Response);
    });

    render(<TrackPage />);
    expect(
      await screen.findByRole("heading", { name: /tracked dependencies/i }),
    ).toBeInTheDocument();
    expect(screen.getByText(/signed in as me/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /load github repos/i })).toBeDisabled();
    expect(screen.getByRole("button", { name: /log in with github/i })).toBeInTheDocument();
  });

  it("clears all tracked packages after confirmation", async () => {
    const user = userEvent.setup();
    vi.stubGlobal("confirm", vi.fn(() => true));

    const trackedRow = {
      id: "t1",
      package_id: "pack-1",
      note: null,
      created_at: "2020-01-01T00:00:00.000Z",
      updated_at: "2020-01-01T00:00:00.000Z",
      package_name: "left-pad",
      ecosystem: "npm",
      description: null,
      latest_version: "1.0.0",
      latest_version_id: null,
      last_release: null,
      has_repository: true,
      maintainer_count: 1,
      fanout_direct: 0,
      fanin_dependents: 0,
      staleness_years: 0,
      risk_score: 0.2,
      risk_bucket: "low" as const,
      checked_at: "2024-01-01T00:00:00.000Z",
    };

    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
    fetchMock.mockImplementation((input: RequestInfo, init?: RequestInit) => {
      const raw = typeof input === "string" ? input : (input as Request).url;

      if (raw.includes("/api/auth/me")) {
        return Promise.resolve({
          ok: true,
          json: async () => ({
            user: {
              id: "u1",
              email: "me@gmail.com",
              displayName: "Me",
              avatarUrl: null,
            },
          }),
        } as Response);
      }
      if (raw.includes("tracked-dependencies") && init?.method === "DELETE") {
        return Promise.resolve({
          ok: true,
          json: async () => ({ deleted: 1 }),
        } as Response);
      }
      if (raw.includes("tracked-dependencies")) {
        return Promise.resolve({
          ok: true,
          json: async () => [trackedRow],
        } as Response);
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({}),
      } as Response);
    });

    render(<TrackPage />);
    await screen.findByRole("heading", { name: /tracked dependencies/i });
    expect(await screen.findByText(/left-pad/i)).toBeInTheDocument();

    const clearBtn = screen.getByRole("button", {
      name: /clear all tracked packages/i,
    });
    expect(clearBtn).toBeInTheDocument();
    await user.click(clearBtn);

    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/tracked-dependencies"),
      expect.objectContaining({ method: "DELETE" }),
    );

    expect(
      await screen.findByText(/no tracked dependencies yet/i),
    ).toBeInTheDocument();
  });
});
