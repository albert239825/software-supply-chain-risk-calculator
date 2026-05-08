import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

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
});
