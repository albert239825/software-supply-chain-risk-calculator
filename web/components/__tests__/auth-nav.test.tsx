import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AuthNav } from "@/components/auth/auth-nav";

const fetchMock = vi.fn();

describe("AuthNav", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    fetchMock.mockResolvedValue({
      json: async () => ({ user: null }),
    } as Response);
    vi.stubGlobal("fetch", fetchMock);
  });

  it("shows Gmail and GitHub when logged out", async () => {
    render(<AuthNav />);
    await waitFor(() =>
      expect(screen.getByRole("button", { name: /gmail/i })).toBeInTheDocument(),
    );
    expect(screen.getByRole("button", { name: /github/i })).toBeInTheDocument();
  });

  it("shows account link when user is present", async () => {
    fetchMock.mockResolvedValueOnce({
      json: async () => ({
        user: {
          id: "u1",
          email: null,
          displayName: "Taylor",
          avatarUrl: null,
        },
      }),
    } as Response);

    render(<AuthNav />);
    expect(
      await screen.findByRole("link", { name: /taylor/i }),
    ).toHaveAttribute("href", "/track");
  });
});
