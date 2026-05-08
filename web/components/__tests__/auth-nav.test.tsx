import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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
    Object.defineProperty(window, "location", {
      configurable: true,
      value: { href: "http://localhost/" },
    });
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

  it("falls back to email or Account for the account label", async () => {
    fetchMock.mockResolvedValueOnce({
      json: async () => ({
        user: {
          id: "u1",
          email: "dev@example.com",
          displayName: null,
          avatarUrl: null,
        },
      }),
    } as Response);

    const { unmount } = render(<AuthNav />);
    expect(await screen.findByRole("link", { name: /dev@example.com/i })).toBeInTheDocument();
    unmount();

    fetchMock.mockResolvedValueOnce({
      json: async () => ({
        user: {
          id: "u2",
          email: null,
          displayName: null,
          avatarUrl: null,
        },
      }),
    } as Response);

    render(<AuthNav />);
    expect(await screen.findByRole("link", { name: /account/i })).toBeInTheDocument();
  });

  it("navigates to provider login endpoints", async () => {
    render(<AuthNav />);

    fireEvent.click(await screen.findByRole("button", { name: /gmail/i }));
    expect(window.location.href).toBe("/api/auth/google");

    fireEvent.click(screen.getByRole("button", { name: /github/i }));
    expect(window.location.href).toBe("/api/auth/github");
  });

  it("logs out and clears the account", async () => {
    fetchMock
      .mockResolvedValueOnce({
        json: async () => ({
          user: {
            id: "u1",
            email: "dev@example.com",
            displayName: "Dev",
            avatarUrl: null,
          },
        }),
      } as Response)
      .mockResolvedValueOnce({ json: async () => ({ ok: true }) } as Response);

    render(<AuthNav />);
    fireEvent.click(await screen.findByRole("button", { name: /log out/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenLastCalledWith("/api/auth/logout", { method: "POST" });
      expect(window.location.href).toBe("/");
    });
  });

  it("shows logged-out controls when the session lookup fails", async () => {
    fetchMock.mockRejectedValueOnce(new Error("offline"));

    render(<AuthNav />);
    expect(await screen.findByRole("button", { name: /gmail/i })).toBeInTheDocument();
  });
});
