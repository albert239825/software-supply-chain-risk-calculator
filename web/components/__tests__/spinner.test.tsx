import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { PageSpinner, Spinner } from "@/components/ui/spinner";

describe("Spinner", () => {
  it("renders an animated loader", () => {
    const { container } = render(<Spinner />);
    expect(container.querySelector("svg")).toBeTruthy();
  });

  it("PageSpinner exposes a polite status region", () => {
    render(<PageSpinner label="Patience…" />);
    expect(screen.getByRole("status")).toHaveTextContent("Patience…");
  });
});
