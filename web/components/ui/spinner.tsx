import { Loader2 } from "lucide-react";

import { cn } from "@/lib/utils";

const sizeClass = {
  sm: "size-4",
  md: "size-6",
  lg: "size-8",
} as const;

/**
 * Animated loading indicator (Lucide Loader2 + Tailwind spin).
 * Use PageSpinner for centered full-width placeholder regions.
 */

export type SpinnerProps = {
  /** Additional classes on the SVG (e.g. `text-primary`) */
  className?: string;
  size?: keyof typeof sizeClass;
};

export function Spinner({ className, size = "md" }: SpinnerProps) {
  return (
    <Loader2
      aria-hidden
      className={cn("animate-spin", sizeClass[size], className)}
    />
  );
}

export type PageSpinnerProps = {
  className?: string;
  /** Visible helper text below the spinner */
  label?: string;
};

/** Centered spinner + caption for cards and tab content */
export function PageSpinner({ className, label = "Loading…" }: PageSpinnerProps) {
  return (
    <div
      role="status"
      aria-live="polite"
      aria-busy="true"
      className={cn(
        "flex min-h-[6rem] w-full flex-col items-center justify-center gap-3 py-6 text-muted-foreground",
        className,
      )}
    >
      <Spinner size="lg" className="text-primary" />
      <span className="text-sm">{label}</span>
    </div>
  );
}
