"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";

const navItems = [
  { href: "/risk", label: "Risk" },
  { href: "/graph", label: "Graph" },
  { href: "/stats", label: "Stats" },
  { href: "/packages", label: "Packages" },
  { href: "/track", label: "Tracked" },
  { href: "/maintainers", label: "Maintainers" },
  { href: "/abandoned", label: "Abandoned" },
  { href: "/no-repo", label: "No Repo" },
];

function isActive(pathname: string, href: string) {
  return pathname === href || pathname.startsWith(`${href}/`);
}

export function SiteNav() {
  const pathname = usePathname();

  return (
    <div className="flex min-w-0 flex-1 items-center gap-1 overflow-x-auto">
      {navItems.map((item) => {
        const active = isActive(pathname, item.href);
        return (
          <Link
            key={item.href}
            href={item.href}
            aria-current={active ? "page" : undefined}
            className={cn(
              "relative shrink-0 rounded-md px-2.5 py-1.5 font-medium text-muted-foreground transition-colors hover:bg-muted/70 hover:text-foreground",
              active &&
                "bg-muted text-foreground shadow-sm after:absolute after:inset-x-2.5 after:bottom-0 after:h-0.5 after:rounded-full after:bg-primary",
            )}
          >
            {item.label}
          </Link>
        );
      })}
    </div>
  );
}
