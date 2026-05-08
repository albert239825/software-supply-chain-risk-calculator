import Link from "next/link";
import {
  Activity,
  Bell,
  GitBranch,
  PackageSearch,
  ShieldCheck,
} from "lucide-react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";

const pages = [
  {
    name: "Risk Analysis",
    blurb: "Rank packages by composite supply-chain risk and inspect weak signals.",
    href: "/risk",
    icon: Activity,
    color: "bg-red-50 text-red-700 border-red-100",
  },
  {
    name: "Graph Explorer",
    blurb: "Trace transitive dependency paths from a package seed.",
    href: "/graph",
    icon: GitBranch,
    color: "bg-cyan-50 text-cyan-700 border-cyan-100",
  },
  {
    name: "Tracked Dependencies",
    blurb: "Sign in and save packages you want to monitor over time.",
    href: "/track",
    icon: Bell,
    color: "bg-amber-50 text-amber-800 border-amber-100",
  },
  {
    name: "Package Browser",
    blurb: "Review versions, release dates, maintainers, and repo metadata.",
    href: "/packages",
    icon: PackageSearch,
    color: "bg-indigo-50 text-indigo-700 border-indigo-100",
  },
];

const stats = [
  { label: "Risk signals", value: "5" },
  { label: "Core SQL views", value: "10" },
  { label: "Tracked lists", value: "User" },
];

export default function Home() {
  return (
    <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-8 px-6 pb-16 pt-10">
      <section className="grid gap-8 lg:grid-cols-[1.2fr_0.8fr] lg:items-center">
        <div className="flex flex-col gap-6">
          <div className="inline-flex w-fit items-center gap-2 rounded-md border border-border bg-card px-3 py-1 text-sm font-medium text-muted-foreground">
            <ShieldCheck className="size-4" />
            Dependency intelligence for security reviews
          </div>
          <div className="flex flex-col gap-4">
            <h1 className="max-w-3xl text-4xl font-semibold tracking-tight text-foreground sm:text-5xl">
              Software supply-chain risk, mapped before it surprises you.
            </h1>
            <p className="max-w-2xl text-lg leading-8 text-muted-foreground">
              Explore package metadata, dependency graph structure, maintainer
              concentration, release staleness, and saved watch lists from one
              normalized PostgreSQL-backed app.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <Button asChild size="lg">
              <Link href="/track">
                <Bell />
                Track dependencies
              </Link>
            </Button>
            <Button asChild variant="outline" size="lg">
              <Link href="/graph">Open graph explorer</Link>
            </Button>
          </div>
        </div>

        <div className="rounded-lg border border-border bg-card p-5 shadow-sm">
          <div className="grid gap-4">
            <div className="grid grid-cols-3 gap-3">
              {stats.map((stat) => (
                <div key={stat.label} className="rounded-md border border-border bg-background p-3">
                  <p className="text-2xl font-semibold text-foreground">{stat.value}</p>
                  <p className="mt-1 text-xs font-medium text-muted-foreground">{stat.label}</p>
                </div>
              ))}
            </div>
            <div className="rounded-md border border-border bg-muted/50 p-4">
              <p className="text-sm font-semibold text-foreground">Watch-list ready</p>
              <p className="mt-1 text-sm leading-6 text-muted-foreground">
                Sign in with Gmail or GitHub, choose packages, and persist the
                dependencies each user wants to track.
              </p>
            </div>
          </div>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {pages.map((page) => {
          const Icon = page.icon;
          return (
            <Link key={page.name} href={page.href} className="group">
              <Card className="h-full border-border bg-card shadow-sm transition-colors hover:border-primary/40">
                <CardHeader>
                  <div className={`mb-3 flex size-10 items-center justify-center rounded-lg border ${page.color}`}>
                    <Icon className="size-5" />
                  </div>
                  <CardTitle className="text-lg">{page.name}</CardTitle>
                  <CardDescription className="leading-6">{page.blurb}</CardDescription>
                </CardHeader>
                <CardContent>
                  <span className="text-sm font-semibold text-primary group-hover:underline">
                    Open module
                  </span>
                </CardContent>
              </Card>
            </Link>
          );
        })}
      </section>
    </main>
  );
}
