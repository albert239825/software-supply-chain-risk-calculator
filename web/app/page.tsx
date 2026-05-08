import Link from "next/link";
import {
  Activity,
  Bell,
  GitBranch,
  PackageSearch,
  Search,
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

const modules = [
  {
    name: "Browse packages",
    blurb: "Start from the package catalog and review versions by release date.",
    href: "/packages",
    icon: PackageSearch,
  },
  {
    name: "Rank risk",
    blurb: "Compare packages using the composite risk score and supporting signals.",
    href: "/risk",
    icon: Activity,
  },
  {
    name: "Explore dependencies",
    blurb: "Open the graph explorer to inspect direct and transitive dependency paths.",
    href: "/graph",
    icon: GitBranch,
  },
  {
    name: "Track over time",
    blurb: "Log in and save the packages you want to revisit later.",
    href: "/track",
    icon: Bell,
  },
];

const workflow = [
  "Find a package in the database.",
  "Review its maintainers, versions, repo status, and dependency graph.",
  "Save high-priority dependencies to your personal tracking list.",
];

export default function Home() {
  return (
    <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-8 px-6 pb-16 pt-10">
      <section className="grid gap-6 lg:grid-cols-[1fr_360px]">
        <div className="rounded-lg border border-border bg-card p-6 shadow-sm">
          <div className="flex flex-col gap-5">
            <div className="inline-flex w-fit items-center gap-2 rounded-md border border-border bg-muted/50 px-3 py-1 text-sm font-medium text-muted-foreground">
              <ShieldCheck className="size-4" />
              Software supply-chain risk calculator
            </div>
            <div className="flex flex-col gap-3">
              <h1 className="max-w-3xl text-4xl font-semibold tracking-tight sm:text-5xl">
                Evaluate dependency risk from package data you already collected.
              </h1>
              <p className="max-w-2xl text-lg leading-8 text-muted-foreground">
                Use the app to inspect packages, compare risk signals, explore
                dependency paths, and save the dependencies each user wants to
                track.
              </p>
            </div>
            <div className="flex flex-wrap gap-3">
              <Button asChild size="lg">
                <Link href="/packages">
                  <Search />
                  Browse packages
                </Link>
              </Button>
              <Button asChild variant="outline" size="lg">
                <Link href="/track">
                  <Bell />
                  Track dependencies
                </Link>
              </Button>
            </div>
          </div>
        </div>

        <Card className="border-border bg-card shadow-sm">
          <CardHeader>
            <CardTitle>Typical workflow</CardTitle>
            <CardDescription>
              The main path through the risk review tools.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ol className="grid gap-3">
              {workflow.map((step, index) => (
                <li key={step} className="flex gap-3">
                  <span className="flex size-7 shrink-0 items-center justify-center rounded-md bg-secondary text-sm font-semibold text-secondary-foreground">
                    {index + 1}
                  </span>
                  <span className="text-sm leading-6 text-muted-foreground">{step}</span>
                </li>
              ))}
            </ol>
          </CardContent>
        </Card>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {modules.map((module) => {
          const Icon = module.icon;
          return (
            <Link key={module.name} href={module.href} className="group">
              <Card className="h-full border-border bg-card shadow-sm transition-colors hover:border-primary/40">
                <CardHeader>
                  <div className="mb-3 flex size-10 items-center justify-center rounded-md border border-border bg-muted/50 text-primary">
                    <Icon className="size-5" />
                  </div>
                  <CardTitle className="text-lg">{module.name}</CardTitle>
                  <CardDescription className="leading-6">{module.blurb}</CardDescription>
                </CardHeader>
                <CardContent>
                  <span className="text-sm font-semibold text-primary group-hover:underline">
                    Open
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
