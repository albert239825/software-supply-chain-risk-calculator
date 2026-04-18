import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

const pages = [
  {
    name: "Home",
    blurb: "Top risky packages, ecosystem counts, and search.",
    href: "/",
  },
  {
    name: "Graph Explorer",
    blurb: "Interactive dependency graph for any package.",
    href: "/graph",
  },
  {
    name: "Risk Analysis",
    blurb: "Ranked composite risk plus per-signal breakdowns.",
    href: "/risk",
  },
  {
    name: "Package Detail",
    blurb: "Versions, maintainers, and direct deps/dependents.",
    href: "/packages",
  },
];

export default function Home() {
  return (
    <main className="mx-auto flex w-full max-w-3xl flex-1 flex-col gap-8 px-6 py-16">
      <header className="flex flex-col gap-3">
        <h1 className="text-3xl font-semibold tracking-tight sm:text-4xl">
          Software Supply Chain Risk Scorer
        </h1>
        <p className="text-muted-foreground text-lg leading-7">
          A web app that lets security teams explore the NPM package ecosystem
          via an interactive dependency graph and a composite risk score,
          backed by a normalized PostgreSQL database.
        </p>
      </header>

      <Card>
        <CardHeader>
          <CardTitle>Planned pages</CardTitle>
          <CardDescription>
            Placeholders — wiring happens in subsequent PRs.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ul className="flex flex-col gap-3">
            {pages.map((page) => (
              <li key={page.name}>
                <a
                  href={page.href}
                  className="group flex flex-col gap-0.5 rounded-md border border-border p-3 transition-colors hover:bg-accent hover:text-accent-foreground"
                >
                  <span className="font-medium">{page.name}</span>
                  <span className="text-muted-foreground text-sm">
                    {page.blurb}
                  </span>
                </a>
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>
    </main>
  );
}
