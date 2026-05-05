"use client";

import {
  coerceDependencyEdgeRows,
  dependencyRowsToForceGraph,
  type DependencyExplorerFgLink,
  type DependencyExplorerFgNode,
  type DependencyGraphEdgeRow,
} from "@/lib/graph/dependency-explorer-force-model";
import dynamic from "next/dynamic";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

const DependencyExplorerGraph = dynamic(
  () =>
    import("@/components/dependency-explorer-graph").then((m) => m.DependencyExplorerGraph),
  {
    ssr: false,
    loading: () => (
      <div className="flex h-[min(62vh,640px)] min-h-[340px] w-full animate-pulse items-center justify-center rounded-lg border bg-muted/30 text-muted-foreground text-sm">
        Loading visualization…
      </div>
    ),
  },
);

type GraphSeed = {
  package_id: string;
  version_id: string;
  ecosystem: string;
  package_name: string;
  version: string;
  dependency_count: number;
};

type ExplorerLoad = {
  rootVersionId: string;
  edges: DependencyGraphEdgeRow[];
};

export default function GraphExplorer() {
  const [rootVersionId, setRootVersionId] = useState("");
  const [explorer, setExplorer] = useState<ExplorerLoad | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [seeds, setSeeds] = useState<GraphSeed[]>([]);
  const [seedsError, setSeedsError] = useState<string | null>(null);
  const [seedsDone, setSeedsDone] = useState(false);

  const graphModel = useMemo(() => {
    const emptyGraph: {
      nodes: DependencyExplorerFgNode[];
      links: DependencyExplorerFgLink[];
    } = { nodes: [], links: [] };
    if (!explorer) return emptyGraph;
    return dependencyRowsToForceGraph(explorer.edges, explorer.rootVersionId);
  }, [explorer]);

  const loadGraph = useCallback(async (versionId: string) => {
    const trimmed = versionId.trim();
    if (!trimmed) return;

    setRootVersionId(trimmed);
    setLoading(true);
    setError(null);
    setExplorer(null);
    try {
      const res = await fetch(`/api/packages/${encodeURIComponent(trimmed)}/graph`);
      const payload: unknown = await res.json();

      if (
        !res.ok ||
        (payload !== null && typeof payload === "object" && "error" in payload)
      ) {
        const msg =
          payload !== null && typeof payload === "object" && "error" in payload
            ? String((payload as { error?: string }).error)
            : "Not found or error fetching graph";
        throw new Error(msg);
      }

      if (!Array.isArray(payload)) {
        throw new Error("Graph API returned an unexpected response");
      }

      setExplorer({
        rootVersionId: trimmed,
        edges: coerceDependencyEdgeRows(payload),
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch("/api/graph/seeds?limit=18");
        const payload: unknown = await res.json();

        if (
          !res.ok ||
          (payload !== null &&
            typeof payload === "object" &&
            "error" in payload)
        ) {
          const msg =
            payload !== null && typeof payload === "object" && "error" in payload
              ? String((payload as { error?: string }).error)
              : "Could not load example roots";
          throw new Error(msg);
        }

        if (!Array.isArray(payload)) throw new Error("Unexpected seeds response");

        if (!cancelled) setSeeds(payload as GraphSeed[]);
      } catch (e) {
        if (!cancelled)
          setSeedsError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setSeedsDone(true);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const edgeSummary = explorer ? `${explorer.edges.length} edge rows` : "";

  return (
    <main className="mx-auto max-w-6xl space-y-6 px-6 py-16">
      <Card>
        <CardHeader>
          <CardTitle>Graph Explorer</CardTitle>
          <CardDescription>
            Walk outbound dependencies starting from one version row. Paste the{" "}
            <span className="font-medium text-foreground">
              root version UUID
            </span>{" "}
            (
            <code className="rounded bg-muted px-1 py-0.5 text-xs">
              versions.id
            </code>
            ). The graph packs point from a version toward each dependency
            package; arrows aggregate multiple requirement lines when needed.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="flex flex-wrap gap-2">
            <Input
              value={rootVersionId}
              onChange={(e) => setRootVersionId(e.target.value)}
              placeholder="Root version UUID (versions.id)"
              className="min-w-[200px] flex-1 font-mono text-sm"
            />
            <Button
              type="button"
              onClick={() => void loadGraph(rootVersionId)}
              disabled={loading || !rootVersionId.trim()}
            >
              Explore
            </Button>
          </div>

          {loading && (
            <div className="text-muted-foreground text-sm">Loading…</div>
          )}
          {error && <div className="text-destructive text-sm">{error}</div>}

          {explorer && (
            <>
              <div className="text-muted-foreground text-xs">
                {edgeSummary}
                {explorer.edges.length > 0
                  ? ` · ${graphModel.nodes.length} nodes · ${graphModel.links.length} unique links`
                  : null}
              </div>

              <DependencyExplorerGraph graphData={graphModel} />

              <details className="group rounded-lg border bg-muted/20">
                <summary className="cursor-pointer select-none rounded-lg px-3 py-2 text-sm transition-colors hover:bg-muted/40">
                  Raw JSON (edge rows)
                </summary>
                <pre className="max-h-[min(360px,40vh)] overflow-auto whitespace-pre-wrap border-t px-3 py-2 font-mono text-[11px] leading-relaxed">
                  {JSON.stringify(explorer.edges, null, 2)}
                </pre>
              </details>
            </>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Quick picks</CardTitle>
          <CardDescription>
            High fan-out roots (many direct dependencies). Each row lists the
            canonical package ID and the version ID the graph API expects.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {!seedsDone && (
            <p className="text-muted-foreground text-sm">Loading examples…</p>
          )}
          {seedsDone && seedsError && (
            <p className="text-destructive text-sm">{seedsError}</p>
          )}
          {seedsDone && !seedsError && seeds.length === 0 && (
            <p className="text-muted-foreground text-sm">No seeded roots found.</p>
          )}
          <ul className="flex flex-col gap-2">
            {seeds.map((s) => (
              <li key={s.version_id}>
                <button
                  type="button"
                  disabled={loading}
                  onClick={() => void loadGraph(s.version_id)}
                  className={cn(
                    "w-full rounded-lg border bg-card px-3 py-2.5 text-left text-sm shadow-sm transition-colors",
                    "hover:bg-muted/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                    loading && "pointer-events-none opacity-60",
                  )}
                >
                  <div className="font-medium">
                    <span className="text-muted-foreground">{s.ecosystem}</span>
                    {" · "}
                    {s.package_name}
                    <span className="text-muted-foreground">
                      {" "}
                      @{s.version}
                    </span>
                    <span className="float-right font-normal text-muted-foreground text-xs tabular-nums">
                      {s.dependency_count} deps
                    </span>
                  </div>
                  <div className="mt-1 space-y-0.5 break-all font-mono text-[11px] text-muted-foreground leading-snug">
                    <div>
                      package_id{" "}
                      <span className="text-foreground/80">{s.package_id}</span>
                    </div>
                    <div>
                      root{" "}
                      <span className="text-foreground/80">{s.version_id}</span>
                    </div>
                  </div>
                </button>
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>
    </main>
  );
}
