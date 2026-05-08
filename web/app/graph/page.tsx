'use client';

import {
  coerceDependencyEdgeRows,
  dependencyRowsToPackageForceGraph,
  type DependencyExplorerFgLink,
  type DependencyExplorerFgNode,
} from '@/lib/graph/dependency-explorer-force-model';
import dynamic from 'next/dynamic';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { PageSpinner, Spinner } from '@/components/ui/spinner';
import { cn } from '@/lib/utils';

const DependencyExplorerGraph = dynamic(
  () =>
    import('@/components/dependency-explorer-graph').then((m) => m.DependencyExplorerGraph),
  {
    ssr: false,
    loading: () => (
      <div className="flex h-[min(62vh,640px)] min-h-[340px] w-full items-center justify-center rounded-lg border bg-muted/30">
        <PageSpinner className="min-h-0 py-4" label="Loading visualization…" />
      </div>
    ),
  },
);

type PackageSearchHit = {
  package_id: string;
  package_name: string;
  ecosystem: string;
  latest_version: string;
  latest_version_id: string;
};

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
  rootPackageId: string | null;
  edges: ReturnType<typeof coerceDependencyEdgeRows>;
};

export default function GraphExplorer() {
  const [nameQuery, setNameQuery] = useState('');
  const [suggestionsOpen, setSuggestionsOpen] = useState(false);
  const [suggestions, setSuggestions] = useState<PackageSearchHit[]>([]);
  const [suggestBusy, setSuggestBusy] = useState(false);

  const [manualVersionId, setManualVersionId] = useState('');
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

    return dependencyRowsToPackageForceGraph(explorer.edges, explorer.rootPackageId);
  }, [explorer]);

  const loadGraph = useCallback(
    async (versionId: string, rootPackageHint: string | null) => {
      const trimmed = versionId.trim();
      if (!trimmed) return;

      setManualVersionId(trimmed);
      setLoading(true);
      setError(null);
      setExplorer(null);
      setSuggestionsOpen(false);
      try {
        const res = await fetch(
          `/api/packages/${encodeURIComponent(trimmed)}/graph?maxOrder=2`,
        );

        const payload: unknown = await res.json();

        if (
          !res.ok ||
          (payload !== null && typeof payload === 'object' && 'error' in payload)
        ) {
          const msg =
            payload !== null && typeof payload === 'object' && 'error' in payload
              ? String((payload as { error?: string }).error)
              : 'Not found or error fetching graph';

          throw new Error(msg);
        }

        if (!Array.isArray(payload)) {
          throw new Error('Graph API returned an unexpected response');
        }

        setExplorer({
          rootVersionId: trimmed,
          rootPackageId: rootPackageHint?.trim() ?? null,
          edges: coerceDependencyEdgeRows(payload),
        });
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const versionId = params.get('versionId');
    const packageId = params.get('packageId');

    if (versionId) {
      const timeout = window.setTimeout(() => {
        void loadGraph(versionId, packageId);
      }, 0);

      return () => window.clearTimeout(timeout);
    }

    return undefined;
  }, [loadGraph]);

  /** Package name autocomplete */
  useEffect(() => {
    const q = nameQuery.trim();
    if (q.length < 2) {
      return undefined;
    }

    const ctrl = new AbortController();
    const timer = window.setTimeout(async () => {
      try {
        setSuggestBusy(true);
        const res = await fetch(
          `/api/graph/search?q=${encodeURIComponent(q)}`,
          { signal: ctrl.signal },
        );
        const body: unknown = await res.json();
        if (!res.ok || !Array.isArray(body)) {
          setSuggestions([]);
        } else {
          setSuggestions(body as PackageSearchHit[]);
        }
      } catch {
        setSuggestions([]);
      } finally {
        setSuggestBusy(false);
      }
    }, 260);

    return () => {
      ctrl.abort();
      window.clearTimeout(timer);
    };
  }, [nameQuery]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch('/api/graph/seeds?limit=18');
        const payload: unknown = await res.json();

        if (
          !res.ok ||
          (payload !== null && typeof payload === 'object' && 'error' in payload)
        ) {
          const msg =
            payload !== null && typeof payload === 'object' && 'error' in payload
              ? String((payload as { error?: string }).error)
              : 'Could not load example roots';

          throw new Error(msg);
        }

        if (!Array.isArray(payload)) throw new Error('Unexpected seeds response');

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

  const edgeSummary = explorer ? `${explorer.edges.length} edge rows (hops ≤ 2)` : '';

  return (
    <main className="mx-auto max-w-6xl space-y-6 px-6 py-16">
      <Card className="relative z-10 overflow-visible">
        <CardHeader>
          <CardTitle>Graph Explorer</CardTitle>
          <CardDescription>
            Search by package name, or paste a{' '}
            <code className="rounded bg-muted px-1 py-0.5 text-xs">
              versions.id
            </code>{' '}
            for advanced mode. Edges shown are&nbsp;
            <span className="font-medium text-foreground">
              direct (1<sup>st</sup>‑order)
            </span>{' '}
            and&nbsp;
            <span className="font-medium text-foreground">
              one step beyond (2<sup>nd</sup>‑order)
            </span>
            {' — '}
            one node per canonical package so the diagram stays readable.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6 overflow-visible">
          <div className="relative z-20 space-y-2">
            <label className="text-muted-foreground text-xs leading-none font-medium uppercase">
              Find package
            </label>

            <div className="relative">
              <Input
                role="combobox"
                aria-expanded={suggestionsOpen}
                aria-autocomplete="list"
                value={nameQuery}
                onChange={(e) => {
                  const v = e.target.value;

                  setNameQuery(v);

                  if (v.trim().length < 2) {
                    setSuggestions([]);
                    setSuggestBusy(false);
                  }
                  setSuggestionsOpen(true);
                }}
                onFocus={() => {
                  if (nameQuery.trim().length >= 2) setSuggestionsOpen(true);
                }}

                onBlur={() => window.setTimeout(() => setSuggestionsOpen(false), 120)}
                placeholder="Type a package name (e.g. express, boto3)"
                autoComplete="off"
                spellCheck={false}
              />

              {suggestionsOpen && nameQuery.trim().length >= 2 && (
                <ul
                  role="listbox"
                  className="absolute z-50 mt-1 max-h-64 w-full overflow-auto rounded-lg border bg-popover p-1 shadow-lg"
                >
                  {suggestBusy ? (
                    <li className="text-muted-foreground flex items-center gap-2 px-3 py-2 text-sm">
                      <Spinner size="sm" className="text-primary" />
                      Searching…
                    </li>
                  ) : suggestions.length === 0 ? (
                    <li className="text-muted-foreground px-3 py-2 text-sm">No matches</li>
                  ) : (
                    suggestions.map((hit) => (
                      <li key={`${hit.ecosystem}-${hit.package_id}`}>
                        <button
                          type="button"
                          className="flex w-full flex-col rounded-md px-3 py-2 text-left hover:bg-accent"
                          onMouseDown={(e) => e.preventDefault()}
                          disabled={loading}
                          onClick={() => {
                            setNameQuery(
                              `${hit.ecosystem}:${hit.package_name} @${hit.latest_version}`,
                            );
                            void loadGraph(hit.latest_version_id, hit.package_id);
                          }}
                        >
                          <span className="font-medium">
                            <span className="text-muted-foreground">{hit.ecosystem}</span>{' '}
                            {hit.package_name}{' '}
                            <span className="font-normal text-muted-foreground">
                              @{hit.latest_version}
                            </span>
                          </span>

                          <span className="mt-1 font-mono text-[10px] text-muted-foreground break-all opacity-75">
                            {hit.package_id}
                          </span>
                        </button>
                      </li>
                    ))
                  )}
                </ul>
              )}
            </div>
          </div>

          <details className="rounded-lg border px-3 py-2">
            <summary className="cursor-pointer text-sm hover:underline">
              Advanced: explore by root version UUID
            </summary>
            <div className="mt-3 mb-2 flex flex-wrap gap-2">
              <Input
                value={manualVersionId}
                onChange={(e) => setManualVersionId(e.target.value)}
                placeholder="Root version UUID (versions.id)"
                className="min-w-[200px] flex-1 font-mono text-sm"
              />
              <Button
                type="button"
                onClick={() => void loadGraph(manualVersionId, null)}
                disabled={loading || !manualVersionId.trim()}
              >
                {loading ? (
                  <span className="inline-flex items-center justify-center gap-2">
                    <Spinner size="sm" className="text-primary" />
                    Explore
                  </span>
                ) : (
                  'Explore'
                )}
              </Button>
            </div>
          </details>

          {loading && (
            <PageSpinner className="min-h-[7rem]" label="Loading graph…" />
          )}
          {error && <div className="text-destructive text-sm">{error}</div>}

          {explorer && (
            <>
              <div className="text-muted-foreground text-xs">
                {edgeSummary}

                {explorer.edges.length > 0
                  ? ` · ${graphModel.nodes.length} packages · ${graphModel.links.length} links`
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

      <Card className="relative z-0">
        <CardHeader>
          <CardTitle className="text-base">Quick picks</CardTitle>
          <CardDescription>
            High fan-out roots (many direct dependencies). Opens the same 2‑hop explorer.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {!seedsDone && (
            <div className="text-muted-foreground flex items-center gap-2 py-4 text-sm">
              <Spinner size="sm" className="text-primary" />
              Loading examples…
            </div>
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
                  onClick={() => {
                    setNameQuery(`${s.ecosystem}:${s.package_name}@${s.version}`);
                    void loadGraph(s.version_id, s.package_id);
                  }}
                  className={cn(
                    'w-full rounded-lg border bg-card px-3 py-2.5 text-left text-sm shadow-sm transition-colors',
                    'hover:bg-muted/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
                    loading && 'pointer-events-none opacity-60',
                  )}
                >
                  <div className="font-medium">
                    <span className="text-muted-foreground">{s.ecosystem}</span>
                    {' · '}
                    {s.package_name}

                    <span className="text-muted-foreground"> @{s.version}</span>

                    <span className="float-right font-normal text-muted-foreground text-xs tabular-nums">
                      {s.dependency_count} deps
                    </span>
                  </div>
                  <div className="mt-1 space-y-0.5 break-all font-mono text-[11px] text-muted-foreground leading-snug">
                    <div>
                      package_id{' '}
                      <span className="text-foreground/80">{s.package_id}</span>
                    </div>
                    <div>
                      root{' '}
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
