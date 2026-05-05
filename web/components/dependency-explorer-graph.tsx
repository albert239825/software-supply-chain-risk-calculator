'use client';

import ForceGraph2D, { ForceGraphMethods } from 'react-force-graph-2d';
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import type {
  DependencyExplorerFgLink,
  DependencyExplorerFgNode,
} from '@/lib/graph/dependency-explorer-force-model';

const LINK_PALETTE = ['#0ea5e9', '#22c55e', '#eab308', '#f97316', '#a855f7', '#64748b'] as const;

function linkColor(depKinds: string[]): string {
  const k = (depKinds[0] ?? '').trim() || 'dependency';
  let h = 2166136261 >>> 0;
  for (let i = 0; i < k.length; i++) {
    h ^= k.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  return LINK_PALETTE[h % LINK_PALETTE.length];
}

export function DependencyExplorerGraph({
  graphData,
}: {
  graphData: {
    nodes: DependencyExplorerFgNode[];
    links: DependencyExplorerFgLink[];
  };
}) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const fgRef = useRef<
    ForceGraphMethods<DependencyExplorerFgNode, DependencyExplorerFgLink> | undefined
  >(undefined);
  const [dims, setDims] = useState({ w: 640, h: 480 });
  const [focus, setFocus] = useState<DependencyExplorerFgNode | null>(null);

  useLayoutEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const run = () => {
      const w = Math.max(240, Math.floor(el.clientWidth));
      const h = Math.max(320, Math.floor(el.clientHeight));
      setDims({ w, h });
    };
    run();
    const ro = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(run) : null;
    ro?.observe(el);
    window.addEventListener('resize', run);
    return () => {
      ro?.disconnect();
      window.removeEventListener('resize', run);
    };
  }, []);

  useEffect(() => {
    const fg = fgRef.current;
    if (!fg || graphData.nodes.length === 0) return undefined;
    const t = window.setTimeout(() => {
      fg.zoomToFit?.(520, 40);
      fg.d3ReheatSimulation?.();
    }, 60);
    return () => window.clearTimeout(t);
  }, [graphData]);

  const empty = graphData.nodes.length === 0;

  return (
    <div className="space-y-3">
      <div
        ref={wrapRef}
        className={
          empty
            ? 'flex min-h-[320px] w-full flex-col items-center justify-center rounded-lg border bg-muted/20 p-8 text-center'
            : 'relative h-[min(62vh,640px)] w-full min-h-[340px] overflow-hidden rounded-lg border bg-background'
        }
      >
        {empty ? (
          <div className="text-muted-foreground text-sm leading-relaxed">
            No dependency edges returned for this root. Pick another{' '}
            <span className="font-medium text-foreground">Quick pick</span> or try another version UUID.
          </div>
        ) : (
          <ForceGraph2D<DependencyExplorerFgNode, DependencyExplorerFgLink>
            ref={fgRef}
            graphData={graphData}
            width={dims.w}
            height={dims.h}
            cooldownTicks={80}
            d3VelocityDecay={0.35}
            nodeRelSize={4}
            linkDirectionalArrowLength={7}
            linkDirectionalArrowRelPos={0.92}
            linkDirectionalArrowColor={(link) =>
              linkColor(Array.isArray(link.depKinds) ? link.depKinds : [])
            }
            linkColor={(link) =>
              linkColor(Array.isArray(link.depKinds) ? link.depKinds : [])
            }
            linkWidth={0.8}
            linkLabel={(link) =>
              typeof link.label === 'string' && link.label.length <= 280
                ? link.label
                : `${String(link.label).slice(0, 280)}…`
            }
            linkDirectionalParticles={0}
            nodeLabel={(n) =>
              `<div style="max-width:360px;line-height:1.35">${n.name}<br/><span style="opacity:0.8;font-family:monospace;font-size:11px">${n.uuid}</span></div>`
            }
            nodeVal={(n) => n.val}
            nodeColor={(n) => n.color}
            onBackgroundClick={() => setFocus(null)}
            onNodeClick={(node) => setFocus(node)}
            enablePanInteraction
            enableZoomInteraction
          />
        )}
      </div>

      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-muted-foreground text-xs">
        <span>
          <span className="inline-block size-2 rounded-full bg-blue-600 align-middle" /> Root
        </span>
        <span>
          <span className="inline-block size-2 rounded-full bg-violet-600 align-middle" /> 1<sup>st</sup>-order deps
        </span>
        <span>
          <span className="inline-block size-2 rounded-full bg-slate-500 align-middle" /> 2<sup>nd</sup>-order deps
        </span>
        <span className="ml-auto shrink-0">Drag background to pan · scroll to zoom · hover edges for specs</span>
      </div>

      {focus && (
        <div className="rounded-lg border bg-muted/30 p-3 text-sm">
          <div className="font-medium">{focus.isRoot ? 'Root package' : `Tier ${focus.tier} package`}</div>
          <div className="mt-1 break-all text-muted-foreground">{focus.name}</div>
          <div className="mt-1 break-all text-muted-foreground text-xs">{focus.subtitle}</div>
          <div className="mt-2 font-mono text-muted-foreground text-xs break-all">{focus.uuid}</div>
        </div>
      )}
    </div>
  );
}
