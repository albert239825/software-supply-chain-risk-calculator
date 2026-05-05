/** Rows returned by `/api/packages/:id/graph` */

export type DependencyGraphEdgeRow = {
  from_package_id: string;
  from_version_id: string;
  to_package_id: string;
  from_package: string;
  from_version: string;
  to_package: string;
  version_spec: string | null;
  dep_kind: string | null;
  depth: number;
};

export type DependencyExplorerFgNode = {
  id: string;
  uuid: string;
  name: string;
  subtitle: string;
  tier: number;
  isRoot: boolean;
  val: number;
  color: string;
};

export type DependencyExplorerFgLink = {
  source: string;
  target: string;
  label: string;
  depKinds: string[];
};

const ROOT_PKG = '#2563eb';
const FIRST_ORDER_PKG = '#7c3aed';
const SECOND_ORDER_PKG = '#64748b';
const ORPHAN_PKG = '#94a3b8';

function normalizeDepKind(kind: string | null): string {
  return (kind ?? 'dependency').trim() || 'dependency';
}

function coerceDepth(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  const n = Number(typeof v === 'string' ? v.trim() : v);
  return Number.isFinite(n) ? n : null;
}

export function coerceDependencyEdgeRows(rows: unknown): DependencyGraphEdgeRow[] {
  if (!Array.isArray(rows)) return [];
  const out: DependencyGraphEdgeRow[] = [];

  for (const item of rows) {
    if (item === null || typeof item !== 'object') continue;
    const r = item as Record<string, unknown>;
    const dep = coerceDepth(r.depth);
    const from_package_id = asNonEmptyStr(r.from_package_id);
    const from_version_id = asNonEmptyStr(r.from_version_id);
    const to_package_id = asNonEmptyStr(r.to_package_id);
    const from_package = asNonEmptyStr(r.from_package);
    const from_version = asNonEmptyStr(r.from_version);
    const to_package = asNonEmptyStr(r.to_package);
    if (
      dep === null ||
      dep <= 0 ||
      !Number.isFinite(dep) ||
      !from_package_id ||
      !from_version_id ||
      !to_package_id ||
      !from_package ||
      !from_version ||
      !to_package
    ) {
      continue;
    }
    out.push({
      from_package_id,
      from_version_id,
      to_package_id,
      from_package,
      from_version,
      to_package,
      version_spec:
        r.version_spec === null || r.version_spec === undefined ? null : String(r.version_spec),
      dep_kind:
        r.dep_kind === null || r.dep_kind === undefined ? null : String(r.dep_kind),
      depth: Math.min(Math.floor(dep), 98),
    });
  }

  return out;
}

function asNonEmptyStr(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

/** If only one package appears as `from_*` at hop depth==1, that is almost always the explorer root package. */
export function inferRootPackageFromEdges(rows: DependencyGraphEdgeRow[]): string | null {
  const depth1Parents = rows
    .filter((r) => r.depth === 1)
    .map((r) => r.from_package_id.trim())
    .filter(Boolean);
  const set = new Set(depth1Parents);
  if (set.size !== 1) return null;
  return [...set][0] ?? null;
}

function buildOutgoing(rows: DependencyGraphEdgeRow[]): Map<string, Set<string>> {
  const outgoing = new Map<string, Set<string>>();
  for (const r of rows) {
    const a = r.from_package_id.trim();
    const b = r.to_package_id.trim();
    if (!a || !b || a === b) continue;

    let s = outgoing.get(a);
    if (!s) {
      s = new Set();
      outgoing.set(a, s);
    }
    s.add(b);
  }

  return outgoing;
}

/** Shortest hops from root along edges `owner → depended-on`; capped at 2 for styling. */
function tiersFromRoot(
  rootPkg: string,
  outgoing: Map<string, Set<string>>,
): Map<string, number> {
  const tier = new Map<string, number>();
  const q: string[] = [];
  tier.set(rootPkg, 0);
  q.push(rootPkg);

  while (q.length) {
    const u = q.shift()!;
    const tu = tier.get(u)!;
    if (tu >= 2) continue;

    for (const v of outgoing.get(u) ?? []) {
      const next = tu + 1;
      const prev = tier.get(v);
      if (prev === undefined || prev > next) {
        tier.set(v, Math.min(next, 2));
        if (next < 2) q.push(v);
      }
    }
  }

  return tier;
}

function fallbackTierByIncomingDepth(rows: DependencyGraphEdgeRow[]): Map<string, number> {
  const minTo = new Map<string, number>();
  for (const r of rows) {
    const t = r.to_package_id.trim();
    const d = Math.min(r.depth, 2);
    const prev = minTo.get(t);
    if (prev === undefined || d < prev) minTo.set(t, d);
  }

  /** Sources that never appear as targets get a tier from their shallowest outbound edge minus 1. */
  const minFrom = new Map<string, number>();
  for (const r of rows) {
    const f = r.from_package_id.trim();
    const d = r.depth;
    const prev = minFrom.get(f);
    if (prev === undefined || d < prev) minFrom.set(f, d);
  }

  const all = new Set<string>();
  for (const r of rows) {
    all.add(r.from_package_id.trim());
    all.add(r.to_package_id.trim());
  }

  const tier = new Map<string, number>();
  for (const id of all) {
    const t =
      minTo.get(id) ??
      (() => {
        const outbound = minFrom.get(id);
        if (outbound === undefined) return 1;
        const guess = outbound - 1;
        return Math.min(Math.max(guess, 1), 2);
      })();
    tier.set(id, Math.min(Math.max(t, 1), 2));
  }

  return tier;
}

function styleForTier(tierNum: number, isRoot: boolean): { color: string; val: number } {
  if (isRoot) return { color: ROOT_PKG, val: 6 };

  /** Unknown / disconnected packages */
  if (tierNum <= 0) return { color: ORPHAN_PKG, val: 2.75 };

  if (tierNum === 1) return { color: FIRST_ORDER_PKG, val: 5 };
  if (tierNum === 2) return { color: SECOND_ORDER_PKG, val: 3.75 };

  return { color: ORPHAN_PKG, val: 3 };
}

/**
 * Collapses API rows into one node per **`packages.id`** plus merged package→package links.
 * Rows should already be limited to hops 1..2 (`maxOrder=2`).
 */
export function dependencyRowsToPackageForceGraph(
  rows: DependencyGraphEdgeRow[],
  explicitRootPackageId: string | null,
): { nodes: DependencyExplorerFgNode[]; links: DependencyExplorerFgLink[] } {
  const inferred = inferRootPackageFromEdges(rows);
  const root = (explicitRootPackageId ?? inferred ?? '').trim();

  const outgoing = buildOutgoing(rows);

  const tierMap =
    root ? tiersFromRoot(root, outgoing) : fallbackTierByIncomingDepth(rows);

  const edgeMap = new Map<string, { specs: Set<string>; depKinds: Set<string> }>();

  for (const row of rows) {
    const from = row.from_package_id.trim();
    const to = row.to_package_id.trim();
    if (!from || !to || from === to) continue;

    const key = `${from}->${to}`;
    let bucket = edgeMap.get(key);
    if (!bucket) {
      bucket = { specs: new Set(), depKinds: new Set() };
      edgeMap.set(key, bucket);
    }
    const spec = `${normalizeDepKind(row.dep_kind)} (${row.version_spec ?? '*'})`;
    bucket.specs.add(spec);
    bucket.depKinds.add(normalizeDepKind(row.dep_kind));
  }

  const names = new Map<string, string>();
  const ids = new Set<string>();
  for (const r of rows) {
    names.set(r.from_package_id.trim(), r.from_package);
    names.set(r.to_package_id.trim(), r.to_package);
    ids.add(r.from_package_id.trim());
    ids.add(r.to_package_id.trim());
  }

  if (root) ids.add(root);

  const nodes: DependencyExplorerFgNode[] = [];
  for (const id of ids) {
    const isRoot = Boolean(root && id === root);
    let tier = isRoot ? 0 : tierMap.get(id) ?? 0;

    /** If BFS didn't reach a node (weird), fall back to incoming depth */
    if (!isRoot && (!tier || tier < 0)) {
      const inc = rows
        .filter((r) => r.to_package_id.trim() === id)
        .map((r) => r.depth);
      tier = inc.length ? Math.min(...inc) : 1;
      tier = Math.min(Math.max(tier, 1), 2);
    }

    const style = styleForTier(tier, isRoot);
    nodes.push({
      id: `p:${id}`,
      uuid: id,
      name: names.get(id) ?? id.slice(0, 8),
      subtitle: `package · ${id}`,
      tier: isRoot ? 0 : Math.min(Math.max(tier, 1), 2),
      isRoot,
      val: style.val,
      color: style.color,
    });
  }

  const links: DependencyExplorerFgLink[] = [];
  for (const [key, bucket] of edgeMap) {
    const arrow = key.indexOf('->');
    if (arrow === -1) continue;
    const from = key.slice(0, arrow).trim();
    const to = key.slice(arrow + 2).trim();
    if (!from || !to) continue;

    links.push({
      source: `p:${from}`,
      target: `p:${to}`,
      label: [...bucket.specs].sort().join('; '),
      depKinds: [...bucket.depKinds].sort(),
    });
  }

  return { nodes, links };
}
