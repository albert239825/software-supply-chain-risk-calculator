/** Rows returned by `/api/packages/:id/graph` */

export type DependencyGraphEdgeRow = {
  from_version_id: string;
  to_package_id: string;
  from_package: string;
  from_version: string;
  to_package: string;
  version_spec: string | null;
  dep_kind: string | null;
};

export type DependencyExplorerFgNode = {
  id: string;
  uuid: string;
  name: string;
  subtitle: string;
  kind: "version" | "package";
  isRoot?: boolean;
  val: number;
  color: string;
};

export type DependencyExplorerFgLink = {
  source: string;
  target: string;
  label: string;
  depKinds: string[];
};

export function coerceDependencyEdgeRows(rows: unknown): DependencyGraphEdgeRow[] {
  if (!Array.isArray(rows)) return [];
  const out: DependencyGraphEdgeRow[] = [];

  for (const item of rows) {
    if (item === null || typeof item !== "object") continue;
    const r = item as Record<string, unknown>;
    const from_version_id = asNonEmptyStr(r.from_version_id);
    const to_package_id = asNonEmptyStr(r.to_package_id);
    const from_package = asNonEmptyStr(r.from_package);
    const from_version = asNonEmptyStr(r.from_version);
    const to_package = asNonEmptyStr(r.to_package);
    if (
      !from_version_id ||
      !to_package_id ||
      !from_package ||
      !from_version ||
      !to_package
    ) {
      continue;
    }
    out.push({
      from_version_id,
      to_package_id,
      from_package,
      from_version,
      to_package,
      version_spec: r.version_spec === null || r.version_spec === undefined ? null : String(r.version_spec),
      dep_kind:
        r.dep_kind === null || r.dep_kind === undefined ? null : String(r.dep_kind),
    });
  }

  return out;
}

function asNonEmptyStr(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

const VERSION_COLOR = "#7c3aed";
const PACKAGE_COLOR = "#64748b";
const ROOT_COLOR = "#2563eb";

function normalizeDepKind(kind: string | null): string {
  return (kind ?? "dependency").trim() || "dependency";
}

/** Build `{ nodes, links }` where version rows are intermediate sources and destinations are packages. */
export function dependencyRowsToForceGraph(
  rows: DependencyGraphEdgeRow[],
  rootVersionId: string,
): { nodes: DependencyExplorerFgNode[]; links: DependencyExplorerFgLink[] } {
  const rootNorm = rootVersionId.trim();

  /** version id → label */
  const versionNodes = new Map<string, { name: string; subtitle: string; isRoot: boolean }>();

  /** package id → package name label */
  const packageNodes = new Map<string, { name: string; subtitle: string }>();

  const edgeMap = new Map<
    string,
    { specs: Set<string>; depKinds: Set<string> }
  >();

  for (const row of rows) {
    const vId = row.from_version_id.trim();
    const pId = row.to_package_id.trim();
    const isRoot = rootNorm !== "" && vId === rootNorm;
    const prevV = versionNodes.get(vId);

    versionNodes.set(vId, {
      name: `${row.from_package}@${row.from_version}`,
      subtitle: `version · ${vId}`,
      isRoot: Boolean(prevV?.isRoot || isRoot),
    });

    packageNodes.set(pId, {
      name: row.to_package,
      subtitle: `package · ${pId}`,
    });

    const key = `${vId}->${pId}`;
    let bucket = edgeMap.get(key);
    if (!bucket) {
      bucket = { specs: new Set(), depKinds: new Set() };
      edgeMap.set(key, bucket);
    }
    const spec = `${normalizeDepKind(row.dep_kind)} (${row.version_spec ?? "*"})`;
    bucket.specs.add(spec);
    bucket.depKinds.add(normalizeDepKind(row.dep_kind));
  }

  const nodes: DependencyExplorerFgNode[] = [];

  for (const [uuid, meta] of versionNodes) {
    const id = `v:${uuid}`;
    const isRoot = meta.isRoot;
    nodes.push({
      id,
      uuid,
      name: meta.name,
      subtitle: meta.subtitle,
      kind: "version",
      isRoot,
      val: isRoot ? 5 : 3.25,
      color: isRoot ? ROOT_COLOR : VERSION_COLOR,
    });
  }

  for (const [uuid, meta] of packageNodes) {
    const id = `p:${uuid}`;
    nodes.push({
      id,
      uuid,
      name: meta.name,
      subtitle: meta.subtitle,
      kind: "package",
      val: 2,
      color: PACKAGE_COLOR,
    });
  }

  const links: DependencyExplorerFgLink[] = [];
  for (const [key, bucket] of edgeMap) {
    const [fromV, toP] = key.split("->");
    if (!fromV || !toP) continue;
    const sortedSpecs = [...bucket.specs].sort();
    const sortedKinds = [...bucket.depKinds].sort();
    links.push({
      source: `v:${fromV}`,
      target: `p:${toP}`,
      label: sortedSpecs.join("; "),
      depKinds: sortedKinds,
    });
  }

  return { nodes, links };
}
