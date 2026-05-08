import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import { getCurrentUser } from '@/lib/auth';
import {
  createDependencySets,
  extractPackageJsonDependencies,
  extractPackageLockDependencies,
  extractPyprojectDependencies,
  extractRequirementsDependencies,
  findDependencyManifestPaths,
  fetchPackageJson,
  fetchRepoFileText,
  getGitHubAccessToken,
} from '@/lib/github';

type ImportBody = {
  fullName?: unknown;
};

function normalizeFullName(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(trimmed) ? trimmed : null;
}

export async function POST(req: NextRequest) {
  try {
    const user = await getCurrentUser(req);
    if (!user) {
      return NextResponse.json({ error: 'login required' }, { status: 401 });
    }

    const token = await getGitHubAccessToken(user.id);
    if (!token) {
      return NextResponse.json(
        { error: 'log in with GitHub to import repositories' },
        { status: 403 },
      );
    }

    const body = (await req.json()) as ImportBody;
    const fullName = normalizeFullName(body.fullName);
    if (!fullName) {
      return NextResponse.json({ error: 'valid repo fullName is required' }, { status: 400 });
    }

    const manifests = await findDependencyManifestPaths(token, fullName);

    if (manifests.length === 0) {
      return NextResponse.json({
        repo: fullName,
        manifests: [],
        total: 0,
        matched: [],
        unmatched: [],
        message: 'No supported dependency files found in this repository.',
      });
    }

    const dependencies = createDependencySets();
    for (const manifest of manifests) {
      if (manifest.kind === 'package.json') {
        const packageJson = await fetchPackageJson(token, fullName, manifest.path);
        for (const dependencyName of extractPackageJsonDependencies(packageJson)) {
          dependencies.npm.add(dependencyName);
        }
        continue;
      }

      if (manifest.kind === 'package-lock.json') {
        const lockfile = await fetchPackageJson(token, fullName, manifest.path);
        for (const dependencyName of extractPackageLockDependencies(lockfile)) {
          dependencies.npm.add(dependencyName);
        }
        continue;
      }

      const text = await fetchRepoFileText(token, fullName, manifest.path);
      const extracted =
        manifest.kind === 'requirements.txt'
          ? extractRequirementsDependencies(text)
          : extractPyprojectDependencies(text);

      for (const dependencyName of extracted) {
        dependencies.pypi.add(dependencyName);
      }
    }

    const npmDependencies = [...dependencies.npm].sort((a, b) => a.localeCompare(b));
    const pypiDependencies = [...dependencies.pypi].sort((a, b) => a.localeCompare(b));
    const totalDependencies = npmDependencies.length + pypiDependencies.length;

    if (totalDependencies === 0) {
      return NextResponse.json({
        repo: fullName,
        manifests: manifests.map((manifest) => manifest.path),
        total: 0,
        matched: [],
        unmatched: [],
        message: 'Dependency files were found, but no dependencies were listed.',
      });
    }

    const { rows: matched } = await pool.query<{
      package_id: string;
      package_name: string;
      ecosystem: string;
      latest_version: string;
      latest_version_id: string | null;
    }>(
      `
      SELECT
        p.id AS package_id,
        p.name AS package_name,
        p.ecosystem,
        p.latest_version,
        (
          SELECT v.id
          FROM versions v
          WHERE v.package_id = p.id
            AND v.version = p.latest_version
          LIMIT 1
        ) AS latest_version_id
      FROM packages p
      WHERE (
          p.ecosystem = 'npm'
          AND lower(p.name) = ANY($1::text[])
        )
        OR (
          p.ecosystem = 'pypi'
          AND lower(p.name) = ANY($2::text[])
        )
      ORDER BY p.name ASC;
      `,
      [npmDependencies, pypiDependencies],
    );

    if (matched.length > 0) {
      await pool.query(
        `
        INSERT INTO user_tracked_dependencies (user_id, package_id, note)
        SELECT $1, package_id, $2
        FROM unnest($3::text[]) AS package_id
        ON CONFLICT (user_id, package_id)
        DO UPDATE SET
          note = EXCLUDED.note,
          updated_at = now();
        `,
        [
          user.id,
          `Imported from GitHub repo ${fullName}`,
          matched.map((row) => row.package_id),
        ],
      );
    }

    const matchedKeys = new Set(
      matched.map((row) => `${row.ecosystem}:${row.package_name.toLowerCase()}`),
    );
    const unmatched = [
      ...npmDependencies
        .filter((name) => !matchedKeys.has(`npm:${name}`))
        .map((name) => `npm:${name}`),
      ...pypiDependencies
        .filter((name) => !matchedKeys.has(`pypi:${name}`))
        .map((name) => `pypi:${name}`),
    ];

    return NextResponse.json({
      repo: fullName,
      manifests: manifests.map((manifest) => manifest.path),
      total: totalDependencies,
      matched,
      unmatched,
    });
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}
