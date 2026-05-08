import pool from './db';

export type GitHubRepo = {
  id: number;
  name: string;
  full_name: string;
  private: boolean;
  default_branch: string;
  html_url: string;
  updated_at: string;
};

type GitHubContent = {
  content?: string;
  encoding?: string;
};

type GitHubTree = {
  truncated?: boolean;
  tree?: Array<{
    path?: string;
    type?: string;
  }>;
};

export type DependencyManifest = {
  path: string;
  kind: 'package.json' | 'package-lock.json' | 'requirements.txt' | 'pyproject.toml';
};

export type ExtractedDependencySets = {
  npm: Set<string>;
  pypi: Set<string>;
};

export async function getGitHubAccessToken(userId: string): Promise<string | null> {
  const { rows } = await pool.query<{ provider_access_token: string | null }>(
    `
    SELECT provider_access_token
    FROM user_auth_identities
    WHERE user_id = $1
      AND provider = 'github'
    LIMIT 1;
    `,
    [userId],
  );

  return rows[0]?.provider_access_token ?? null;
}

export async function fetchGitHubJson<T>(
  accessToken: string,
  url: string,
): Promise<T> {
  const res = await fetch(url, {
    headers: {
      accept: 'application/vnd.github+json',
      authorization: `Bearer ${accessToken}`,
      'x-github-api-version': '2022-11-28',
    },
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`GitHub API error ${res.status}: ${body.slice(0, 180)}`);
  }

  return (await res.json()) as T;
}

export async function fetchRepos(accessToken: string): Promise<GitHubRepo[]> {
  return fetchGitHubJson<GitHubRepo[]>(
    accessToken,
    'https://api.github.com/user/repos?per_page=100&sort=updated&affiliation=owner,collaborator,organization_member',
  );
}

export async function fetchRepoFileText(
  accessToken: string,
  fullName: string,
  path: string,
): Promise<string> {
  const encodedPath = path
    .split('/')
    .map((segment) => encodeURIComponent(segment))
    .join('/');

  const content = await fetchGitHubJson<GitHubContent>(
    accessToken,
    `https://api.github.com/repos/${fullName}/contents/${encodedPath}`,
  );

  if (content.encoding !== 'base64' || !content.content) {
    throw new Error(`${path} content was not base64 encoded`);
  }

  return Buffer.from(content.content, 'base64').toString('utf8');
}

export async function fetchPackageJson(
  accessToken: string,
  fullName: string,
  path = 'package.json',
): Promise<Record<string, unknown>> {
  const json = await fetchRepoFileText(accessToken, fullName, path);
  return JSON.parse(json) as Record<string, unknown>;
}

export async function findDependencyManifestPaths(
  accessToken: string,
  fullName: string,
): Promise<DependencyManifest[]> {
  const tree = await fetchGitHubJson<GitHubTree>(
    accessToken,
    `https://api.github.com/repos/${fullName}/git/trees/HEAD?recursive=1`,
  );

  return (tree.tree ?? [])
    .filter((item) => item.type === 'blob')
    .map((item) => item.path ?? '')
    .filter((path) => {
      const parts = path.split('/');
      return !parts.some((part) =>
        ['node_modules', '.next', '.git', 'dist', 'build', 'vendor'].includes(part),
      );
    })
    .map((path): DependencyManifest | null => {
      const file = path.split('/').at(-1);
      if (file === 'package.json') {
        return { path, kind: 'package.json' };
      }
      if (file === 'package-lock.json' || file === 'npm-shrinkwrap.json') {
        return { path, kind: 'package-lock.json' };
      }
      if (file === 'requirements.txt') {
        return { path, kind: 'requirements.txt' };
      }
      if (file === 'pyproject.toml') {
        return { path, kind: 'pyproject.toml' };
      }
      return null;
    })
    .filter((manifest): manifest is DependencyManifest => manifest !== null)
    .sort((a, b) => {
      if (a.path === 'package.json') return -1;
      if (b.path === 'package.json') return 1;
      return a.path.localeCompare(b.path);
    });
}

export function extractPackageJsonDependencies(
  packageJson: Record<string, unknown>,
): string[] {
  const sections = [
    'dependencies',
    'devDependencies',
    'peerDependencies',
    'optionalDependencies',
  ];
  const names = new Set<string>();

  for (const section of sections) {
    const deps = packageJson[section];
    if (!deps || typeof deps !== 'object' || Array.isArray(deps)) {
      continue;
    }

    for (const name of Object.keys(deps)) {
      names.add(name.toLowerCase());
    }
  }

  return [...names].sort((a, b) => a.localeCompare(b));
}

export function extractPackageLockDependencies(
  lockfile: Record<string, unknown>,
): string[] {
  const names = new Set<string>();
  const dependencies = lockfile.dependencies;
  if (dependencies && typeof dependencies === 'object' && !Array.isArray(dependencies)) {
    for (const name of Object.keys(dependencies)) {
      names.add(name.toLowerCase());
    }
  }

  const packages = lockfile.packages;
  if (packages && typeof packages === 'object' && !Array.isArray(packages)) {
    for (const path of Object.keys(packages)) {
      const marker = 'node_modules/';
      const index = path.lastIndexOf(marker);
      if (index >= 0) {
        names.add(path.slice(index + marker.length).toLowerCase());
      }
    }
  }

  return [...names].sort((a, b) => a.localeCompare(b));
}

export function extractRequirementsDependencies(text: string): string[] {
  const names = new Set<string>();

  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.split('#')[0].trim();
    if (!line || line.startsWith('-') || line.startsWith('http')) {
      continue;
    }

    const match = line.match(/^([A-Za-z0-9_.-]+)/);
    if (match?.[1]) {
      names.add(match[1].replaceAll('_', '-').toLowerCase());
    }
  }

  return [...names].sort((a, b) => a.localeCompare(b));
}

export function extractPyprojectDependencies(text: string): string[] {
  const names = new Set<string>();
  const dependencyArrayPattern = /dependencies\s*=\s*\[([\s\S]*?)\]/g;
  let arrayMatch: RegExpExecArray | null;

  while ((arrayMatch = dependencyArrayPattern.exec(text)) !== null) {
    const quotedDependencyPattern = /["']([^"']+)["']/g;
    let depMatch: RegExpExecArray | null;
    while ((depMatch = quotedDependencyPattern.exec(arrayMatch[1])) !== null) {
      const nameMatch = depMatch[1].trim().match(/^([A-Za-z0-9_.-]+)/);
      if (nameMatch?.[1]) {
        names.add(nameMatch[1].replaceAll('_', '-').toLowerCase());
      }
    }
  }

  return [...names].sort((a, b) => a.localeCompare(b));
}

export function createDependencySets(): ExtractedDependencySets {
  return {
    npm: new Set<string>(),
    pypi: new Set<string>(),
  };
}
