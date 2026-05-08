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

export async function fetchPackageJson(
  accessToken: string,
  fullName: string,
): Promise<Record<string, unknown>> {
  const content = await fetchGitHubJson<GitHubContent>(
    accessToken,
    `https://api.github.com/repos/${fullName}/contents/package.json`,
  );

  if (content.encoding !== 'base64' || !content.content) {
    throw new Error('package.json content was not base64 encoded');
  }

  const json = Buffer.from(content.content, 'base64').toString('utf8');
  return JSON.parse(json) as Record<string, unknown>;
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
