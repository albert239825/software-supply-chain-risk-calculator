import { NextRequest, NextResponse } from 'next/server';
import { getCurrentUser } from '@/lib/auth';
import { fetchRepos, getGitHubAccessToken } from '@/lib/github';

export async function GET(req: NextRequest) {
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

    const repos = await fetchRepos(token);
    return NextResponse.json(
      repos.map((repo) => ({
        id: repo.id,
        name: repo.name,
        fullName: repo.full_name,
        private: repo.private,
        defaultBranch: repo.default_branch,
        htmlUrl: repo.html_url,
        updatedAt: repo.updated_at,
      })),
      { status: 200 },
    );
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}
