import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import { readJsonObject } from '@/lib/api/validation';
import {
  applySessionCookie,
  createOrUpdateUserFromOAuth,
  createSession,
} from '@/lib/auth';

type LoginBody = {
  email?: unknown;
  githubUsername?: unknown;
};

function normalizeEmail(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const email = value.trim().toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : null;
}

function normalizeGitHubUsername(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const username = value.trim().replace(/^@/, '');
  return /^[a-zA-Z0-9-]{1,39}$/.test(username) ? username : null;
}

export async function POST(req: NextRequest) {
  try {
    const parsed = await readJsonObject(req);
    if (!parsed.ok) {
      return NextResponse.json({ error: parsed.error }, { status: 400 });
    }

    const body = parsed.data as LoginBody;
    const email = normalizeEmail(body.email);
    const githubUsername = normalizeGitHubUsername(body.githubUsername);

    if (!email) {
      return NextResponse.json({ error: 'valid Gmail address is required' }, { status: 400 });
    }

    if (!email.endsWith('@gmail.com')) {
      return NextResponse.json({ error: 'please use a Gmail address' }, { status: 400 });
    }

    if (!githubUsername) {
      return NextResponse.json({ error: 'valid GitHub username is required' }, { status: 400 });
    }

    const user = await createOrUpdateUserFromOAuth({
      provider: 'github',
      providerUserId: githubUsername.toLowerCase(),
      email,
      displayName: githubUsername,
      avatarUrl: `https://github.com/${githubUsername}.png`,
      profileUrl: `https://github.com/${githubUsername}`,
    });

    await pool.query(
      `
      INSERT INTO user_auth_identities (
        user_id,
        provider,
        provider_user_id,
        provider_email,
        profile_url
      )
      VALUES ($1, 'google', $2, $2, NULL)
      ON CONFLICT (provider, provider_user_id)
      DO UPDATE SET
        user_id = EXCLUDED.user_id,
        provider_email = EXCLUDED.provider_email,
        updated_at = now();
      `,
      [user.id, email],
    );

    const sessionToken = await createSession(user.id);
    const res = NextResponse.json({ user }, { status: 200 });
    applySessionCookie(res, sessionToken);
    return res;
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}
