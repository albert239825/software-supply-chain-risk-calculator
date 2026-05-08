import { createHash, randomBytes } from 'crypto';
import { NextRequest, NextResponse } from 'next/server';
import pool from './db';

export const SESSION_COOKIE = 'ssc_session';

export type AuthProvider = 'google' | 'github';

export type CurrentUser = {
  id: string;
  email: string | null;
  displayName: string | null;
  avatarUrl: string | null;
  authProviders?: AuthProvider[];
  hasGitHubAccess?: boolean;
};

export type OAuthProfile = {
  provider: AuthProvider;
  providerUserId: string;
  email: string | null;
  displayName: string | null;
  avatarUrl: string | null;
  profileUrl: string | null;
  accessToken?: string | null;
  scopes?: string | null;
};

const SESSION_DAYS = 30;

export function getBaseUrl(req: NextRequest): string {
  return process.env.AUTH_REDIRECT_BASE_URL || req.nextUrl.origin;
}

export function getCallbackUrl(req: NextRequest, provider: AuthProvider): string {
  return `${getBaseUrl(req)}/api/auth/callback/${provider}`;
}

export function getOAuthStateCookie(provider: AuthProvider): string {
  return `ssc_oauth_state_${provider}`;
}

export function createOpaqueToken(): string {
  return randomBytes(32).toString('base64url');
}

export function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex');
}

export function readCookie(req: Request, name: string): string | null {
  const header = req.headers.get('cookie');
  if (!header) {
    return null;
  }

  for (const cookie of header.split(';')) {
    const [rawName, ...rawValue] = cookie.trim().split('=');
    if (rawName === name) {
      return decodeURIComponent(rawValue.join('='));
    }
  }

  return null;
}

export function applySessionCookie(res: NextResponse, token: string): void {
  res.cookies.set(SESSION_COOKIE, token, {
    httpOnly: true,
    sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    maxAge: SESSION_DAYS * 24 * 60 * 60,
  });
}

export function clearSessionCookie(res: NextResponse): void {
  res.cookies.set(SESSION_COOKIE, '', {
    httpOnly: true,
    sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    maxAge: 0,
  });
}

export async function createOrUpdateUserFromOAuth(
  profile: OAuthProfile,
): Promise<CurrentUser> {
  const existingIdentity = await pool.query<{ user_id: string }>(
    `
    SELECT user_id
    FROM user_auth_identities
    WHERE provider = $1 AND provider_user_id = $2
    LIMIT 1;
    `,
    [profile.provider, profile.providerUserId],
  );

  let userId = existingIdentity.rows[0]?.user_id;

  if (!userId && profile.email) {
    const existingUser = await pool.query<{ id: string }>(
      'SELECT id FROM users WHERE lower(email) = lower($1) LIMIT 1;',
      [profile.email],
    );
    userId = existingUser.rows[0]?.id;
  }

  if (userId) {
    await pool.query(
      `
      UPDATE users
      SET
        email = COALESCE($2, email),
        display_name = COALESCE($3, display_name),
        avatar_url = COALESCE($4, avatar_url),
        updated_at = now(),
        last_login_at = now()
      WHERE id = $1;
      `,
      [userId, profile.email, profile.displayName, profile.avatarUrl],
    );
  } else {
    const created = await pool.query<{ id: string }>(
      `
      INSERT INTO users (email, display_name, avatar_url, last_login_at)
      VALUES ($1, $2, $3, now())
      RETURNING id;
      `,
      [profile.email, profile.displayName, profile.avatarUrl],
    );
    userId = created.rows[0].id;
  }

  await pool.query(
    `
    INSERT INTO user_auth_identities (
      user_id,
      provider,
      provider_user_id,
      provider_email,
      provider_access_token,
      provider_scopes,
      profile_url
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (provider, provider_user_id)
    DO UPDATE SET
      provider_email = EXCLUDED.provider_email,
      provider_access_token = COALESCE(EXCLUDED.provider_access_token, user_auth_identities.provider_access_token),
      provider_scopes = COALESCE(EXCLUDED.provider_scopes, user_auth_identities.provider_scopes),
      profile_url = EXCLUDED.profile_url,
      updated_at = now();
    `,
    [
      userId,
      profile.provider,
      profile.providerUserId,
      profile.email,
      profile.accessToken ?? null,
      profile.scopes ?? null,
      profile.profileUrl,
    ],
  );

  const user = await pool.query<{
    id: string;
    email: string | null;
    display_name: string | null;
    avatar_url: string | null;
  }>(
    `
    SELECT id, email, display_name, avatar_url
    FROM users
    WHERE id = $1;
    `,
    [userId],
  );

  const row = user.rows[0];
  return {
    id: row.id,
    email: row.email,
    displayName: row.display_name,
    avatarUrl: row.avatar_url,
  };
}

export async function createSession(userId: string): Promise<string> {
  const token = createOpaqueToken();
  await pool.query(
    `
    INSERT INTO user_sessions (user_id, token_hash, expires_at)
    VALUES ($1, $2, now() + ($3::text || ' days')::interval);
    `,
    [userId, hashToken(token), SESSION_DAYS],
  );
  return token;
}

export async function getCurrentUser(req: Request): Promise<CurrentUser | null> {
  const token = readCookie(req, SESSION_COOKIE);
  if (!token) {
    return null;
  }

  const session = await pool.query<{
    user_id: string;
    email: string | null;
    display_name: string | null;
    avatar_url: string | null;
    auth_providers: AuthProvider[] | null;
    has_github_access: boolean | null;
  }>(
    `
    SELECT
      u.id AS user_id,
      u.email,
      u.display_name,
      u.avatar_url,
      COALESCE(
        array_remove(array_agg(DISTINCT i.provider), NULL),
        ARRAY[]::text[]
      ) AS auth_providers,
      COALESCE(
        bool_or(i.provider = 'github' AND i.provider_access_token IS NOT NULL),
        false
      ) AS has_github_access
    FROM user_sessions s
    JOIN users u ON u.id = s.user_id
    LEFT JOIN user_auth_identities i ON i.user_id = u.id
    WHERE s.token_hash = $1
      AND s.expires_at > now()
    GROUP BY u.id, u.email, u.display_name, u.avatar_url
    LIMIT 1;
    `,
    [hashToken(token)],
  );

  const row = session.rows[0];
  if (!row) {
    return null;
  }

  await pool.query(
    'UPDATE user_sessions SET last_seen_at = now() WHERE token_hash = $1;',
    [hashToken(token)],
  );

  return {
    id: row.user_id,
    email: row.email,
    displayName: row.display_name,
    avatarUrl: row.avatar_url,
    authProviders: row.auth_providers ?? [],
    hasGitHubAccess: Boolean(row.has_github_access),
  };
}

export async function deleteSession(req: Request): Promise<void> {
  const token = readCookie(req, SESSION_COOKIE);
  if (!token) {
    return;
  }

  await pool.query('DELETE FROM user_sessions WHERE token_hash = $1;', [
    hashToken(token),
  ]);
}
