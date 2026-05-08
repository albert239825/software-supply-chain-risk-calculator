import { NextRequest } from 'next/server';
import {
  AuthProvider,
  OAuthProfile,
  getCallbackUrl,
  getOAuthStateCookie,
} from './auth';

type GoogleTokenResponse = {
  access_token?: string;
  error?: string;
  error_description?: string;
};

type GoogleUserInfo = {
  sub: string;
  email?: string;
  name?: string;
  picture?: string;
};

type GitHubTokenResponse = {
  access_token?: string;
  error?: string;
  error_description?: string;
};

type GitHubUser = {
  id: number;
  login: string;
  email: string | null;
  name: string | null;
  avatar_url: string | null;
  html_url: string | null;
};

type GitHubEmail = {
  email: string;
  primary: boolean;
  verified: boolean;
};

function mustEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is not configured`);
  }
  return value;
}

export function buildOAuthUrl(
  req: NextRequest,
  provider: AuthProvider,
  state: string,
): string {
  if (provider === 'google') {
    const url = new URL('https://accounts.google.com/o/oauth2/v2/auth');
    url.searchParams.set('client_id', mustEnv('GOOGLE_CLIENT_ID'));
    url.searchParams.set('redirect_uri', getCallbackUrl(req, provider));
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('scope', 'openid email profile');
    url.searchParams.set('state', state);
    url.searchParams.set('prompt', 'select_account');
    return url.toString();
  }

  const url = new URL('https://github.com/login/oauth/authorize');
  url.searchParams.set('client_id', mustEnv('GITHUB_CLIENT_ID'));
  url.searchParams.set('redirect_uri', getCallbackUrl(req, provider));
  url.searchParams.set('scope', 'read:user user:email');
  url.searchParams.set('state', state);
  return url.toString();
}

export function assertValidState(
  req: NextRequest,
  provider: AuthProvider,
  state: string | null,
): void {
  const expected = req.cookies.get(getOAuthStateCookie(provider))?.value;
  if (!state || !expected || state !== expected) {
    throw new Error('OAuth state did not match');
  }
}

export async function exchangeCodeForProfile(
  req: NextRequest,
  provider: AuthProvider,
  code: string,
): Promise<OAuthProfile> {
  return provider === 'google'
    ? exchangeGoogleCode(req, code)
    : exchangeGitHubCode(req, code);
}

async function exchangeGoogleCode(
  req: NextRequest,
  code: string,
): Promise<OAuthProfile> {
  const tokenResponse = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: mustEnv('GOOGLE_CLIENT_ID'),
      client_secret: mustEnv('GOOGLE_CLIENT_SECRET'),
      code,
      grant_type: 'authorization_code',
      redirect_uri: getCallbackUrl(req, 'google'),
    }),
  });

  const token = (await tokenResponse.json()) as GoogleTokenResponse;
  if (!tokenResponse.ok || !token.access_token) {
    throw new Error(token.error_description || token.error || 'Google login failed');
  }

  const profileResponse = await fetch('https://www.googleapis.com/oauth2/v3/userinfo', {
    headers: { authorization: `Bearer ${token.access_token}` },
  });

  if (!profileResponse.ok) {
    throw new Error('Could not read Google profile');
  }

  const profile = (await profileResponse.json()) as GoogleUserInfo;
  return {
    provider: 'google',
    providerUserId: profile.sub,
    email: profile.email ?? null,
    displayName: profile.name ?? profile.email ?? null,
    avatarUrl: profile.picture ?? null,
    profileUrl: null,
  };
}

async function exchangeGitHubCode(
  req: NextRequest,
  code: string,
): Promise<OAuthProfile> {
  const tokenResponse = await fetch('https://github.com/login/oauth/access_token', {
    method: 'POST',
    headers: {
      accept: 'application/json',
      'content-type': 'application/x-www-form-urlencoded',
    },
    body: new URLSearchParams({
      client_id: mustEnv('GITHUB_CLIENT_ID'),
      client_secret: mustEnv('GITHUB_CLIENT_SECRET'),
      code,
      redirect_uri: getCallbackUrl(req, 'github'),
    }),
  });

  const token = (await tokenResponse.json()) as GitHubTokenResponse;
  if (!tokenResponse.ok || !token.access_token) {
    throw new Error(token.error_description || token.error || 'GitHub login failed');
  }

  const profileResponse = await fetch('https://api.github.com/user', {
    headers: {
      accept: 'application/vnd.github+json',
      authorization: `Bearer ${token.access_token}`,
    },
  });

  if (!profileResponse.ok) {
    throw new Error('Could not read GitHub profile');
  }

  const profile = (await profileResponse.json()) as GitHubUser;
  const email = profile.email ?? (await fetchPrimaryGitHubEmail(token.access_token));

  return {
    provider: 'github',
    providerUserId: String(profile.id),
    email,
    displayName: profile.name ?? profile.login,
    avatarUrl: profile.avatar_url,
    profileUrl: profile.html_url,
  };
}

async function fetchPrimaryGitHubEmail(accessToken: string): Promise<string | null> {
  const response = await fetch('https://api.github.com/user/emails', {
    headers: {
      accept: 'application/vnd.github+json',
      authorization: `Bearer ${accessToken}`,
    },
  });

  if (!response.ok) {
    return null;
  }

  const emails = (await response.json()) as GitHubEmail[];
  return (
    emails.find((email) => email.primary && email.verified)?.email ??
    emails.find((email) => email.verified)?.email ??
    null
  );
}
