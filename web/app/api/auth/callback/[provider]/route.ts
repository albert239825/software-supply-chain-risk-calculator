import { NextRequest, NextResponse } from 'next/server';
import {
  AuthProvider,
  applySessionCookie,
  createOrUpdateUserFromOAuth,
  createSession,
  getBaseUrl,
  getOAuthStateCookie,
} from '@/lib/auth';
import { assertValidState, exchangeCodeForProfile } from '@/lib/oauth';

type RouteContext = {
  params: Promise<{ provider: string }>;
};

function parseProvider(provider: string): AuthProvider | null {
  return provider === 'google' || provider === 'github' ? provider : null;
}

export async function GET(req: NextRequest, ctx: RouteContext) {
  const { provider: rawProvider } = await ctx.params;
  const provider = parseProvider(rawProvider);

  if (!provider) {
    return NextResponse.json({ error: 'unsupported auth provider' }, { status: 404 });
  }

  const code = req.nextUrl.searchParams.get('code');
  const state = req.nextUrl.searchParams.get('state');
  const error = req.nextUrl.searchParams.get('error');

  if (error) {
    return NextResponse.redirect(`${getBaseUrl(req)}/track?auth=failed`);
  }

  if (!code) {
    return NextResponse.json({ error: 'missing OAuth code' }, { status: 400 });
  }

  try {
    assertValidState(req, provider, state);
    const profile = await exchangeCodeForProfile(req, provider, code);
    const user = await createOrUpdateUserFromOAuth(profile);
    const sessionToken = await createSession(user.id);

    const res = NextResponse.redirect(`${getBaseUrl(req)}/track`);
    applySessionCookie(res, sessionToken);
    res.cookies.set(getOAuthStateCookie(provider), '', {
      httpOnly: true,
      sameSite: 'lax',
      secure: process.env.NODE_ENV === 'production',
      path: '/',
      maxAge: 0,
    });
    return res;
  } catch (callbackError) {
    return NextResponse.json(
      { error: (callbackError as Error).message },
      { status: 400 },
    );
  }
}
