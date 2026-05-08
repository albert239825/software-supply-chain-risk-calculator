import { NextRequest, NextResponse } from 'next/server';
import {
  AuthProvider,
  createOpaqueToken,
  getOAuthStateCookie,
} from '@/lib/auth';
import { buildOAuthUrl } from '@/lib/oauth';

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

  try {
    const state = createOpaqueToken();
    const res = NextResponse.redirect(buildOAuthUrl(req, provider, state));
    res.cookies.set(getOAuthStateCookie(provider), state, {
      httpOnly: true,
      sameSite: 'lax',
      secure: process.env.NODE_ENV === 'production',
      path: '/',
      maxAge: 10 * 60,
    });
    return res;
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}
