import { NextResponse } from 'next/server';

function isConfigured(keys: string[]): boolean {
  return keys.every((key) => Boolean(process.env[key]));
}

export async function GET() {
  const baseUrl = process.env.AUTH_REDIRECT_BASE_URL || 'http://localhost:3000';

  return NextResponse.json({
    providers: {
      google: {
        label: 'Gmail',
        configured: isConfigured(['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET']),
        requiredEnv: ['GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET'],
        callbackUrl: `${baseUrl}/api/auth/callback/google`,
      },
      github: {
        label: 'GitHub',
        configured: isConfigured(['GITHUB_CLIENT_ID', 'GITHUB_CLIENT_SECRET']),
        requiredEnv: ['GITHUB_CLIENT_ID', 'GITHUB_CLIENT_SECRET'],
        callbackUrl: `${baseUrl}/api/auth/callback/github`,
      },
    },
    redirectBaseUrl: {
      configured: Boolean(process.env.AUTH_REDIRECT_BASE_URL),
      requiredEnv: ['AUTH_REDIRECT_BASE_URL'],
      value: baseUrl,
    },
  });
}
