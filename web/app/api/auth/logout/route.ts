import { NextRequest, NextResponse } from 'next/server';
import { clearSessionCookie, deleteSession } from '@/lib/auth';

export async function POST(req: NextRequest) {
  try {
    await deleteSession(req);
    const res = NextResponse.json({ ok: true }, { status: 200 });
    clearSessionCookie(res);
    return res;
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}
