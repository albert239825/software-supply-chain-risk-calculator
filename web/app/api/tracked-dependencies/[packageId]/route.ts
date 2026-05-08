import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import { getCurrentUser } from '@/lib/auth';
import { invalidPathIdMessage, normalizePathId } from '@/lib/api/validation';

type RouteContext = {
  params: Promise<{ packageId: string }>;
};

export async function DELETE(req: NextRequest, ctx: RouteContext) {
  try {
    const user = await getCurrentUser(req);
    if (!user) {
      return NextResponse.json({ error: 'login required' }, { status: 401 });
    }

    const params = await ctx.params;
    const packageId = normalizePathId(params.packageId);
    if (!packageId) {
      return NextResponse.json({ error: invalidPathIdMessage('packageId') }, { status: 400 });
    }

    const { rowCount } = await pool.query(
      `
      DELETE FROM user_tracked_dependencies
      WHERE user_id = $1 AND package_id = $2;
      `,
      [user.id, packageId],
    );

    if (rowCount === 0) {
      return NextResponse.json({ error: 'tracked dependency not found' }, { status: 404 });
    }

    return NextResponse.json({ ok: true }, { status: 200 });
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}
