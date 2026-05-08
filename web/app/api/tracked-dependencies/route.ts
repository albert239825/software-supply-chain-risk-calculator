import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import { getCurrentUser } from '@/lib/auth';

type TrackBody = {
  packageId?: unknown;
  note?: unknown;
};

function normalizeNote(note: unknown): string | null {
  if (typeof note !== 'string') {
    return null;
  }

  const trimmed = note.trim();
  return trimmed.length > 0 ? trimmed.slice(0, 500) : null;
}

export async function GET(req: NextRequest) {
  try {
    const user = await getCurrentUser(req);
    if (!user) {
      return NextResponse.json({ error: 'login required' }, { status: 401 });
    }

    const { rows } = await pool.query(
      `
      SELECT
        utd.id,
        utd.package_id,
        utd.note,
        utd.created_at,
        utd.updated_at,
        p.name AS package_name,
        p.ecosystem,
        p.description,
        p.latest_version,
        (
          SELECT v.id
          FROM versions v
          WHERE v.package_id = p.id
            AND v.version = p.latest_version
          LIMIT 1
        ) AS latest_version_id
      FROM user_tracked_dependencies utd
      JOIN packages p ON p.id = utd.package_id
      WHERE utd.user_id = $1
      ORDER BY utd.created_at DESC;
      `,
      [user.id],
    );

    return NextResponse.json(rows, { status: 200 });
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}

export async function POST(req: NextRequest) {
  try {
    const user = await getCurrentUser(req);
    if (!user) {
      return NextResponse.json({ error: 'login required' }, { status: 401 });
    }

    const body = (await req.json()) as TrackBody;
    if (typeof body.packageId !== 'string' || body.packageId.length === 0) {
      return NextResponse.json({ error: 'packageId is required' }, { status: 400 });
    }

    const note = normalizeNote(body.note);
    const { rows } = await pool.query(
      `
      INSERT INTO user_tracked_dependencies (user_id, package_id, note)
      VALUES ($1, $2, $3)
      ON CONFLICT (user_id, package_id)
      DO UPDATE SET
        note = EXCLUDED.note,
        updated_at = now()
      RETURNING id, package_id, note, created_at, updated_at;
      `,
      [user.id, body.packageId, note],
    );

    return NextResponse.json(rows[0], { status: 201 });
  } catch (error) {
    return NextResponse.json({ error: (error as Error).message }, { status: 500 });
  }
}
