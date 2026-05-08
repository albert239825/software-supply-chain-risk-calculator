import { NextRequest } from 'next/server';

export type JsonParseResult =
  | { ok: true; data: unknown }
  | { ok: false; error: string };

export async function readJsonObject(req: NextRequest): Promise<JsonParseResult> {
  try {
    const data = (await req.json()) as unknown;
    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      return { ok: false, error: 'request body must be a JSON object' };
    }
    return { ok: true, data };
  } catch {
    return { ok: false, error: 'request body must be valid JSON' };
  }
}

export function normalizePathId(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  if (trimmed.length === 0 || trimmed.length > 256) {
    return null;
  }

  return trimmed;
}

export function invalidPathIdMessage(label = 'id') {
  return `${label} must be a non-empty string under 256 characters`;
}

export function parseBoundedIntegerParam(
  searchParams: URLSearchParams,
  name: string,
  defaultValue: number,
  min: number,
  max: number,
): { ok: true; value: number } | { ok: false; error: string } {
  const raw = searchParams.get(name);
  if (raw === null || raw.trim() === '') {
    return { ok: true, value: defaultValue };
  }

  if (!/^-?\d+$/.test(raw.trim())) {
    return { ok: false, error: `${name} must be an integer between ${min} and ${max}` };
  }

  const value = Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    return { ok: false, error: `${name} must be an integer between ${min} and ${max}` };
  }

  return { ok: true, value };
}
