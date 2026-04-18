import { createClient, type SupabaseClient } from "@supabase/supabase-js";

/**
 * Supabase client factories for the Supply Chain Risk Scorer web app.
 *
 * Two environments are supported:
 *
 * - Server-side (API routes, server components, route handlers): uses the
 *   service role key and must NEVER be imported into client components.
 * - Browser (client components): uses the anon key via `NEXT_PUBLIC_*` vars.
 *
 * Misuse (e.g. importing the server client from a component that ends up in
 * the client bundle) fails loudly at call time rather than leaking secrets.
 */

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(
      `Missing required environment variable: ${name}. ` +
        `Set it in web/.env.local — see web/.env.example for the full list.`,
    );
  }
  return value;
}

/**
 * Server-only Supabase client. Uses the service role key, which bypasses RLS
 * and MUST stay on the server. Importing this from a client component will
 * throw because `SUPABASE_SERVICE_ROLE_KEY` is not exposed to the browser.
 */
export function createSupabaseServerClient(): SupabaseClient {
  if (typeof window !== "undefined") {
    throw new Error(
      "createSupabaseServerClient() must not be called in the browser. " +
        "Use createSupabaseBrowserClient() from client components instead.",
    );
  }
  const url = requireEnv("SUPABASE_URL");
  const key = requireEnv("SUPABASE_SERVICE_ROLE_KEY");
  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

/**
 * Browser-safe Supabase client. Uses the anon key, which is safe to ship to
 * the client.
 *
 * Must reference `process.env.NEXT_PUBLIC_*` via direct static property access
 * (not through a helper) so Next.js inlines the values at build time. Dynamic
 * lookups like `process.env[name]` are not replaced in the browser bundle and
 * would always be `undefined`.
 */
export function createSupabaseBrowserClient(): SupabaseClient {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
  if (!url || !key) {
    throw new Error(
      "Missing NEXT_PUBLIC_SUPABASE_URL or NEXT_PUBLIC_SUPABASE_ANON_KEY. " +
        "Set them in web/.env.local — see web/.env.example for the full list.",
    );
  }
  return createClient(url, key);
}
