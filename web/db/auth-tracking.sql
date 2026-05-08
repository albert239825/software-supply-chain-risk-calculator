-- Auth and dependency tracking tables.
-- Run this in Supabase SQL editor before enabling OAuth routes.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email text UNIQUE,
  display_name text,
  avatar_url text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_login_at timestamptz
);

CREATE TABLE IF NOT EXISTS user_auth_identities (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  provider text NOT NULL CHECK (provider IN ('google', 'github')),
  provider_user_id text NOT NULL,
  provider_email text,
  provider_access_token text,
  provider_scopes text,
  profile_url text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, provider_user_id),
  UNIQUE (user_id, provider)
);

CREATE TABLE IF NOT EXISTS user_sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS user_tracked_dependencies (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  package_id text NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
  note text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, package_id)
);

ALTER TABLE user_auth_identities
  ADD COLUMN IF NOT EXISTS provider_access_token text;

ALTER TABLE user_auth_identities
  ADD COLUMN IF NOT EXISTS provider_scopes text;

CREATE INDEX IF NOT EXISTS idx_user_sessions_user_expires
  ON user_sessions(user_id, expires_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_tracked_dependencies_user_created
  ON user_tracked_dependencies(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_tracked_dependencies_package
  ON user_tracked_dependencies(package_id);
