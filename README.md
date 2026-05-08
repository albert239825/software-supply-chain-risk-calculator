# Software Supply Chain Risk Scorer

## Description

This project helps teams reason about **software supply chain risk** for open-source dependencies. We made a  **Next.js** web app that provides search, package detail, an interactive **dependency graph**, and **risk-style signals** over ecosystem data stored in **Supabase** (PostgreSQL). For the data cleaning and accumulaton, we also have a separate **Python pipeline** (`collect_data.py`) can fetch **NPM** and/or **PyPI** metadata and dependency graphs from public APIs, then write **normalized CSVs** (`data/clean/`) suitable for loading into your database. Together, the pieces support exploring how packages connect in the wild and comparing candidates with more context than a single version string.

---

Instructions to run the code: 
## Run locally

You need **Node.js** (a current LTS, e.g. 20+) for the web app and **Python 3.11+** if you want to run data collection.

### 1) Web application

```bash
cd web
npm install
cp .env.example .env.local
```

Edit `web/.env.local` and set at least **`NEXT_PUBLIC_SUPABASE_URL`** and **`NEXT_PUBLIC_SUPABASE_ANON_KEY`** (the supabase is in our report)

```bash
npm run dev
```

Open **http://localhost:3000** (dev server uses port **3000** by default). Useful checks:

- `http://localhost:3000/api/health` — liveness; includes DB status when configured.

### 2) Data collection (optional)

Generates `data/raw/<run_id>/…` and `data/clean/*.csv` without touching the web app.

**Conda (recommended)**

```bash
conda env create -f environment.yml
conda activate cis5500
python collect_data.py --npm --pypi --top-n 50
```

**pip**

```bash
python3 -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python collect_data.py --npm --pypi --top-n 50
```

Use `--npm` and/or `--pypi`. See [Python data collection](#python-data-collection-collect_datapy) below for flags and output layout.

---

## Repository layout

1. **`web/`** — Next.js App Router app (TypeScript, Tailwind, shadcn/ui, Supabase client): UI and API routes.
2. **Root + `src/`** — Python collectors and CSV cleaning used by `collect_data.py`.

See [`docs/PLAN.md`](docs/PLAN.md) for scope and milestones, and [`docs/api-spec.md`](docs/api-spec.md) for the v1 API contract.

---

## Web application (`web/`)

Next.js App Router, TypeScript (strict), Tailwind CSS, shadcn/ui, and `@supabase/supabase-js`. The [Run locally](#run-locally) section covers install, env, dev server, and basic URLs; what follows adds auth, scripts, and API details.

Supabase credentials live in `web/.env.local` (gitignored). See `web/.env.example` for the required variable names.

### Authenticated dependency tracking

The web app supports Gmail and GitHub OAuth login plus a per-user dependency watch list. Before using it:

1. Run [`web/db/auth-tracking.sql`](web/db/auth-tracking.sql) in the Supabase SQL editor.
2. Add `SUPABASE_DB_URL` and `AUTH_REDIRECT_BASE_URL=http://localhost:3000` to `web/.env.local` or `web/.env`.
3. Create OAuth apps for the providers you want to enable:
   - Google: open Google Auth Platform Clients, create a **Web application** client, add authorized redirect URI `http://localhost:3000/api/auth/callback/google`, then copy the client ID and secret into `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET`.
   - GitHub: open GitHub Developer settings → OAuth Apps → New OAuth App, set homepage URL `http://localhost:3000`, set authorization callback URL `http://localhost:3000/api/auth/callback/github`, then copy the client ID and secret into `GITHUB_CLIENT_ID` and `GITHUB_CLIENT_SECRET`.
4. Restart `npm run dev`, then visit `http://localhost:3000/track`. The page shows which providers are configured and enables the login buttons when ready.

Once configured, users can sign in from the nav or `http://localhost:3000/track` and manage tracked packages. The auth/tracking API is:

| Route | Purpose |
|---|---|
| `GET /api/auth/me` | Current session user |
| `POST /api/auth/logout` | End the current session |
| `GET /api/tracked-dependencies` | List the signed-in user's tracked packages |
| `POST /api/tracked-dependencies` | Track `{ "packageId": "..." }` |
| `DELETE /api/tracked-dependencies/:packageId` | Stop tracking a package |
| `GET /api/github/repos` | List repositories for the signed-in GitHub user |
| `POST /api/github/repos/import` | Import tracked dependencies from supported repo dependency files |

If your auth tables already exist, run these additions before using GitHub import:

```sql
ALTER TABLE user_auth_identities
  ADD COLUMN IF NOT EXISTS provider_access_token text;

ALTER TABLE user_auth_identities
  ADD COLUMN IF NOT EXISTS provider_scopes text;
```

Common scripts:

| Script | Purpose |
|---|---|
| `npm run dev` | Next.js dev server on port 3000 |
| `npm run build` | Production build |
| `npm run start` | Serve the production build |
| `npm run lint` | ESLint via `eslint-config-next` |
| `npm run typecheck` | `tsc --noEmit` |
| `npm run test` | Vitest unit tests (`vitest run`) |
| `npm run test:watch` | Vitest in watch mode |

### Exercising the backend routes

Phase 0 ships three template routes end-to-end (contract in [`docs/api-spec.md`](docs/api-spec.md)):

- **A1** `GET /api/packages/search?q=<name>&limit=<n>&ecosystem=<npm|pypi>`
- **A7** `GET /api/stats/counts?ecosystem=<npm|pypi>`
- **A8** `GET /api/health`

With `npm run dev` already running (see [Run locally](#run-locally)), in another terminal:

```bash
curl -sS http://localhost:3000/api/health | jq .
curl -sS "http://localhost:3000/api/stats/counts?ecosystem=npm" | jq .
curl -sS "http://localhost:3000/api/packages/search?q=react&limit=10" | jq .
```

A VS Code / JetBrains "REST Client" file is also checked in at [`web/requests.http`](web/requests.http) — open it and click **Send Request** above any block. Phase 1 sub-devins append their routes to the same file.

---

## Python data collection (`collect_data.py`)

Fetches NPM and/or PyPI package metadata and dependency graphs in two stages:

1. **Raw** — collector CSVs under `<out>/raw/<run_id>/<ecosystem>/` (immutable per run; `run_id` is UTC `YYYYMMDDTHHMMSSZ`).
2. **Clean** — normalized, deduplicated CSVs plus `manifest.json` under `<out>/clean/` (latest import-ready snapshot).

`--out` (default `data`) is the only output root; the script creates `raw/` and `clean/` under it. One command runs collection and then the clean step.

Environment setup is summarized under **Run locally → 2) Data collection** above; use **conda** (`environment.yml`) or **pip** (`requirements.txt`) as you prefer.

### Run

```bash
python collect_data.py --npm --pypi
```

Use `--npm` and/or `--pypi`. Optional:

| Flag | Default | Meaning |
|------|---------|---------|
| `--out` | `data` | Output root; subdirs `raw/<run_id>/` and `clean/` are created automatically |
| `--top-n` | `100` | Number of seed packages (per ecosystem you enable) |
| `--workers` | `32` | Max concurrent HTTP requests per BFS level when fetching registry/PyPI JSON |

Examples:

```bash
python collect_data.py --npm --top-n 200
python collect_data.py --pypi --out ./artifacts
```

### Output layout

**Raw (per run)** — under `<out>/raw/<run_id>/`:

- `npm/` — four CSVs if `--npm` was used (same schema as historical single-folder export).
- `pypi/` — four CSVs if `--pypi` was used.

NPM and PyPI no longer overwrite each other; each ecosystem writes to its own subfolder.

**Clean (latest)** — under `<out>/clean/`:

| File | Description |
|------|-------------|
| `packages_clean.csv` | One row per package; includes deterministic `id` (UUIDv5). |
| `versions_clean.csv` | One row per resolved package version; `package_id` FK to packages. |
| `dependencies_clean.csv` | Directed edges; `from_version_id` / `to_package_id` helpers for joins. |
| `maintainers_clean.csv` | Maintainer/author rows; `package_id` when the package exists. |
| `manifest.json` | `run_id`, ecosystems, `top_n`, `workers`, raw/clean row counts, unresolved reference counts, paths. |

#### Schema (columns)

**`packages.csv`**

- `ecosystem` — `npm` or `pypi`
- `name` — package name
- `description` — readme-style blurb (PyPI uses the project summary here)
- `latest_version` — semver string for the latest release used in the graph

**`versions.csv`**

- `ecosystem`, `package_name`, `version`, `released`
- `has_repository` — *(NPM only in raw files)* whether a repo URL was present; PyPI raw rows omit this column (clean fills empty)
- `github_owner`, `github_repo` — parsed GitHub location when metadata allows

**`dependencies.csv`**

- `ecosystem`, `from_package`, `from_version`, `to_package`, `version_spec`, `dep_kind` — edge from one package version to a dependency (NPM: dependency / peer / optional; PyPI: `requires_dist`)

**`maintainers.csv`**

- `ecosystem`, `package_name`, `username`, `name`, `role`, `email`

#### Clean file schemas (normalized)

Global rules: `ecosystem` lowercased; package names lowercased; PyPI names use `-` instead of `_`; strings trimmed; deterministic sort order; IDs are UUIDv5 strings in a fixed project namespace.

**`packages_clean.csv`**: `id`, `ecosystem`, `name`, `description`, `latest_version` — unique on `(ecosystem, name)`.

**`versions_clean.csv`**: `id`, `package_id`, `ecosystem`, `package_name`, `version`, `released`, `has_repository`, `github_owner`, `github_repo` — unique on `(ecosystem, package_name, version)`.

**`dependencies_clean.csv`**: `id`, `ecosystem`, `from_package`, `from_version`, `to_package`, `version_spec`, `dep_kind`, `from_version_id`, `to_package_id` — unique on the natural edge key; stub package rows are added so `to_package_id` usually resolves when the name appears in the graph.

**`maintainers_clean.csv`**: `id`, `ecosystem`, `package_name`, `package_id`, `username`, `name`, `role`, `email` — deduped on `(ecosystem, package_name, role, username, name, email)`.

See `manifest.json` after each run for row counts and `unresolved_*` diagnostics.
