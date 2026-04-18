# Query Optimization Log

> Running table of before/after timings for every route whose backing SQL
> query is a candidate for the "complex query" rubric requirement. The
> CIS 5500 rubric (see `PLAN.md` §8) mandates **≥2 complex queries with
> >15s pre-optimization and <1s post-optimization** and a before/after
> table in the final report — this file is the working version of that
> table.

## How to fill this in

1. Check out the branch the route lives on and spin up the Supabase
   project loaded with a realistic top-N scrape (see `collect_data.py`
   and `docs/proposed-indexes.md` §2 for size assumptions).
2. Warm the cache with one invocation, then benchmark `N=5` runs
   back-to-back via `curl -w '%{time_total}\n'` or the REST Client
   file, and record the median into the **pre-optimization ms** column.
3. Apply the optimization listed in the **optimization applied**
   column (index, view, materialized view, query rewrite) by opening a
   dedicated PR — never mix measurement with optimization landings.
4. Re-benchmark with the same inputs and record the median into the
   **post-optimization ms** column.
5. In **notes**, link the PR that landed the optimization plus any
   EXPLAIN ANALYZE / index-usage screenshots.
6. When the rubric target is reached for a row, strike it through in
   the table (keep the row for historical context; do not delete it).

## Log

| Route | Input scale | Pre-optimization ms | Optimization applied | Post-optimization ms | Notes |
|-------|-------------|---------------------|----------------------|----------------------|-------|
| R10 `/api/risk/ranked` (Q10) | 1 NPM ecosystem (~10⁴–10⁵ packages) | TBD | Phase 1: `v_risk_signals` view + TS-side `computeComposite`. Phase 2: `mv_risk_ranked` materialized view that persists composite + bucket so the route becomes `SELECT * FROM mv_risk_ranked ORDER BY composite DESC LIMIT`. | TBD | Baseline is the B-rank PR. Phase-2 optimization tracked in `docs/proposed-indexes.md` §4 (MV2 candidate). |
