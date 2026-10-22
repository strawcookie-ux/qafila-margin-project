# Qafila gross margin

Submitted by: SHAHAD BANDAR AL SHAMMARI — academy: @SDAIAAcademy

The task, rules and grading are in [BRIEF.md](BRIEF.md).

```bash
uv sync
uv run python tools/make_data.py          # data/
uv run python run.py                      # data/ -> warehouse/ (DuckDB + Iceberg)
uv run python tools/check.py              # contract self-check
uv run python tools/evidence.py           # the data-problem counts and Iceberg snapshots quoted below
```

[PROJECT.ipynb](PROJECT.ipynb) walks through the same pipeline step by step (runs on Google Colab): every file of the project is written by a `%%writefile` cell where it's explained, then the notebook runs it, re-runs it, adds the correction and reads the Iceberg snapshots.

## Design

**Flow:** supplier CSVs + app SQLite → `raw` (DuckDB, `run.py: ingest`) → `staging` (dbt views) → `silver` (dbt tables: checks, SCD2 history, costed lines) → `gold` / `quality` (dbt tables) → Iceberg (`run.py: publish`, PyIceberg).

| Model | Layer | Grain (one row per ...) | Materialization |
|---|---|---|---|
| `stg_orders` | staging | order, with its Riyadh `order_date` | view |
| `stg_order_items` | staging | order line (`order_id`, `line_no`), with `revenue_sar` | view |
| `stg_products` / `stg_stores` | staging | product (SKU trimmed) / store | view |
| `stg_supplier_prices` | staging | row of a supplier file, cost parsed, nothing filtered | view |
| `supplier_price_checks` | silver | row of a supplier file, with `reject_reason` (NULL = usable) | table |
| `product_cost_history` | silver | SKU × cost version (`valid_from` inclusive, `valid_to` exclusive) | table |
| `order_lines_costed` | silver | counted order line, with the cost valid on its order date | table |
| `weekly_margin` | gold | `iso_week` × `city` × `category` with at least one counted line | table |
| `price_quarantine` | quality | rejected supplier row | table |

`raw` is a full refresh every run (`CREATE OR REPLACE`), and every supplier column is landed as text, so nothing is "fixed" before staging. The app tables are read with Python's `sqlite3` and Polars rather than DuckDB's `ATTACH ... (TYPE sqlite)`, which downloads an extension on first use: the pipeline needs no internet connection. The quarantine rule is written once, in `supplier_price_checks`: the history reads its usable rows and the quarantine reads the rest, so they can't disagree. The only fixed date is the first supplier week, a dbt var (`margin_start_date`); file names, weeks and SKUs all come from the data.

**How the SCD2 history is built:** usable rows are first reduced to one quote per SKU and `valid_from`; when two files quote the same pair, the file name that sorts last wins, which is how `prices_2026-W39_fix.csv` replaces `W39` without naming either file. Each quote is compared with the SKU's previous quote (`lag`): only a different cost starts a new version, and a week where the SKU isn't quoted has no row at all, so the previous cost simply carries on. `valid_to` is the next version's `valid_from` (`lead`), empty for the current one.

**How each order line finds its cost:** a point-in-time left join on the trimmed SKU, `valid_from <= order_date < valid_to` (or no `valid_to`). Versions never overlap, so a line matches at most one (a uniqueness test on `order_lines_costed` guards that); a line with no match stays in with a NULL cost and counts as uncosted.

## Data problems I found

Counts from the course dataset (seed 2026), printed by `tools/evidence.py`.

| Problem | Where | Rows affected | What I did |
|---|---|---|---|
| Decimal commas (`9,49`) | `prices_2026-W38.csv` | 126 | Read as text, `translate` the comma to a dot, then `try_cast` |
| Arabic-Indic digits (`٩٫٤٩`) | `prices_2026-W38.csv` | 34 | Same `translate`: `٠-٩` → `0-9`, `٫` → `.` |
| Byte-order mark | `prices_2026-W38.csv` | whole file | DuckDB's CSV reader skips it; the header reads `supplier_id` |
| Renamed column `unit_cost` → `cost_sar` (+ `currency`) | `prices_2026-W39.csv` | 160 | `union_by_name` when landing; `coalesce(cost_sar, unit_cost)` in staging; both columns always exist in `raw` |
| Zero or negative cost | `prices_2026-W39.csv` | 4 | Quarantined, `invalid_cost` |
| SKUs Qafila doesn't sell (`QF-NEW-…`) | `prices_2026-W39.csv` | 3 | Quarantined, `unknown_sku` (checked before the cost) |
| SKUs with trailing spaces | `products` | 2 | `trim` on both sides before any comparison |
| Quantity ≤ 0 | `order_items` | 142 | Kept in staging, excluded from the margin (rule 7) |
| Order times are UTC text | `orders` | every row | `created_at + 3 hours` → Riyadh `order_date` |
| Lines without a valid cost | margin | 20,626 | Counted in `lines` / `revenue_sar`, not in the cost columns: 28 products are never quoted, and others are ordered before their first quote |
| Late correction for W39 | `prices_2026-W39_fix.csv` | 9 (5 replace a W39 quote, 4 fix quarantined rows) | Rule 5: the later file name wins; Iceberg keeps the old numbers |

## Tests

`dbt build` runs 27 tests. Tests on the **inputs** (staging) warn, so an unexpected source never stops the run. Tests on **my outputs** fail the build.

| Test | On | What it protects against |
|---|---|---|
| `unique` (`sku`, `valid_from`) + `not_null` | `product_cost_history` | Two costs for the same SKU on the same day |
| `assert_history_one_current_version_per_sku` | `product_cost_history` | A SKU with no current cost, or two |
| `assert_history_versions_chain_without_gaps` | `product_cost_history` | Gaps or overlaps between versions, empty versions, a "new" version with the same cost |
| `assert_history_costs_are_positive` | `product_cost_history` | A zero/negative cost slipping past the quarantine |
| `unique` (`order_id`, `line_no`) | `order_lines_costed` | The point-in-time join matching two versions and counting a line twice |
| `unique` (`iso_week`, `city`, `category`) + `not_null` | `weekly_margin` | A broken grain |
| `assert_margin_adds_up` | `weekly_margin` | `margin ≠ costed revenue − cost`, costed revenue above revenue, more uncosted lines than lines |
| `assert_margin_reconciles_with_source` | `weekly_margin` vs staging | A join silently dropping or duplicating eligible lines or revenue |
| `assert_every_supplier_row_is_used_or_quarantined` | checks vs quarantine | A supplier row vanishing without a trace |
| `accepted_values` + `not_null` on `reason` | `price_quarantine`, `supplier_price_checks` | A reason other than `unknown_sku` / `invalid_cost` |
| `unique` / `not_null` (warn) | staging: `order_id`, trimmed `sku`, line key, `valid_from`, `order_date` | Source surprises: duplicate SKUs after trimming, unparseable dates |

## Re-runs and the correction

`publish()` compares each model with the current Iceberg table: identical rows → no commit; different → `table.overwrite()` **in place** (a new snapshot, the old one kept). Tables are only created on the very first run.

| Run | Data | `product_cost_history` | `weekly_margin` | `price_quarantine` |
|---|---|---|---|---|
| 1 | empty warehouse | created, 633 rows | created, 165 rows | created, 7 rows |
| 2 | same | unchanged (1 snapshot) | unchanged (1 snapshot) | unchanged (1 snapshot) |
| 3 | + `W39_fix` | updated, 637 rows (3 snapshots) | updated, 165 rows (3 snapshots) | unchanged |
| 4 | same | unchanged (3 snapshots) | unchanged (3 snapshots) | unchanged |

Time travel on `gold.weekly_margin` (`overwrite` = a delete snapshot + an append snapshot):

| Snapshot | Operation | Rows | Total `cost_sar` | Total `margin_sar` |
|---|---|---|---|---|
| `9100076211011398883` | append (before the correction) | 165 | 1,654,199.98 | 801,016.88 |
| `5651348000588675627` | delete | 0 | | |
| `5300961335407549812` | append (after the correction) | 165 | 1,651,701.06 | 803,515.80 |

Scanning the first snapshot (`table.scan(snapshot_id=9100076211011398883)`) still returns the pre-correction numbers. Snapshot ids change on every fresh warehouse; `tools/evidence.py` prints yours.

I also ran the pipeline on seeds 11, 7 and 99 (`tools/make_data.py --seed …`), each on an empty warehouse, then again, then with its correction file: `tools/check.py` passed 22/22 every time and the second run changed nothing. Seed 99 ran from a fresh copy of the repository (`uv sync` first), the way the grader runs it.

## What I'd do next

- **Daily supplier files** are the first thing to break. The weekly file name is the only tie-breaker (rule 5), so two files for the same day need a real delivery timestamp or sequence number. And `publish()` overwrites whole tables: a few changed costs rewrite the full history and margin, which gets expensive.
- **100× more data:** `raw` is rebuilt from scratch on every run and the margin is recomputed for every order line. I'd load orders incrementally on `updated_at` with a look-back window (Lab 2.3), make `order_lines_costed` an incremental dbt model, and only recompute the weeks touched by a new or corrected price.
- **Publishing:** compare snapshots by a row hash instead of reading the full table back, and write only the changed partitions (partition `weekly_margin` by `iso_week`) with `overwrite_filter`.
- **Supplier feedback:** send `price_quarantine` back to the supplier automatically, and add a data contract (Lab 4.2) so a renamed column like `cost_sar` fails at the door instead of being absorbed by `coalesce`.

## AI assistance

used claude to:
1. save time  by writing the readme + Push the final commit to my public fork
2. help improve readability by adding comments to the notbook
3. help explain erorrs
