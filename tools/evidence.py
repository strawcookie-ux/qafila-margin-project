"""Print the evidence the README quotes: data problems (with counts) and the Iceberg history of the outputs.

    uv run python tools/evidence.py --data-dir data --warehouse warehouse

Read-only: it opens the DuckDB file read-only and only scans the Iceberg tables.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run import get_catalog  # noqa: E402  (the same catalog the pipeline and the grader use)


def data_problems(data_dir: Path, db: Path) -> None:
    """Count every problem from DATA_ISSUES.txt that touches this project, straight from raw / staging."""
    with duckdb.connect(str(db), read_only=True) as con:
        q = lambda sql: con.sql(sql).fetchone()[0]                    # noqa: E731
        counts = {
            "decimal commas (9,49)": q(r"SELECT count(*) FROM raw.supplier_prices WHERE unit_cost LIKE '%,%'"),
            "Arabic-Indic digits (٩٫٤٩)": q("SELECT count(*) FROM raw.supplier_prices WHERE regexp_matches(coalesce(cost_sar, unit_cost), '[٠-٩]')"),
            "rows using cost_sar (renamed column)": q("SELECT count(*) FROM raw.supplier_prices WHERE cost_sar IS NOT NULL"),
            "zero / negative / unparseable cost": q("SELECT count(*) FROM quality.price_quarantine WHERE reason = 'invalid_cost'"),
            "SKUs Qafila doesn't sell": q("SELECT count(*) FROM quality.price_quarantine WHERE reason = 'unknown_sku'"),
            "product SKUs with trailing spaces": q("SELECT count(*) FROM raw.products WHERE sku <> trim(sku)"),
            "order lines with quantity <= 0": q("SELECT count(*) FROM raw.order_items WHERE quantity <= 0"),
            "rows replaced by a later file (rule 5)": q("""
                SELECT count(*) - count(DISTINCT (sku, valid_from))
                FROM silver.supplier_price_checks WHERE reject_reason IS NULL"""),
            "repeated costs merged into one version": q("""
                SELECT count(DISTINCT (sku, valid_from)) FROM silver.supplier_price_checks WHERE reject_reason IS NULL""")
                - q("SELECT count(*) FROM silver.product_cost_history"),
            "counted lines without a valid cost (uncosted)": q("SELECT sum(uncosted_lines) FROM gold.weekly_margin"),
        }
    boms = [f.name for f in sorted((data_dir / "suppliers").glob("prices_*.csv")) if f.read_bytes()[:3] == b"\xef\xbb\xbf"]
    print("Data problems")
    for name, n in counts.items():
        print(f"  {name:48s} {n:>6,}")
    print(f"  {'files starting with a byte-order mark':48s} {', '.join(boms) or 'none'}")


def iceberg_history(warehouse: Path) -> None:
    """Every snapshot of the three tables; for gold.weekly_margin, the totals each snapshot holds (time travel)."""
    catalog = get_catalog(warehouse)
    print("\nIceberg snapshots")
    for name in ["silver.product_cost_history", "gold.weekly_margin", "quality.price_quarantine"]:
        table = catalog.load_table(name)
        print(f"  {name}")
        for s in table.snapshots():
            rows = table.scan(snapshot_id=s.snapshot_id).to_arrow()
            line = f"    {s.snapshot_id:<20} {s.summary.operation.value:9s} rows {rows.num_rows:>4}"
            if name == "gold.weekly_margin" and rows.num_rows:
                df = rows.to_pandas()
                line += f" | cost_sar {df.cost_sar.sum():>12,.2f} | margin_sar {df.margin_sar.sum():>12,.2f}"
            print(line)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()
    data_problems(args.data_dir, args.warehouse / "qafila.duckdb")
    iceberg_history(args.warehouse)


if __name__ == "__main__":
    main()
