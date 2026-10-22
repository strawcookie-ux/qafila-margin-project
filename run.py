"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import duckdb
import polars as pl
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"
APP_TABLES = ["orders", "order_items", "products", "stores"]      # the four tables of the app database we need

# The dbt models published to Iceberg, with the columns that make one row unique (also the sort order we write in,
# so the same data always produces the same file and a re-run can be compared row by row).
PUBLISH = {
    "silver.product_cost_history": ["sku", "valid_from"],
    "gold.weekly_margin": ["iso_week", "city", "category"],
    "quality.price_quarantine": ["source_file", "sku", "valid_from", "raw_cost"],
}


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive.

    - From data_dir/qafila_oltp.sqlite: orders, order_items, products, stores.
    - Every data_dir/suppliers/prices_*.csv, keeping the file name of each row.

    Running this twice must not duplicate anything.
    """
    supplier_files = (data_dir / "suppliers" / "prices_*.csv").as_posix()   # DuckDB likes forward slashes, also on Windows

    with duckdb.connect(str(db)) as con:  # closes the file before dbt opens it
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")

        # App database: read the four tables with Python's own sqlite3 driver and Polars (Lab 0 · Polars intro),
        # then copy them into raw as they are. DuckDB's ATTACH ... (TYPE sqlite) would need an extension that is
        # downloaded on first use, and the grader's machine may have no internet: this way needs nothing extra.
        # CREATE OR REPLACE = a full refresh, so running twice never duplicates a row.
        with closing(sqlite3.connect(data_dir / "qafila_oltp.sqlite")) as app:
            for table in APP_TABLES:
                # infer_schema_length=None: look at every row before choosing a type. promo_code is empty on
                # most orders, and guessing from the first 100 rows could type it as "always null".
                incoming = pl.read_database(f"SELECT * FROM {table}", app, infer_schema_length=None)
                con.register("incoming", incoming)
                con.sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM incoming")
                con.unregister("incoming")

        # Supplier files: every column read as text (all_varchar), so nothing is "fixed" on the way in:
        # '9,49', '٩٫٤٩' and '-5.5' land exactly as the supplier wrote them, and staging decides what they mean.
        # union_by_name lines up the files by column name: W39 renamed unit_cost -> cost_sar and added currency.
        # filename = true keeps the file each row came from (rule 5 needs it to pick the winning file).
        con.sql(f"""
          CREATE OR REPLACE TABLE raw.supplier_prices AS
          SELECT * EXCLUDE (filename),
                 parse_filename(filename) AS source_file
          FROM read_csv('{supplier_files}', union_by_name = true, all_varchar = true, filename = true)
        """)
        # A dataset where no file uses one of the two cost columns would leave it out of the table,
        # and staging reads both: make sure both always exist (empty if no file has them).
        for col in ["unit_cost", "cost_sar"]:
            con.sql(f"ALTER TABLE raw.supplier_prices ADD COLUMN IF NOT EXISTS {col} VARCHAR")

        counts = {t: con.sql(f"SELECT count(*) FROM raw.{t}").fetchone()[0] for t in APP_TABLES + ["supplier_prices"]}
        n_files = con.sql("SELECT count(DISTINCT source_file) FROM raw.supplier_prices").fetchone()[0]
    print("ingest:", ", ".join(f"{t} {n:,}" for t, n in counts.items()), f"({n_files} supplier files)")


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish three dbt models to Iceberg (see the brief for the exact columns).

        silver.product_cost_history   gold.weekly_margin   quality.price_quarantine

    Update existing tables in place, never drop and recreate them: the table's snapshots are
    how we see what a number was before the supplier's correction arrived.
    """
    catalog = get_catalog(warehouse)
    with duckdb.connect(str(db), read_only=True) as con:
        for name, keys in PUBLISH.items():
            data = con.sql(f"SELECT * FROM {name} ORDER BY {', '.join(keys)}").to_arrow_table()
            catalog.create_namespace_if_not_exists(name.split(".")[0])

            # First run: create the table. This is the only time a table is ever created.
            if not catalog.table_exists(name):
                table = catalog.create_table(name, schema=data.schema)
                table.append(data)
                print(f"publish: {name:30s} created, {data.num_rows:,} rows")
                continue

            # Later runs: compare with what the table holds now. Same rows -> no commit at all, so a re-run on the
            # same data leaves the table (and its snapshot list) exactly as it was.
            table = catalog.load_table(name)
            current = table.scan().to_arrow().select(data.column_names)
            if current.sort_by([(k, "ascending") for k in keys]).to_pylist() == data.to_pylist():
                print(f"publish: {name:30s} unchanged, {data.num_rows:,} rows ({len(table.snapshots())} snapshots)")
                continue

            # Something changed (e.g. the supplier's correction arrived): overwrite IN PLACE. Iceberg writes a new
            # snapshot and keeps the old one, so time travel still shows the numbers from before the change.
            table.overwrite(data)
            print(f"publish: {name:30s} updated, {data.num_rows:,} rows ({len(table.snapshots())} snapshots)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
