import sqlite3
import pandas as pd
from pathlib import Path

# ── Paths (script is in project_root/datawarehouse/)
THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parent

DB_PATH = PROJECT_ROOT / "smart_store_warehouse.sqlite3"
SQL_FILES = [
    "01_create_tables.sql",
    "02_load_dimensions.sql",
    "03_load_facts.sql",
]

CSV_DIR = PROJECT_ROOT / "data" / "prepared"
CSV_CUSTOMERS = CSV_DIR / "customers_prepared.csv"
CSV_PRODUCTS = CSV_DIR / "products_prepared.csv"
CSV_SALES = CSV_DIR / "sales_prepared.csv"

# Staging table names to use in your SQL files
STAGING_CUSTOMERS = "staging_customers"
STAGING_PRODUCTS = "staging_products"
STAGING_SALES = "staging_sales"


def ensure_db():
    DB_PATH.touch(exist_ok=True)
    print(f"Using SQLite DB: {DB_PATH}")


def load_staging(conn: sqlite3.Connection):
    """Load prepared CSVs into SQLite staging tables via pandas.to_sql()."""
    # Read CSVs
    customers_df = pd.read_csv(CSV_CUSTOMERS)
    products_df = pd.read_csv(CSV_PRODUCTS)
    sales_df = pd.read_csv(CSV_SALES)

    # Load/replace staging tables
    customers_df.to_sql(STAGING_CUSTOMERS, conn, if_exists="replace", index=False)
    products_df.to_sql(STAGING_PRODUCTS, conn, if_exists="replace", index=False)
    sales_df.to_sql(STAGING_SALES, conn, if_exists="replace", index=False)

    # Basic counts
    cur = conn.cursor()
    for tbl in (STAGING_CUSTOMERS, STAGING_PRODUCTS, STAGING_SALES):
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"   • {tbl}: {cur.fetchone()[0]} rows")
    cur.close()
    print("Loaded staging tables from CSVs.")


def run_sql_file(conn: sqlite3.Connection, path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing SQL file: {path}")
    sql_text = path.read_text(encoding="utf-8")
    # SQLite friendly: enable FKs, exec script
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(sql_text)
    print(f"Executed {path.name}")


def main():
    # sanity checks
    for p in (CSV_CUSTOMERS, CSV_PRODUCTS, CSV_SALES):
        if not p.exists():
            raise FileNotFoundError(f"Missing CSV: {p}")
    for name in SQL_FILES:
        if not (THIS_DIR / name).exists():
            print(f"Warning: {name} not found in {THIS_DIR}. (Will skip if missing)")

    ensure_db()

    with sqlite3.connect(DB_PATH) as conn:
        # 0) Load CSVs → staging_* tables in the **same DB**
        print("\n== Step 0: Load staging from CSVs ==")
        load_staging(conn)

        # 1..3) Execute SQL files from datawarehouse/
        for name in SQL_FILES:
            path = THIS_DIR / name
            if path.exists():
                print(f"\n== Running {name} ==")
                run_sql_file(conn, path)
            else:
                print(f"⏭Skipping missing file: {name}")

        print("\n🎉 Done.")


if __name__ == "__main__":
    main()
