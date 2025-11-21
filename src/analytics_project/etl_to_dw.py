"""End-to-end ETL loader for the Smart Sales data warehouse.

This script performs the full warehouse refresh process:
1. Resolves project-root paths for /data/prepared and /data/dw
2. Drops and recreates all dimension and fact tables in smart_sales.db
3. Reads cleaned CSV datasets (customers, products, sales)
4. Loads datasets into SQLite using pandas' to_sql
5. Produces a repeatable, deterministic warehouse state for downstream analytics

Intended for development, testing, and coursework demonstrations.
"""

import pathlib
import sqlite3
import sys

import pandas as pd

# For local imports, temporarily add project root to sys.path
# Note: this can be removed - our project uses a modern /src/ folder and __init__.py files
# To make local imports easier.
# Adjust the paths and code to fit with this updated organization.
# Questions: Ask them here in this project discussion and we can help.
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]

# Ensure imports work
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Build paths relative to project root
DW_DIR = PROJECT_ROOT / "data" / "dw"
DB_PATH = DW_DIR / "smart_sales.db"
PREPARED_DATA_DIR = PROJECT_ROOT / "data" / "prepared"


def reset_schema(cursor: sqlite3.Cursor) -> None:
    """Drop all warehouse tables (fact then dimensions)."""
    # Drop fact table first (it depends on dimensions)
    cursor.execute("DROP TABLE IF EXISTS fact_sales")
    cursor.execute("DROP TABLE IF EXISTS dim_date")
    cursor.execute("DROP TABLE IF EXISTS dim_store")
    cursor.execute("DROP TABLE IF EXISTS dim_product")
    cursor.execute("DROP TABLE IF EXISTS dim_customer")


def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in a star schema for the data warehouse."""

    # -----------------------
    # Dimension: Customer
    # -----------------------
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_customer (
            CustomerID        INTEGER PRIMARY KEY,
            CustomerName      TEXT,
            Region            TEXT,
            JoinDate          TEXT,
            MemberPoints      INTEGER,
            MemberStatus      TEXT,
            PreferredContact  TEXT
        )
        """
    )

    # -----------------------
    # Dimension: Product
    # -----------------------
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_product (
            ProductID      INTEGER PRIMARY KEY,
            ProductName    TEXT,
            Category       TEXT,
            UnitPrice      REAL,
            YearReleased   INTEGER,
            MonthPurchased TEXT,
            OrderingSKU    TEXT,
            SupplierName   TEXT
        )
        """
    )

    # -----------------------
    # Dimension: Store
    # -----------------------
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_store (
            StoreID         INTEGER PRIMARY KEY,
            StoreName       TEXT,
            Region          TEXT,
            StoreType       TEXT
        )
        """
    )

    # -----------------------
    # Dimension: Date
    # -----------------------
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            DateID         INTEGER PRIMARY KEY,
            DateValue      TEXT,
            Year           INTEGER,
            Quarter        INTEGER,
            Month          INTEGER,
            Week           INTEGER,
            Day            INTEGER,
            DayOfWeek      TEXT
        )
        """
    )

    # -----------------------
    # Fact Table: Sales
    # -----------------------
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_sales (
            TransactionID     INTEGER PRIMARY KEY,

            -- Foreign Keys
            CustomerID        INTEGER,
            ProductID         INTEGER,
            StoreID           INTEGER,
            DateID            INTEGER,  -- can be populated later

            -- Raw date (for now, matches CSV)
            SaleDate          TEXT,

            -- Measures
            SaleAmount        REAL,
            PercentDiscount   REAL,
            SaleFinal         REAL,
            PaidWithPoints    TEXT,

            FOREIGN KEY (CustomerID) REFERENCES dim_customer (CustomerID),
            FOREIGN KEY (ProductID)  REFERENCES dim_product (ProductID),
            FOREIGN KEY (StoreID)    REFERENCES dim_store (StoreID),
            FOREIGN KEY (DateID)     REFERENCES dim_date (DateID)
        )
        """
    )


def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the warehouse tables."""
    cursor.execute("DELETE FROM fact_sales")
    cursor.execute("DELETE FROM dim_customer")
    cursor.execute("DELETE FROM dim_product")
    cursor.execute("DELETE FROM dim_store")
    cursor.execute("DELETE FROM dim_date")


def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the dim_customer table."""
    customers_df.to_sql("dim_customer", cursor.connection, if_exists="append", index=False)


def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the dim_product table."""
    products_df.to_sql("dim_product", cursor.connection, if_exists="append", index=False)


def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the fact_sales table."""
    sales_df.to_sql("fact_sales", cursor.connection, if_exists="append", index=False)


def load_data_to_db() -> None:
    """Load the prepared CSV datasets into the smart_sales data warehouse.

    This function:
    - Ensures the data warehouse directory exists
    - Connects to the SQLite database
    - Drops and recreates warehouse tables
    - Loads prepared CSV files
    - Inserts data into customer, product, and sale tables
    - Commits and closes the connection
    """
    DW_DIR.mkdir(parents=True, exist_ok=True)
    conn: sqlite3.Connection | None = None

    try:
        # Connect to SQLite – will create the file if it doesn't exist
        conn = sqlite3.connect(DB_PATH)
        print(f"DB will be created at: {DB_PATH}")
        print(f"Reading prepared data from: {PREPARED_DATA_DIR}")

        cursor = conn.cursor()

        # Drop and recreate schema
        reset_schema(cursor)
        create_schema(cursor)

        # Load prepared data using pandas
        customers_df = pd.read_csv(PREPARED_DATA_DIR / "customers_prepared.csv")
        products_df = pd.read_csv(PREPARED_DATA_DIR / "products_prepared.csv")
        sales_df = pd.read_csv(PREPARED_DATA_DIR / "sales_prepared.csv")

        print(f"Customers Table Rows: {len(customers_df)}")
        print(f"Products Table Rows: {len(products_df)}")
        print(f"Sales Table Rows: {len(sales_df)}")

        # Insert data into the database
        insert_customers(customers_df, cursor)
        insert_products(products_df, cursor)
        insert_sales(sales_df, cursor)

        conn.commit()
        print("ETL LOADING STATUS: COMPLETE!")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    load_data_to_db()
