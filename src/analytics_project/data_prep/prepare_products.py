"""scripts/data_preparation/prepare_products.py.

This script reads data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

"""

#####################################
# Import Modules at the Top
#####################################

# Import from Python Standard Library
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Project root and /src on sys.path (src layout).
# File: …/PROJECT/src/analytics_project/data_prep/prepare_products.py
# parents[0]=data_prep, [1]=analytics_project, [2]=src, [3]=PROJECT
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = Path(__file__).resolve().parents[2]
for p in (str(PROJECT_ROOT), str(SRC_DIR)):
    if p not in sys.path:
        sys.path.append(p)

from analytics_project.utils.logger import logger  # noqa: E402

# Paths (under project root, not under src/)
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

for p in (DATA_DIR, RAW_DATA_DIR, PREPARED_DATA_DIR):
    p.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# IO helpers
# ──────────────────────────────────────────────────────────────────────────────
def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw CSV safely with encoding fallbacks."""
    file_path = RAW_DATA_DIR / file_name
    logger.info(f"READING RAW DATA: {file_path}")
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(file_path, encoding=enc)
        except UnicodeDecodeError:
            logger.warning(f"Decode failed with {enc}. Trying next encoding…")
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return pd.DataFrame()
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error reading {file_path} with {enc}: {e}")
            return pd.DataFrame()
    logger.error(f"All encoding attempts failed for: {file_path}")
    return pd.DataFrame()


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save the prepared DataFrame to CSV."""
    out_path = PREPARED_DATA_DIR / file_name
    logger.info(f"WRITING CLEANED DATA: {out_path}")
    df.to_csv(out_path, index=False)
    logger.info(f"Saved cleaned data ({len(df)} rows) to: {out_path}")


# ──────────────────────────────────────────────────────────────────────────────
# Cleaning Steps
# ──────────────────────────────────────────────────────────────────────────────
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate product records based on ProductID only.
    Keeps the first row for each ProductID.
    """
    start_shape = df.shape

    if "ProductID" in df.columns:
        logger.info(f"Removing duplicates using ProductID. start shape={start_shape}")
        out = df.drop_duplicates(subset=["ProductID"], keep="first")
        removed = start_shape[0] - out.shape[0]
        logger.info(f"Removed {removed} duplicated ProductID rows. new shape={out.shape}")
        return out

    # Fallback if column doesn't exist
    logger.warning("ProductID column not found — falling back to exact row de-dupe.")
    out = df.drop_duplicates()
    logger.info(f"Exact de-dupe result shape={out.shape}")
    return out


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Apply simple defaults without changing original column names."""
    logger.info("Handling missing values…")
    logger.info(f"Missing (before):\n{df.isna().sum().to_string()}")

    if "ProductName" in df.columns:
        df["ProductName"] = df["ProductName"].fillna("Unknown Product")

    if "YearReleased" in df.columns:
        mode_year = df["YearReleased"].mode(dropna=True)
        if not mode_year.empty:
            df["YearReleased"] = df["YearReleased"].fillna(mode_year.iat[0])

    if "UnitPrice" in df.columns:
        df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
        if df["UnitPrice"].notna().any():
            df["UnitPrice"] = df["UnitPrice"].fillna(df["UnitPrice"].median())

    logger.info(f"Missing (after):\n{df.isna().sum().to_string()}")
    return df


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Apply formatting rules without altering column case.

    - ProductName: keep text after first hyphen (drop prefix+hyphen)
    - YearReleased: keep first 4-digit year if present
    - UnitPrice: ensure numeric (round later)
    """
    logger.info("Standardizing formats…")

    if "ProductName" in df.columns:
        df["ProductName"] = (
            df["ProductName"].astype(str).str.replace(r"^[^-]*-\s*", "", regex=True).str.strip()
        )

    if "YearReleased" in df.columns:
        year = df["YearReleased"].astype(str).str.extract(r"(\d{4})")[0]
        df["YearReleased"] = year.fillna(df["YearReleased"])
        df["YearReleased"] = pd.to_numeric(df["YearReleased"], errors="coerce").astype("Int64")

    if "UnitPrice" in df.columns:
        df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")

    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove outliers using the IQR rule on numeric product size/price columns."""
    logger.info("Removing outliers via IQR on numeric columns…")

    candidates = ["Price", "UnitPrice", "Weight", "Length", "Width", "Height"]
    numeric_cols = [c for c in candidates if c in df.columns]

    start = len(df)
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr == 0:
            continue
        lb, ub = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        before = len(df)
        df = df[(df[col] >= lb) & (df[col] <= ub)]
        logger.info(f"{col}: bounds [{lb:.6g}, {ub:.6g}] removed {before - len(df)} rows")

    logger.info(f"Total removed: {start - len(df)}; remaining: {len(df)}")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce non-negative prices."""
    logger.info("Validating data…")
    for col in ("UnitPrice", "Price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df[df[col] >= 0]
    return df


def finalize_presentation(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure UnitPrice always displays 2 decimal places, including 0.00.

    Convert only at the end, after numeric operations.
    """
    if "UnitPrice" in df.columns:
        df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0).round(2)
        df["UnitPrice"] = df["UnitPrice"].map("{:.2f}".format)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    """Run the product data cleaning pipeline."""
    logger.info("=== START prepare_products.py ===")

    input_file = "products_data.csv"
    output_file = "products_prepared.csv"

    df = read_raw_data(input_file)
    if df.empty:
        logger.error("No data to process (empty DataFrame). Exiting.")
        return

    original_shape = df.shape
    logger.info(f"Initial shape: {original_shape}")

    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = standardize_formats(df)
    df = remove_outliers(df)
    df = validate_data(df)
    df = finalize_presentation(df)  # ensures 2-decimal formatting

    save_prepared_data(df, output_file)

    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Final shape:    {df.shape}")
    logger.info("=== FINISHED prepare_products.py ===")


if __name__ == "__main__":
    main()
