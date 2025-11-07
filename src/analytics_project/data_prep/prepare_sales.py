"""scripts/data_preparation/prepare_sales.py

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
# File: …/PROJECT/src/analytics_project/data_prep/prepare_sales.py
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
    """Write cleaned CSV to data/prepared."""
    out_path = PREPARED_DATA_DIR / file_name
    logger.info(f"WRITING CLEANED DATA: {out_path}")
    df.to_csv(out_path, index=False)
    logger.info(f"Saved cleaned data ({len(df)} rows) to: {out_path}")
    # ^ If Ruff flags the f-string, change to len(df).


# ──────────────────────────────────────────────────────────────────────────────
# Cleaning steps
# ──────────────────────────────────────────────────────────────────────────────
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows."""
    logger.info(f"Removing duplicates… start shape={df.shape}")
    out = df.drop_duplicates()
    logger.info(f"Duplicates removed. new shape={out.shape}")
    return out


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values with safe defaults.

    - SaleAmount: numeric -> fill median
    - Quantity: numeric -> fill median, cast to Int64
    - OrderDate: parse to datetime; fill NaT with mode if available
    - PaidWithPoints: blanks/whitespace/NaN -> "No"
    """
    logger.info("Handling missing values…")
    logger.info(f"Missing (before):\n{df.isna().sum().to_string()}")

    # SaleAmount
    if "SaleAmount" in df.columns:
        df["SaleAmount"] = (
            df["SaleAmount"].astype(str).str.replace(r"[\$,]", "", regex=True).str.strip()
        )
        df["SaleAmount"] = pd.to_numeric(df["SaleAmount"], errors="coerce")
        if df["SaleAmount"].notna().any():
            df["SaleAmount"] = df["SaleAmount"].fillna(df["SaleAmount"].median())

    # Quantity
    if "Quantity" in df.columns:
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
        if df["Quantity"].notna().any():
            df["Quantity"] = df["Quantity"].fillna(df["Quantity"].median())
        df["Quantity"] = df["Quantity"].astype("Int64")

    # OrderDate
    if "OrderDate" in df.columns:
        parsed = pd.to_datetime(df["OrderDate"], errors="coerce")
        if parsed.notna().any():
            mode_dt = parsed.mode(dropna=True)
            if not mode_dt.empty:
                parsed = parsed.fillna(mode_dt.iat[0])
        df["OrderDate"] = parsed

    # PaidWithPoints (default blanks -> "No")
    if "PaidWithPoints" in df.columns:
        df["PaidWithPoints"] = (
            df["PaidWithPoints"].astype(str).str.strip().replace({"": "No"}).fillna("No")
        )

    logger.info(f"Missing (after):\n{df.isna().sum().to_string()}")
    return df


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize presentation formats without renaming columns.

    - OrderDate: convert to ISO YYYY-MM-DD
    - SaleAmount, PercentDiscount, SaleFinal: numeric + rounded to 2 decimals
    - Quantity: ensure Int64 dtype if present
    """
    logger.info("Standardizing formats…")

    # OrderDate → ISO string
    if "OrderDate" in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df["OrderDate"]):
            df["OrderDate"] = df["OrderDate"].dt.strftime("%Y-%m-%d")
        else:
            parsed = pd.to_datetime(df["OrderDate"], errors="coerce")
            df["OrderDate"] = parsed.dt.strftime("%Y-%m-%d")

    # Monetary/percent-like fields → numeric + round(2)
    for col in ("SaleAmount", "PercentDiscount", "SaleFinal"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r"[\$,]", "", regex=True).str.strip()
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    # Quantity stays integer-like
    if "Quantity" in df.columns:
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").astype("Int64")

    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove outliers via IQR on SaleAmount and Quantity (if present)."""
    logger.info("Removing outliers via IQR…")
    start = len(df)

    for col in ("SaleAmount", "Quantity"):
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
        q1, q3 = df[col].quantile(0.25), df[col].quantile(0.75)
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
    """Validate that SaleAmount and Quantity are non-negative."""
    logger.info("Validating data…")
    before = len(df)

    for col in ("SaleAmount", "Quantity"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            neg = df[df[col] < 0].shape[0]
            if neg:
                logger.info(f"{col}: dropping {neg} rows with negative values")
                df = df[df[col] >= 0]

    logger.info(f"Validation complete. Dropped {before - len(df)} rows.")
    return df


def finalize_presentation(df: pd.DataFrame) -> pd.DataFrame:
    """Force selected fields to display with 2 decimals (e.g., 0.00).

    Convert to formatted strings at the end, before saving.
    """
    for col in ("SaleAmount", "PercentDiscount", "SaleFinal"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).round(2)
            df[col] = df[col].map("{:.2f}".format)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    """Run the sales data cleaning pipeline."""
    logger.info("====== INITIALIZING SALES DATA CLEANING PROCESS ======")
    logger.info(f"Project root : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")

    input_file = "sales_data.csv"
    output_file = "sales_prepared.csv"

    df = read_raw_data(input_file)
    if df.empty:
        logger.error("No data to process (empty DataFrame). Exiting.")
        return

    original_shape = df.shape
    logger.info(f"Initial shape: {original_shape}")
    logger.info(f"Initial columns: {list(df.columns)}")

    # Preserve column case; just trim surrounding whitespace on names
    cols_before = df.columns.tolist()
    df.columns = df.columns.str.strip()
    if cols_before != df.columns.tolist():
        changes = [f"{o} -> {n}" for o, n in zip(cols_before, df.columns, strict=False) if o != n]
        logger.info(f"Column name trims applied: {', '.join(changes)}")

    # Cleaning pipeline
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = standardize_formats(df)
    df = remove_outliers(df)
    df = validate_data(df)
    df = finalize_presentation(df)  # ensures 0.00 formatting

    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape:  {df.shape}")
    logger.info("====== COMPLETED SALES DATA CLEANING PROCESS ======")


if __name__ == "__main__":
    main()
