"""Prepare customer data for analytics.

Usage (from project root):
    uv run python -m analytics_project.data_preparation.prepare_customers

Expected project layout:
    PROJECT/
    ├─ src/
    │  └─ analytics_project/
    │     ├─ __init__.py
    │     ├─ utils/
    │     │  ├─ __init__.py
    │     │  └─ logger.py    # must define `logger`
    │     └─ data_preparation/
    │        └─ prepare_customers.py  (this file)
    └─ data/
       ├─ raw/
       │  └─ customers_data.csv
       └─ prepared/

Notes:
- No duplicate function definitions
- No half-written code inside docstrings
- Clear, minimal path logic
- Safe handling when files/columns are missing
"""

from __future__ import annotations


import re

from pathlib import Path

import pandas as pd

from analytics_project.utils.logger import logger  # noqa: E402


# This file lives at:
# smart-store-karlidean/src/analytics_project/data_preparation/prepare_customers.py

# Path to this file
FILE_PATH = Path(__file__).resolve()

# Your project root is the folder that contains BOTH `src` and `data`
PROJECT_ROOT = FILE_PATH.parents[3]  # correct for your layout

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Import logger from package (assumes package imports succeed when run with -m)
from analytics_project.utils.logger import logger  # noqa: E402

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read a CSV from data/raw. Returns empty DataFrame on error."""
    file_path = RAW_DATA_DIR / file_name
    try:
        logger.info("READING RAW DATA: %s", file_path)
        df = pd.read_csv(file_path)
        logger.info("Loaded %s rows, %s columns", len(df), len(df.columns))
        return df
    except FileNotFoundError:
        logger.error("File not found: %s", file_path)
        return pd.DataFrame()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Error reading %s: %s", file_path, exc)
        return pd.DataFrame()


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Write DataFrame to data/prepared."""
    out_path = PREPARED_DATA_DIR / file_name
    logger.info("WRITING CLEANED DATA: %s", out_path)
    df.to_csv(out_path, index=False)
    logger.info("Saved cleaned data (%s rows) to: %s", len(df), out_path)


# --- Duplicate handling -------------------------------------------------------


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate customer records.

    If a `CustomerID` column exists, de-dupe on it (keep first). Otherwise, drop
    exact duplicate rows.
    """
    logger.info("Removing duplicates… start shape=%s", df.shape)
    if "CustomerID" in df.columns:
        out = df.drop_duplicates(subset=["CustomerID"], keep="first")
    else:
        logger.warning("CustomerID not found — dropping exact duplicate rows.")
        out = df.drop_duplicates()
    logger.info("Duplicates removed. new shape=%s", out.shape)
    return out


# --- Missing values -----------------------------------------------------------


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Fill common missing values per business rules.

    - MemberPoints: coerce to numeric, fill NaNs with median if any non-NaN
    - MemberStatus: fill with mode
    - Preferred Contact: treat blanks/placeholder strings as NA, fill with mode
    """
    logger.info("Handling missing values…")
    logger.info(
        "Missing by column (before):\n%s",
        df.isna().sum().to_string(),
    )

    # MemberPoints -> numeric + median
    if "MemberPoints" in df.columns:
        df["MemberPoints"] = pd.to_numeric(df["MemberPoints"], errors="coerce")
        if df["MemberPoints"].notna().any():
            df["MemberPoints"] = df["MemberPoints"].fillna(
                df["MemberPoints"].median(),
            )

    # MemberStatus -> mode
    if "MemberStatus" in df.columns:
        mode = df["MemberStatus"].mode(dropna=True)
        if not mode.empty:
            df["MemberStatus"] = df["MemberStatus"].fillna(mode.iat[0])

    # Preferred Contact variations -> normalize + fill with mode
    contact_col: str | None = None

    # 1) Try exact known names
    if "Preferred_Contact" in df.columns:
        contact_col = "Preferred_Contact"
    elif "Preferred Contact" in df.columns:
        contact_col = "Preferred Contact"
    elif "PreferredContact" in df.columns:
        contact_col = "PreferredContact"
    else:
        # 2) Fuzzy match: any col that contains both 'preferred' and 'contact'
        for col in df.columns:
            col_lower = col.lower()
            if "preferred" in col_lower and "contact" in col_lower:
                contact_col = col
                logger.info(
                    "Inferred Preferred Contact column as: %r",
                    contact_col,
                )
                break

    if contact_col is not None:
        logger.info("Cleaning Preferred Contact column: %r", contact_col)

        # Treat true blanks / whitespace as missing
        df[contact_col] = df[contact_col].replace(
            r"^\s*$",
            pd.NA,
            regex=True,
        )

        # Also treat common placeholder strings as missing
        df[contact_col] = df[contact_col].replace(
            ["nan", "NaN", "None", "NONE", "N/A", "n/a"],
            pd.NA,
        )

        # Fill with mode
        mode_pc = df[contact_col].mode(dropna=True)
        if not mode_pc.empty:
            fill_value = mode_pc.iat[0]
            logger.info(
                "Filling Preferred Contact missing values with mode: %r",
                fill_value,
            )
            df[contact_col] = df[contact_col].fillna(fill_value)
        else:
            logger.warning(
                "Preferred Contact column %r has no non-null values; skipping mode fill.",
                contact_col,
            )
    else:
        logger.warning(
            "No Preferred Contact column found (by name or pattern). Available columns: %s",
            list(df.columns),
        )

    logger.info(
        "Missing by column (after):\n%s",
        df.isna().sum().to_string(),
    )
    return df


# --- Text standardization (names) --------------------------------------------

_PREFIXES = r"(mr|mrs|ms|miss|dr|prof)"
_SUFFIXES = r"(jr|sr|ii|iii|iv|md|dds|phd|dmd|esq|esquire)"
_prefix_pat = re.compile(rf"^{_PREFIXES}\.?\s+", flags=re.IGNORECASE)
_suffix_pat = re.compile(rf"\s*,?\s*{_SUFFIXES}\.?$", flags=re.IGNORECASE)


def _clean_name(name: str) -> str:
    """Remove prefixes/suffixes and collapse whitespace."""
    if name is None:
        return ""
    s = str(name).strip()
    while True:
        new = _prefix_pat.sub("", s)
        if new == s:
            break
        s = new.strip()
    while True:
        new = _suffix_pat.sub("", s)
        if new == s:
            break
        s = new.strip()
    return re.sub(r"\s+", " ", s).strip()


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize text formats; currently cleans the `Name` column if present."""
    logger.info("Standardizing formats (Name cleanup)…")
    if "Name" in df.columns:
        df["Name"] = df["Name"].apply(_clean_name)
    else:
        logger.warning("Name column not found. Skipping name cleanup.")
    return df


# --- Outliers -----------------------------------------------------------------


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where MemberPoints is beyond ±2 std from the mean."""
    logger.info("Removing outliers on MemberPoints (±2 std)…")
    if "MemberPoints" not in df.columns:
        logger.warning("MemberPoints not found. Skipping outlier removal.")
        return df

    mp = pd.to_numeric(df["MemberPoints"], errors="coerce")
    mean_val = mp.mean()
    std_val = mp.std()

    if pd.isna(mean_val) or pd.isna(std_val) or std_val == 0:
        logger.warning(
            "Insufficient variance in MemberPoints; skipping outlier removal.",
        )
        return df

    lower = mean_val - 2 * std_val
    upper = mean_val + 2 * std_val

    start = len(df)
    mask = (mp >= lower) & (mp <= upper)
    out = df.loc[mask].copy()
    removed = start - len(out)
    logger.info(
        "Outlier removal complete. Removed %s rows. Remaining: %s",
        removed,
        len(out),
    )
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline
# ──────────────────────────────────────────────────────────────────────────────


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Strip spaces, replace internal whitespace with underscore, drop non-word chars."""
    old_cols = df.columns.tolist()
    df.columns = (
        df.columns.str.strip()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^0-9A-Za-z_]", "", regex=True)
    )
    if old_cols != df.columns.tolist():
        renames = [f"{o} -> {n}" for o, n in zip(old_cols, df.columns, strict=False) if o != n]
        logger.info(
            "Column normalization applied: %s",
            ", ".join(renames),
        )
    return df


def main() -> None:
    """Run the customer data preparation workflow."""
    logger.info("==================================")
    logger.info("STARTING prepare_customers.py")
    logger.info("==================================")
    logger.info("Project root : %s", PROJECT_ROOT)
    logger.info("data/raw     : %s", RAW_DATA_DIR)
    logger.info("data/prepared: %s", PREPARED_DATA_DIR)

    input_file = "customers_data.csv"
    output_file = "customers_prepared.csv"

    df = read_raw_data(input_file)
    if df.empty:
        logger.error("No data to process (empty DataFrame). Exiting.")
        return

    logger.info("Initial shape: %s", df.shape)
    logger.info("Initial columns: %s", list(df.columns))

    # Cleaning pipeline
    df = normalize_column_names(df)
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = standardize_formats(df)
    df = remove_outliers(df)

    save_prepared_data(df, output_file)

    logger.info("====== CLEANING COMPLETE ======")
    logger.info("Final rows:    %s", df.shape[0])
    logger.info("================================")


if __name__ == "__main__":
    main()
