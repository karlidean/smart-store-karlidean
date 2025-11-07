"""scripts/analytics_project/data_preparation/prepare_customers.py

This script reads customer data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

"""

from __future__ import annotations

import pathlib
import re
import sys

import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Locate project root and /src, add to sys.path so we can import the package.
# Current file: …/PROJECT/src/analytics_project/data_prep/prepare_customers.py
# parents[0]=…/data_prep, [1]=…/analytics_project, [2]=…/src, [3]=…/PROJECT
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
SRC_DIR = pathlib.Path(__file__).resolve().parents[2]
for p in (str(PROJECT_ROOT), str(SRC_DIR)):
    if p not in sys.path:
        sys.path.append(p)

# Requires analytics_project/__init__.py and analytics_project/utils/__init__.py
from analytics_project.utils.logger import logger  # noqa: E402

# ──────────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────────
SCRIPTS_DIR = pathlib.Path(__file__).resolve().parents[2]  # …/src
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw CSV; return empty DataFrame on failure."""
    file_path = RAW_DATA_DIR / file_name
    try:
        logger.info(f"READING RAW DATA: {file_path}")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:  # noqa: BLE001
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Write cleaned CSV to data/prepared."""
    out_path = PREPARED_DATA_DIR / file_name
    logger.info(f"WRITING CLEANED DATA: {out_path}")
    df.to_csv(out_path, index=False)
    logger.info(f"Saved cleaned data ({len(df)} rows) to: {out_path}")


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows."""
    logger.info(f"Removing duplicates… start shape={df.shape}")
    out = df.drop_duplicates()
    logger.info(f"Duplicates removed. new shape={out.shape}")
    return out


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Fill MemberPoints (median), MemberStatus (mode), Preferred_Contact (mode incl. blanks)."""
    logger.info("Handling missing values…")
    logger.info(f"Missing by column (before):\n{df.isna().sum().to_string()}")

    # MemberPoints -> numeric + median
    if "MemberPoints" in df.columns:
        df["MemberPoints"] = pd.to_numeric(df["MemberPoints"], errors="coerce")
        if df["MemberPoints"].notna().any():
            df["MemberPoints"] = df["MemberPoints"].fillna(df["MemberPoints"].median())

    # MemberStatus -> mode
    if "MemberStatus" in df.columns:
        mode = df["MemberStatus"].mode()
        if not mode.empty:
            df["MemberStatus"] = df["MemberStatus"].fillna(mode.iat[0])

    # Preferred_Contact (or "Preferred Contact") -> treat blanks as NA, then fill with mode
    col_pc = (
        "Preferred_Contact"
        if "Preferred_Contact" in df.columns
        else ("Preferred Contact" if "Preferred Contact" in df.columns else None)
    )
    if col_pc:
        df[col_pc] = df[col_pc].astype(str)
        df[col_pc] = df[col_pc].replace(r"^\s*$", pd.NA, regex=True)
        mode_pc = df[col_pc].mode(dropna=True)
        if not mode_pc.empty:
            df[col_pc] = df[col_pc].fillna(mode_pc.iat[0])

    logger.info(f"Missing by column (after):\n{df.isna().sum().to_string()}")
    return df


# --- Name standardizer --------------------------------------------------------

_PREFIXES = r"(mr|mrs|ms|miss|dr|prof)"
_SUFFIXES = r"(jr|sr|ii|iii|iv|md|dds|phd|dmd|esq|esquire)"

_prefix_pat = re.compile(rf"^{_PREFIXES}\.?\s+", flags=re.IGNORECASE)
_suffix_pat = re.compile(rf"\s*,?\s*{_SUFFIXES}\.?$", flags=re.IGNORECASE)


def _clean_name(name: str) -> str:
    """Remove prefixes/suffixes from a single name string."""
    if name is None:
        return ""
    s = str(name).strip()
    # Strip multiple prefixes (e.g., "Dr. Prof. …")
    while True:
        new = _prefix_pat.sub("", s)
        if new == s:
            break
        s = new.strip()
    # Strip multiple suffixes (e.g., "MD, PhD")
    while True:
        new = _suffix_pat.sub("", s)
        if new == s:
            break
        s = new.strip()
    # Collapse internal whitespace
    return re.sub(r"\s+", " ", s).strip()


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize text formats; currently cleans Name prefixes/suffixes."""
    logger.info("Standardizing formats (Name cleanup)…")
    name_col = "Name" if "Name" in df.columns else None
    if not name_col:
        logger.warning("Name column not found. Skipping name cleanup.")
        return df

    df[name_col] = df[name_col].apply(_clean_name)
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where MemberPoints is beyond ±2 std from the mean."""
    logger.info("Removing outliers on MemberPoints (±2 std)…")
    if "MemberPoints" not in df.columns:
        logger.warning("MemberPoints not found. Skipping outlier removal.")
        return df

    # Ensure numeric
    mp = pd.to_numeric(df["MemberPoints"], errors="coerce")
    mean_val = mp.mean()
    std_val = mp.std()

    if pd.isna(mean_val) or pd.isna(std_val) or std_val == 0:
        logger.warning("Insufficient variance in MemberPoints; skipping outlier removal.")
        return df

    lower = mean_val - 2 * std_val
    upper = mean_val + 2 * std_val

    start = len(df)
    mask = (mp >= lower) & (mp <= upper)
    out = df.loc[mask].copy()
    removed = start - len(out)

    logger.info(f"Outlier removal complete. Removed {removed} rows. Remaining: {len(out)}")
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def main() -> None:
    """Run the customer data cleaning pipeline."""
    logger.info("====== STARTING CUSTOMER DATA CLEANING ======")
    logger.info(f"Project root : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts dir  : {SCRIPTS_DIR}")

    input_file = "customers_data.csv"
    output_file = "customers_prepared.csv"

    df = read_raw_data(input_file)
    if df.empty:
        logger.error("No data to process (empty DataFrame). Exiting.")
        return

    original_shape = df.shape
    logger.info(f"Initial shape: {original_shape}")
    logger.info(f"Initial columns: {list(df.columns)}")

    # Normalize column names (strip, spaces->underscores, remove non-word chars)
    old_cols = df.columns.tolist()
    df.columns = (
        df.columns.str.strip()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^0-9A-Za-z_]", "", regex=True)
    )
    if old_cols != df.columns.tolist():
        renames = [f"{o} -> {n}" for o, n in zip(old_cols, df.columns, strict=False) if o != n]
        logger.info(f"Column normalization applied: {', '.join(renames)}")

    # Clean pipeline
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = standardize_formats(df)  # Name cleanup
    df = remove_outliers(df)  # MemberPoints ±2 std

    save_prepared_data(df, output_file)

    logger.info("====== CLEANING COMPLETE ======")
    logger.info(f"Original rows: {original_shape[0]}")
    logger.info(f"Final rows:    {df.shape[0]}")
    logger.info("================================")


if __name__ == "__main__":
    main()
