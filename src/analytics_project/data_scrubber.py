"""
Utilities for cleaning pandas DataFrames.

Reusable `DataScrubber` for consistency checks, duplicates, missing values,
outlier filters, renaming/reordering columns, string standardization, and date parsing.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

# TC002: third-party imports only for typing
if TYPE_CHECKING:
    import pandas as pd
    from pandas import Series as PDSeries  # <- add this alias


class DataScrubber:
    def __init__(self, df: "pd.DataFrame") -> None:
        self.df = df

    def check_data_consistency_before_cleaning(self) -> dict[str, PDSeries | int]:
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        return {"null_counts": null_counts, "duplicate_count": duplicate_count}

    def check_data_consistency_after_cleaning(self) -> dict[str, PDSeries | int]:
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        if int(null_counts.sum()) != 0:
            raise ValueError("Data still contains null values after cleaning.")
        if int(duplicate_count) != 0:
            raise ValueError("Data still contains duplicate records after cleaning.")
        return {"null_counts": null_counts, "duplicate_count": duplicate_count}

    # ---------- Duplicates ----------
    def remove_duplicates(
        self, subset: Sequence[str] | None = None, keep: str = "first"
    ) -> "pd.DataFrame":
        """Drop duplicate rows (optionally by subset of columns)."""
        self.df = self.df.drop_duplicates(subset=subset, keep=keep)
        return self.df

    # ---------- Missing data ----------
    def handle_missing_data(
        self, drop: bool = False, fill_value: Any | None = None
    ) -> "pd.DataFrame":
        if drop:
            self.df = self.df.dropna()
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
        return self.df

    # ---------- Outliers ----------
    def filter_column_outliers(
        self, column: str, lower_bound: float | int, upper_bound: float | int
    ) -> "pd.DataFrame":
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        mask = (self.df[column] >= lower_bound) & (self.df[column] <= upper_bound)
        self.df = self.df.loc[mask]
        return self.df

    # ---------- String standardization ----------
    def standardize_strings(
        self, columns: Iterable[str], case: str = "lower", trim: bool = True
    ) -> "pd.DataFrame":
        for col in columns:
            if col not in self.df.columns:
                raise ValueError(f"Column '{col}' not found.")
            s = self.df[col].astype("string")
            if trim:
                s = s.str.strip()
            if case == "lower":
                s = s.str.lower()
            elif case == "upper":
                s = s.str.upper()
            elif case == "title":
                s = s.str.title()
            self.df[col] = s
        return self.df

    def format_column_strings_to_lower_and_trim(self, column: str) -> "pd.DataFrame":
        return self.standardize_strings([column], case="lower", trim=True)

    def format_column_strings_to_upper_and_trim(self, column: str) -> "pd.DataFrame":
        return self.standardize_strings([column], case="upper", trim=True)

    # ---------- Rename / Reorder ----------
    def rename_columns(
        self, mapping: Mapping[str, str], *, errors: str = "ignore"
    ) -> "pd.DataFrame":
        """Rename columns using a dict; errors='ignore' keeps unknown keys quiet."""
        self.df = self.df.rename(columns=mapping, errors=errors)
        return self.df

    def reorder_columns(self, order: Sequence[str], *, drop_extras: bool = False) -> "pd.DataFrame":
        """Reorder columns; optionally drop columns not listed."""
        missing = [c for c in order if c not in self.df.columns]
        if missing:
            raise ValueError(f"Missing column(s) for reorder: {', '.join(missing)}")
        if drop_extras:
            self.df = self.df.loc[:, list(order)]
        else:
            rest = [c for c in self.df.columns if c not in order]
            self.df = self.df.loc[:, list(order) + rest]
        return self.df

    # ---------- Date parsing ----------
    def parse_dates(
        self,
        columns: Iterable[str],
        *,
        dayfirst: bool = False,
        errors: str = "coerce",
        infer_datetime_format: bool = True,
    ) -> "pd.DataFrame":
        for col in columns:
            if col not in self.df.columns:
                raise ValueError(f"Column '{col}' not found.")
            self.df[col] = self.df[col].astype("string")
            self.df[col] = self.df[col].str.strip()
            self.df[col] = self.df[col].replace({"": None})
            self.df[col] = self.df[col].pipe(
                lambda s: s if s.isna().all() else s  # avoid pandas warning on empty
            )
            self.df[col] = self.df[col].pipe(
                lambda s: __import__("pandas").to_datetime(
                    s,
                    dayfirst=dayfirst,
                    errors=errors,
                    infer_datetime_format=infer_datetime_format,
                )
            )
        return self.df

    # ---------- Utilities ----------
    def convert_column_to_new_data_type(self, column: str, new_type: type) -> "pd.DataFrame":
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found.")
        self.df[column] = self.df[column].astype(new_type)
        return self.df

    def drop_columns(self, columns: list[str]) -> "pd.DataFrame":
        missing = [c for c in columns if c not in self.df.columns]
        if missing:
            raise ValueError(f"Column(s) not found: {', '.join(missing)}.")
        self.df = self.df.drop(columns=columns)
        return self.df

    def inspect_data(self) -> "pd.DataFrame":
        return self.df
