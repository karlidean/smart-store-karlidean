"""Utilities for cleaning pandas DataFrames.

Provides a reusable `DataScrubber` class with helpers to check consistency,
remove duplicates, handle missing values, filter outliers, rename/reorder
columns, standardize strings, and parse date fields for cross-file reuse.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

# TC002: Only import pandas for typing (this module doesn't use pd at runtime)
if TYPE_CHECKING:
    import pandas as pd


class DataScrubber:
    """Reusable cleaning helpers for a pandas DataFrame."""

    def __init__(self, df: "pd.DataFrame") -> None:
        """Initialize the scrubber with a DataFrame."""
        self.df = df

    def check_data_consistency_before_cleaning(self) -> dict[str, "pd.Series" | int]:
        """Return counts of nulls and duplicate rows before cleaning."""
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        return {"null_counts": null_counts, "duplicate_count": duplicate_count}

    def check_data_consistency_after_cleaning(self) -> dict[str, "pd.Series" | int]:
        """Assert no nulls or duplicates remain and return their counts."""
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        assert null_counts.sum() == 0, "Data still contains null values after cleaning."
        assert duplicate_count == 0, "Data still contains duplicate records after cleaning."
        return {"null_counts": null_counts, "duplicate_count": duplicate_count}

    def convert_column_to_new_data_type(
        self,
        column: str,
        new_type: type,
    ) -> "pd.DataFrame":
        """Convert a column to a new data type."""
        try:
            self.df[column] = self.df[column].astype(new_type)
        except KeyError as exc:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.") from exc
        return self.df

    def drop_columns(self, columns: list[str]) -> "pd.DataFrame":
        """Drop specified columns from the DataFrame."""
        missing = [c for c in columns if c not in self.df.columns]
        if missing:
            raise ValueError(f"Column(s) not found in the DataFrame: {', '.join(missing)}.")
        self.df = self.df.drop(columns=columns)
        return self.df

    def filter_column_outliers(
        self,
        column: str,
        lower_bound: float | int,
        upper_bound: float | int,
    ) -> "pd.DataFrame":
        """Filter rows outside [lower_bound, upper_bound] for a column."""
        if column not in self.df.columns:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        mask = (self.df[column] >= lower_bound) & (self.df[column] <= upper_bound)
        self.df = self.df[mask]
        return self.df

    def format_column_strings_to_lower_and_trim(self, column: str) -> "pd.DataFrame":
        """Lowercase and trim whitespace in a string column."""
        if column not in self.df.columns:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df[column] = self.df[column].astype("string").str.lower().str.strip()
        return self.df

    def format_column_strings_to_upper_and_trim(self, column: str) -> "pd.DataFrame":
        """Uppercase and trim whitespace in a string column."""
        if column not in self.df.columns:
            raise ValueError(f"Column name '{column}' not found in the DataFrame.")
        self.df[column] = self.df[column].astype("string").str.upper().str.strip()
        return self.df

    def handle_missing_data(
        self,
        drop: bool = False,
        fill_value: Any | None = None,
    ) -> "pd.DataFrame":
        """Drop rows with NA or fill NA with a value."""
        if drop:
            self.df = self.df.dropna()
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
        return self.df

    def inspect_data(self) -> "pd.DataFrame":
        """Return DataFrame."""
        return self.df
