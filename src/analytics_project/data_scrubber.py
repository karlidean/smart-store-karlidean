import pandas as pd


class DataScrubber:
    """Reusable cleaning utilities for data preparation scripts."""

    @staticmethod
    def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates()

    @staticmethod
    def fill_missing(df: pd.DataFrame, strategy="mean") -> pd.DataFrame:
        """Fill numeric missing values using a given strategy."""
        if strategy == "mean":
            return df.fillna(df.mean(numeric_only=True))
        elif strategy == "median":
            return df.fillna(df.median(numeric_only=True))
        else:
            return df.fillna(strategy)

    @staticmethod
    def standard_clean(df: pd.DataFrame) -> pd.DataFrame:
        df = DataScrubber.remove_duplicates(df)
        df = DataScrubber.fill_missing(df, strategy="mean")
        return df
