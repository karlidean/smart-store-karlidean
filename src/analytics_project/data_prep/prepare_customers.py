"""
scripts/analytics_project/data_preparation/prepare_customers.py

This script reads customer data from the data/raw folder, cleans the data,
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
import pathlib
import sys

# Import from external packages (requires a virtual environment)
import pandas as pd

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import local modules (e.g. utils/logger.py)
from analytics_project.utils.logger import logger


# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = (
    pathlib.Path(__file__).resolve().parent
)  # Directory of the current script
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
PROJECT_ROOT: pathlib.Path = SCRIPTS_DIR.parent
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"  # place to store prepared data

print(f'Raw Data is stored here: {RAW_DATA_DIR}')


# Ensure the directories exist
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Functions and Instructions
#####################################


def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    try:
        logger.info(f"READING: {file_path}.")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Return an empty DataFrame if the file is not found
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if any other error occurs


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    ''' Saves the data to a CSV '''
    logger.info(
        f"SAVING FUNCTION START: Saving prepared data with filename {file_name}, the dataframe's shape is {df.shape}"
    )
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data was successfully saved to {file_path}")


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"DUPLICATE REMOVAL FUNCTION START: removing duplicates with dataframe shape={df.shape}")

    # Perform deduplication
    df_deduped = df.drop_duplicates()

    # Log shapes
    logger.info(f"Deduplication function successful. Deduped dataframe shape: {df_deduped.shape}")
    print(f"Deduplication function successful. Deduped dataframe shape: {df_deduped.shape}")

    return df_deduped


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"MISSING VALUE FUNCTION START: handle_missing_values with dataframe shape={df.shape}")
    print(f"MISSING VALUE FUNCTION START: handle_missing_values with dataframe shape={df.shape}")
    logger.info(f'MISSING VALUE STATUS: {df.isnull()}')
    print(f'MISSING VALUE STATUS: {df.isnull()}')

    # Log missing values count before handling
    missing_before = df.isna().sum().sum()
    logger.info(f"Total missing values before handling: {missing_before}")
    print(f"Total missing values before handling: {missing_before}")

    if 'MemberPoints' in df.columns:
        median_points = df['MemberPoints'].median()
        df.loc[:, 'MemberPoints'] = df['MemberPoints'].fillna(median_points)
        
    if 'MemberStatus' in df.columns:
        mode_style = df['MemberStatus'].mode()
        df.loc[:, 'MemberStatus'] = df['MemberStatus'].fillna(mode_style)
        
    if 'Preferred Contact' in df.columns:
        mode_style = df['Preferred Contact'].mode()
        df.loc[:, 'Preferred Contact'] = df['Preferred Contact'].fillna(mode_style) 
        
    missing_after = df.isna().sum().sum()
    logger.info(f"Total missing values after handling: {missing_after}")
    print(f"Total missing values after handling: {missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    print(f"{len(df)} records remaining after handling missing values.")
    print(f'MISSING VALUE FUNCTION STATUS: COMPLETE')
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers based on thresholds.
    This logic is very specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)

    # TODO: Define numeric columns and apply rules for outlier removal
    # Example:
    # df = df[(df['Age'] > 18) & (df['Age'] < 100)]

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


#####################################
# Define Main Function - The main entry point of the script
#####################################


def main() -> None:
    """
    Main function for processing customer data.
    """
    logger.info("====== INITIALIZING CLEANING OF CUSTOMER FILE ======")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "customers_data.csv"
    output_file = "customers_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()

    # Log if any column names changed
    changed_columns = [
        f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    df = remove_duplicates(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Cleaned shape:  {original_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_customers_data.py")
    logger.info("==================================")


#####################################
# Conditional Execution Block
# Ensures the script runs only when executed directly
# This is a common Python convention.
#####################################

if __name__ == "__main__":
    main()
