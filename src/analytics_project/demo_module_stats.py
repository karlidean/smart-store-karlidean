"""Demonstrate statistical calculations for professional analytics.

This module showcases Python's statistical capabilities using both built-in
functions and the statistics library for common data analysis tasks.

Module Information:
    - Filename: demo_module_stats.py
    - Module: demo_module_stats
    - Location: src/analytics_project/

Key Concepts:
    - Type hints for function parameters and returns
    - Statistical functions (min, max, mean, stdev)
    - Formatted output for professional reporting
    - Logging statistical summaries

Professional Applications:
    - Data quality assessment
    - Performance metrics analysis
    - Risk calculations
    - A/B testing results
"""

#####################################
# Imports At the Top
#####################################

from collections.abc import Sequence
import statistics

# Import the shared logger
from .utils.logger import init_logger, logger

#####################################
# Define Functions
#####################################


def calculate_min(scores: Sequence[float]) -> float:
    """Return the minimum value in the list."""
    return min(scores)


def calculate_max(scores: Sequence[float]) -> float:
    """Return the maximum value in the list."""
    return max(scores)


def calculate_mean(scores: Sequence[float]) -> float:
    """Return the mean (average) of the list."""
    return statistics.mean(scores)


def calculate_standard_deviation(scores: Sequence[float]) -> float:
    """Return the standard deviation of the list."""
    return statistics.stdev(scores)


#####################################
# Demo Function (importable by main.py)
#####################################


def demo_stats(scores: Sequence[float] | None = None) -> None:
    """Demonstrate how to calculate and log statistics for a list of numbers.

    Args:
        scores: Optional list or tuple of numeric values.
                If not provided, uses a default list.
    """
    if scores is None:
        scores = [3.5, 4.0, 4.8, 2.9, 3.7, 4.3, 3.8]

    n = len(scores)
    v_min = calculate_min(scores)
    v_max = calculate_max(scores)
    v_mean = calculate_mean(scores)
    v_std = calculate_standard_deviation(scores)

    # One clean block: easy to scan in terminal and log file.
    # Use v_ prefix to indicate "value of" each metric.
    # So we don't conflict with built-in function names like min() and max().
    # Use formatted strings (f-strings) for alignment and decimal places.
    # Notice the f just before the opening quote of the string.
    # That lets us embed variables directly in the string.
    summary = (
        "STATS SUMMARY\n"
        f"Scores (n={n}): {scores}\n"
        f"{'metric':<18}{'value':>10}\n"
        f"{'-' * 28}\n"
        f"{'min':<18}{v_min:>10.2f}\n"
        f"{'max':<18}{v_max:>10.2f}\n"
        f"{'mean':<18}{v_mean:>10.2f}\n"
        f"{'stdev':<18}{v_std:>10.2f}\n"
    )

    # Log the summary in one block - \n means `new line` and leaves a blank line
    logger.info("\n" + summary)


#####################################
# main() (for standalone testing)
#####################################


def main() -> None:
    """Run demo_stats() locally for testing."""
    try:
        init_logger()
    except Exception as exc:
        logger.exception(f"Exception occurred during logger initialization: {exc}")

    demo_stats()


#####################################
# Conditional Execution
# Run if this file is executed directly
#####################################

if __name__ == "__main__":
    main()


#####################################
# List all exports (things that other modules can use)
#####################################

__all__ = [
    "calculate_min",
    "calculate_max",
    "calculate_mean",
    "calculate_standard_deviation",
    "demo_stats",
]
