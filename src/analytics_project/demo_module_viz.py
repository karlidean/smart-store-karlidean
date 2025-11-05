"""Demonstrate data visualization for professional analytics.

This module demonstrates Python's data visualization capabilities using
Seaborn and Matplotlib to create publication-quality charts for
communicating analytical insights.

Module Information:
    - Filename: demo_module_viz.py
    - Module: demo_module_viz
    - Location: src/analytics_project/

Key Concepts:
    - Statistical data visualization with Seaborn
    - Working with built-in datasets
    - Creating publication-quality figures
    - Customizing plots for clarity and impact

Professional Applications:
    - Executive dashboards
    - Research publications
    - Client presentations
    - Exploratory data analysis
"""
#####################################
# Imports At the Top
#####################################

import matplotlib.pyplot as plt
import seaborn as sns

# Import the shared logger
from .utils.logger import init_logger, logger

#####################################
# Define Functions
#####################################


def demo_viz() -> None:
    """Create and display a scatter plot of penguin data."""
    try:
        # Load the Penguins dataset
        data = sns.load_dataset("penguins")
        logger.info("Loaded Penguins dataset successfully.")

        # Create a scatter plot
        sns.scatterplot(data=data, x="bill_length_mm", y="bill_depth_mm", hue="species")
        plt.title("Penguin Bill Dimensions by Species")
        plt.xlabel("Bill Length (mm)")
        plt.ylabel("Bill Depth (mm)")

        logger.info("Displaying chart... Close the window to exit.")
        plt.show()
        logger.info("Chart closed successfully.")

    except Exception as e:
        logger.error(f"Error creating or displaying chart: {e}")


#####################################
# Define main() for standalone testing
#####################################


def main() -> None:
    """Run chart demo locally with its own logger if needed."""
    try:
        init_logger()
    except Exception as exc:
        logger.exception(f"Exception occurred during logger initialization: {exc}")

    demo_viz()


#####################################
# Conditional Execution
# Run if this file is executed directly
#####################################

if __name__ == "__main__":
    main()

#####################################
# List all exports (things that other modules can use)
#####################################

__all__ = ["demo_viz"]
