"""Entry point for professional analytics project execution.

This module serves as the orchestrator, demonstrating how professional
Python projects integrate multiple modules into a cohesive application.

Module Information:
    - Filename: main.py
    - Module: main
    - Location: src/analytics_project/

Key Concepts:
    - Module orchestration and integration
    - Sequential workflow execution
    - Error handling at the application level
    - Project structure best practices

Professional Applications:
    - ETL pipeline coordination
    - Automated reporting workflows
    - Batch processing systems
    - Scheduled analytics jobs
"""

# Import from local project modules
from .demo_module_basics import demo_basics
from .demo_module_languages import demo_greetings
from .demo_module_stats import demo_stats
from .demo_module_viz import demo_viz
from .utils.logger import init_logger, logger


def main() -> int:
    """Demonstrate a complete Python project structure.

    This function coordinates multiple demo modules to illustrate
    how professional Python projects integrate and run as a pipeline.

    Returns:
        int: Exit status code (0 for success, 1 for failure)
             — standard practice in professional Python projects.
    """
    # Initialize logging for consistent console + file output
    init_logger()
    logger.info("Starting main analytics pipeline.")

    try:
        # Sequentially run each module to simulate an ETL-like process
        demo_basics()  # Basic operations and data handling
        demo_stats()  # Compute and log statistical metrics
        demo_viz()  # Generate example visualizations
        demo_greetings()  # Simple language demo (text output)

        logger.info("Demo pipeline complete.")
        return 0  # Returning 0 indicates success in professional scripts

    except Exception as e:
        # Application-level exception handling (never let a crash go silent)
        logger.error(f"Error executing demo pipeline: {e}")
        return 1  # Non-zero exit codes signal an error to the OS or CI system


# ---------------- Entry Point ----------------
# This ensures the code below runs ONLY when executed directly,
# not when imported as a module (a key Python convention).
if __name__ == "__main__":
    # Raise SystemExit so that the OS / CI runner receives the exit code.
    # Professionals use this pattern to make scripts automation-safe.
    raise SystemExit(main())


# List public exports (best practice for packages)
__all__ = ["main"]
