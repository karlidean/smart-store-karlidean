"""Demonstrate international features.

This module showcases Python's strengths for global analytics projects,
including advanced language features and character encoding.

Module Information:
    - Filename: demo_module_languages.py
    - Module: demo_module_languages
    - Location: src/analytics_project/

Professional Applications:
    - Multi-language data processing
    - Accessible analytics dashboards
    - International team collaboration
    - Voice-enabled reporting systems
"""

#####################################
# Imports At the Top
#####################################

# Import the shared logger
from .utils.logger import init_logger, logger

#####################################
# Define Core Functions
#####################################


def demo_greetings() -> None:
    """Greet the user in multiple languages."""
    greetings = [
        "English: Hello! Welcome to Python programming.",
        "Spanish: Hola! Bienvenido a la programación en Python.",
        "Mandarin: 你好! 欢迎学习Python编程.",
        "French:  Bonjour! Bienvenue à la programmation Python.",
        "German:  Hallo! Willkommen bei der Python-Programmierung.",
        "Telugu:  హలో! Python ప్రోగ్రామింగ్‌లోకి స్వాగతం.",
        "Norwegian: Hei! Velkommen til Python-programmering.",
    ]

    # Join all greetings into one multiline string
    all_greetings = "\n".join(greetings)

    logger.info(f"\nGreetings Professional Analyst:\n{all_greetings}")


#####################################
# Define main() for standalone testing
#####################################


def main() -> None:
    """Run the languages demo."""
    try:
        init_logger()
    except Exception as exc:
        logger.exception(f"Exception occurred during logger initialization: {exc}")
    demo_greetings()


#####################################
# Conditional Execution
# Run if this file is executed directly
#####################################

if __name__ == "__main__":
    main()

#####################################
# List all exports (things that other modules can use)
#####################################

__all__ = ["demo_greetings"]
