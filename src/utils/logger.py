"""
Structured logging utility for the Credit Risk Analytics Platform.

Provides a consistent logger across all modules with structured output
suitable for Databricks log monitoring and debugging.
"""

import logging
import sys


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create a structured logger instance.

    Args:
        name: Logger name (typically __name__ of the calling module).
        level: Logging level (default: INFO).

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(level)
    return logger
