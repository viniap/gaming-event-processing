"""
Structured logging utilities.

Provides JSON-formatted logging for all components with support
for both structured (JSON) and text output formats.
"""

import logging
import sys
from typing import Optional

from pythonjsonlogger import jsonlogger


class StructuredLogger:
    """Factory for creating structured loggers."""

    @staticmethod
    def get_logger(
        name: str, level: str = "INFO", log_format: str = "json"
    ) -> logging.Logger:
        """
        Create a structured logger.

        Args:
            name: Logger name (typically __name__)
            level: Log level (DEBUG, INFO, WARN, ERROR, CRITICAL)
            log_format: Output format ('json' or 'text')

        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))

        # Avoid adding duplicate handlers
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)

            if log_format.lower() == "json":
                formatter = jsonlogger.JsonFormatter(
                    "%(asctime)s %(name)s %(levelname)s %(message)s", timestamp=True
                )
            else:
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )

            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger


def log_exception(
    logger: logging.Logger, exception: Exception, context: Optional[dict] = None
):
    """
    Log an exception with optional context.

    Args:
        logger: Logger instance
        exception: Exception to log
        context: Optional context dictionary
    """
    extra = context or {}
    extra["exception_type"] = type(exception).__name__
    extra["exception_message"] = str(exception)

    logger.error(
        f"Exception occurred: {type(exception).__name__}", extra=extra, exc_info=True
    )
