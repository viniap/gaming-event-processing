"""Event Producer Main Entry Point.

Provides the CLI entry point for the event producer application with
signal handling for graceful shutdown.
"""

import signal
import sys

from src.common.logger import StructuredLogger
from src.producer.core.app import EventProducerApp
from src.producer.core.config import ProducerConfig

SHUTDOWN_REQUESTED = False


def signal_handler(_signum, _frame):
    """Handle OS shutdown signals for graceful termination.

    Sets the global shutdown flag when SIGINT (Ctrl+C) or SIGTERM signals
    are received. This allows the main loop to complete its current batch
    and perform cleanup before exiting.

    Args:
        _signum: Signal number received (SIGINT=2, SIGTERM=15).
        _frame: Current stack frame (unused).

    Note:
        This handler is registered for both SIGINT and SIGTERM in main().
    """
    global SHUTDOWN_REQUESTED  # pylint: disable=global-statement
    logger = StructuredLogger.get_logger(__name__)
    logger.info("Shutdown signal received")
    SHUTDOWN_REQUESTED = True


def main():
    """Main entry point for the event producer application.

    Sets up signal handlers for graceful shutdown, loads configuration,
    initializes the logger, creates the application instance, and starts
    the event generation loop.

    The application can be stopped gracefully by:
    - Pressing Ctrl+C (SIGINT)
    - Sending SIGTERM signal

    Environment Variables:
        All ProducerConfig fields can be overridden via environment variables.
        See ProducerConfig class for available options.

    Exit Codes:
        0: Normal termination (graceful shutdown)

    Example:
        Run from command line:
        $ python -m src.producer.main

        Or with custom config:
        $ EVENT_RATE_PER_SECOND=200 python -m src.producer.main
    """
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    config = ProducerConfig()

    StructuredLogger.get_logger(__name__, level=config.log_level)

    app = EventProducerApp(config)
    app.run(shutdown_flag=lambda: SHUTDOWN_REQUESTED)

    sys.exit(0)


if __name__ == "__main__":
    main()
