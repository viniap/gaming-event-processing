"""Event Producer Application.

Main application class that orchestrates event generation, validation,
and publishing to Kafka.
"""

import time
from typing import Optional

from src.common.logger import StructuredLogger, log_exception
from src.producer.core.config import ProducerConfig
from src.producer.generation.event_generator import EventGenerator
from src.producer.kafka.producer_builder import GameEventProducerBuilder
from src.producer.validation.schema_validator import SchemaLoader

logger = StructuredLogger.get_logger(__name__)


class EventProducerApp:
    """Main application orchestrating event generation and publishing.

    Coordinates all producer components to continuously generate, validate,
    and publish gaming events to Kafka at a configurable rate. Handles
    graceful shutdown and comprehensive statistics tracking.

    The application runs in a continuous loop:
    1. Generate a batch of events
    2. Validate each event against its JSON schema
    3. Publish valid events to appropriate Kafka topics
    4. Log statistics periodically
    5. Sleep to maintain target event rate

    Attributes:
        config: Producer configuration instance.
        generator: Event generator for creating realistic events.
        schema_loader: Schema validator for event validation.
        producer: Kafka producer for publishing to topics.

    Example:
        >>> config = ProducerConfig(event_rate_per_second=100)
        >>> app = EventProducerApp(config)
        >>> app.run()  # Runs until interrupted
    """

    def __init__(self, config: Optional[ProducerConfig] = None):
        """Initialize the event producer application with all components.

        Creates and configures all required components:
        - Event generator with user pool
        - Schema loader with validation schemas
        - Kafka producer with topic routing

        Args:
            config: Optional configuration instance. If not provided,
                creates a new ProducerConfig() which loads from environment
                variables and .env file.

        Raises:
            KafkaError: If Kafka connection fails.
            FileNotFoundError: If schema files are not found.

        Note:
            Topic mapping is created from config:
            - init → config.init_topic
            - match → config.match_topic
            - in-app-purchase → config.purchase_topic
        """
        self.config = config or ProducerConfig()
        self.generator = EventGenerator(self.config.users_pool_size)
        self.schema_loader = SchemaLoader(self.config.schema_dir)

        self.producer = GameEventProducerBuilder.from_config(self.config).build()

        logger.info(
            "Event producer application initialized",
            extra={
                "event_rate": self.config.event_rate_per_second,
                "batch_size": self.config.batch_size,
                "users_pool": self.config.users_pool_size,
                "topics": {
                    "init": self.config.init_topic,
                    "match": self.config.match_topic,
                    "purchase": self.config.purchase_topic,
                },
            },
        )

    def run(self, shutdown_flag=None):
        """Execute the main event generation and publishing loop.

        Continuously generates event batches at the configured rate, validates them,
        and publishes to Kafka. The loop continues until a shutdown signal is received
        (SIGINT/SIGTERM) or an unhandled exception occurs.

        The loop performs these steps each iteration:
        1. Generate a batch of events based on configured probabilities
        2. Validate each event against its JSON schema
        3. Publish valid events to Kafka (routed by event type)
        4. Log statistics every 1000 events
        5. Sleep to maintain the target event rate

        Args:
            shutdown_flag: Optional callable that returns True when shutdown is requested.
                If not provided, runs indefinitely until exception.

        Raises:
            KeyboardInterrupt: Caught and handled gracefully.
            Exception: Any other exception is logged and triggers shutdown.

        Note:
            The event rate is maintained by calculating batch timing:
            sleep_time = batch_size / events_per_second

            Statistics tracked:
            - Total events generated
            - Valid events (passed schema validation)
            - Invalid events (failed schema validation)
            - Kafka success/error counts

        Example:
            >>> app = EventProducerApp()
            >>> app.run()  # Press Ctrl+C to stop gracefully
        """
        logger.info("Starting event producer")

        events_per_batch = self.config.batch_size
        batches_per_second = self.config.event_rate_per_second / events_per_batch
        sleep_time = 1.0 / batches_per_second if batches_per_second > 0 else 1.0

        total_events = 0
        valid_events = 0
        invalid_events = 0

        def should_shutdown():
            return shutdown_flag() if shutdown_flag else False

        try:
            while not should_shutdown():
                batch_start = time.time()

                events = self.generator.generate_events_batch(
                    count=events_per_batch,
                    init_prob=self.config.init_probability,
                    match_prob=self.config.match_probability,
                    purchase_prob=self.config.purchase_probability,
                )

                validated_events = []
                for event in events:
                    if self.schema_loader.validate_event(event):
                        validated_events.append(event)
                        valid_events += 1
                    else:
                        invalid_events += 1
                        logger.warning(
                            "Invalid event generated", extra={"event": event}
                        )

                if validated_events:
                    published = self.producer.publish_batch(validated_events)
                    total_events += published

                if total_events > 0 and total_events % 1000 == 0:
                    stats = self.producer.get_stats()
                    logger.info(
                        "Producer statistics",
                        extra={
                            "total_generated": total_events,
                            "valid_events": valid_events,
                            "invalid_events": invalid_events,
                            "kafka_success": stats["success_count"],
                            "kafka_errors": stats["error_count"],
                        },
                    )

                elapsed = time.time() - batch_start
                if elapsed < sleep_time:
                    time.sleep(sleep_time - elapsed)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            log_exception(logger, exc)
        finally:
            self._shutdown(total_events, valid_events, invalid_events)

    def _shutdown(self, total_events: int, valid_events: int, invalid_events: int):
        """Perform graceful shutdown of the application.

        Ensures all pending messages are delivered and logs final statistics
        before terminating. This method is called automatically when the run
        loop exits.

        Args:
            total_events: Total number of events generated during the session.
            valid_events: Number of events that passed schema validation.
            invalid_events: Number of events that failed schema validation.

        Note:
            Shutdown sequence:
            1. Flush pending Kafka messages
            2. Close Kafka producer connection
            3. Log final comprehensive statistics

            The method ensures no messages are lost during shutdown.
        """
        logger.info("Shutting down event producer")

        self.producer.flush()
        self.producer.close()

        stats = self.producer.get_stats()
        logger.info(
            "Final statistics",
            extra={
                "total_generated": total_events,
                "valid_events": valid_events,
                "invalid_events": invalid_events,
                "kafka_success": stats["success_count"],
                "kafka_errors": stats["error_count"],
            },
        )

        logger.info("Event producer shut down successfully")
