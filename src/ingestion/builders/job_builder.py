"""Builder pattern for BronzeIngestionJob construction.

Provides a fluent API for constructing BronzeIngestionJob instances with
clear, readable configuration. Supports reading from BronzeIngestionConfig or
building with custom parameters.
"""

from typing import Optional

from src.ingestion.core.config import BronzeIngestionConfig
from src.ingestion.core.base import ConfigurableBronzeIngestion
from src.common.logger import StructuredLogger


logger = StructuredLogger.get_logger(__name__)


class BronzeIngestionJobBuilder:
    """Fluent builder for constructing BronzeIngestionJob instances.

    Implements the Builder pattern to provide a clean, readable, and flexible API for
    constructing bronze ingestion jobs with complex configurations. Supports multiple
    construction patterns:
    
    - Configuration-based: Build from BronzeIngestionConfig loaded from environment
    - Programmatic: Build with custom parameters using fluent methods
    - Hybrid: Start from config and override specific parameters

    The builder validates required parameters before construction and provides sensible
    defaults for optional parameters.

    Attributes:
        _kafka_bootstrap_servers: Kafka broker connection string.
        _kafka_topic: Kafka topic to consume from.
        _checkpoint_location: Path for streaming checkpoints.
        _bronze_table_path: Delta Lake path for bronze table.
        _trigger_interval: Micro-batch processing interval.
        _app_name: Spark application name.

    Example:
        Build from configuration:
        >>> config = BronzeIngestionConfig()
        >>> job = BronzeIngestionJobBuilder.from_config(config).build()

        Build with custom parameters:
        >>> job = (
        ...     BronzeIngestionJobBuilder()
        ...     .with_kafka("kafka:9092", "init_events")
        ...     .with_storage("/bronze/path", "/checkpoint/path")
        ...     .with_trigger("10 seconds")
        ...     .build()
        ... )

        Mixed approach with overrides:
        >>> job = (
        ...     BronzeIngestionJobBuilder.from_config(config)
        ...     .with_trigger("5 seconds")
        ...     .build()
        ... )
    """

    def __init__(self):
        """Initialize builder with None values for required parameters and defaults for optional ones."""
        self._kafka_bootstrap_servers: Optional[str] = None
        self._kafka_topic: Optional[str] = None
        self._checkpoint_location: Optional[str] = None
        self._bronze_table_path: Optional[str] = None
        self._trigger_interval: str = "10 seconds"
        self._app_name: Optional[str] = None

    @classmethod
    def from_config(cls, config: BronzeIngestionConfig) -> 'BronzeIngestionJobBuilder':
        """Create a builder initialized from BronzeIngestionConfig.

        This is the recommended construction method when configuration comes from
        environment variables, .env files, or other external configuration sources.
        The builder is pre-populated with all config values and can still be further
        customized with fluent methods before calling build().

        Args:
            config: BronzeIngestionConfig instance with validated settings loaded
                from environment or configuration files.

        Returns:
            Configured BronzeIngestionJobBuilder ready for optional customization
            or immediate building.

        Example:
            >>> config = BronzeIngestionConfig()
            >>> job = BronzeIngestionJobBuilder.from_config(config).build()
        """
        builder = cls()
        builder._kafka_bootstrap_servers = config.kafka_bootstrap_servers
        builder._kafka_topic = config.kafka_topic
        builder._checkpoint_location = config.get_checkpoint_path()
        builder._bronze_table_path = config.storage_bronze_path
        builder._trigger_interval = config.streaming_trigger_interval
        builder._app_name = config.get_app_name()

        logger.debug(
            "Builder initialized from config",
            extra={
                "kafka_topic": config.kafka_topic,
                "bronze_path": config.storage_bronze_path,
                "checkpoint_path": config.get_checkpoint_path()
            }
        )

        return builder

    def with_kafka(
        self,
        bootstrap_servers: str,
        topic: str
    ) -> 'BronzeIngestionJobBuilder':
        """Configure Kafka connection settings.

        Args:
            bootstrap_servers: Kafka broker addresses in host:port format, multiple
                brokers separated by comma (e.g., 'kafka:9092' or 'broker1:9092,broker2:9092').
            topic: Kafka topic name to consume events from.

        Returns:
            Self for method chaining.

        Example:
            >>> builder.with_kafka("kafka:9092", "init_events")
        """
        self._kafka_bootstrap_servers = bootstrap_servers
        self._kafka_topic = topic
        return self

    def with_storage(
        self,
        bronze_table_path: str,
        checkpoint_location: str
    ) -> 'BronzeIngestionJobBuilder':
        """Configure storage paths for Delta Lake and checkpoints.

        Args:
            bronze_table_path: Absolute path to write bronze Delta table. Can be local
                filesystem path or cloud storage path (e.g., s3://, abfss://).
            checkpoint_location: Absolute path for storing streaming checkpoint data.
                Must be reliable storage for fault tolerance.

        Returns:
            Self for method chaining.

        Example:
            >>> builder.with_storage(
            ...     "/opt/bitnami/spark/storage/bronze/events",
            ...     "/opt/bitnami/spark/storage/checkpoints/bronze_init"
            ... )
        """
        self._bronze_table_path = bronze_table_path
        self._checkpoint_location = checkpoint_location
        return self

    def with_trigger(self, interval: str) -> 'BronzeIngestionJobBuilder':
        """Configure streaming trigger interval for micro-batch processing.

        Args:
            interval: Trigger interval in Spark duration format (e.g., '10 seconds',
                '1 minute', '500 milliseconds'). Controls how frequently micro-batches
                are processed.

        Returns:
            Self for method chaining.

        Example:
            >>> builder.with_trigger("5 seconds")
        """
        self._trigger_interval = interval
        return self

    def with_app_name(self, app_name: str) -> 'BronzeIngestionJobBuilder':
        """Configure Spark application name.

        Args:
            app_name: Spark application name used for identification in Spark UI,
                logs, and cluster management interfaces.

        Returns:
            Self for method chaining.

        Example:
            >>> builder.with_app_name("bronze-ingestion-init")
        """
        self._app_name = app_name
        return self

    def build(self) -> ConfigurableBronzeIngestion:
        """Build the ConfigurableBronzeIngestion instance.

        Validates that all required parameters are set and constructs the ingestion job.
        Provides a default application name if not explicitly configured.

        Returns:
            Configured ConfigurableBronzeIngestion instance ready to run.

        Raises:
            ValueError: If any required parameter (kafka_bootstrap_servers, kafka_topic,
                bronze_table_path, checkpoint_location) is missing.

        Example:
            >>> job = builder.build()
            >>> job.run()
        """
        if not self._kafka_bootstrap_servers:
            raise ValueError("Kafka bootstrap servers must be configured")
        if not self._kafka_topic:
            raise ValueError("Kafka topic must be configured")
        if not self._bronze_table_path:
            raise ValueError("Bronze table path must be configured")
        if not self._checkpoint_location:
            raise ValueError("Checkpoint location must be configured")

        if not self._app_name:
            self._app_name = f"bronze-ingestion-{self._kafka_topic}"

        logger.info(
            "Building bronze ingestion job",
            extra={
                "kafka_topic": self._kafka_topic,
                "bronze_path": self._bronze_table_path,
                "trigger_interval": self._trigger_interval,
                "app_name": self._app_name
            }
        )

        return ConfigurableBronzeIngestion(
            kafka_bootstrap_servers=self._kafka_bootstrap_servers,
            kafka_topic=self._kafka_topic,
            checkpoint_location=self._checkpoint_location,
            bronze_table_path=self._bronze_table_path,
            trigger_interval=self._trigger_interval,
            app_name=self._app_name
        )
