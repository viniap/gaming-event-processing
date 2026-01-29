"""Builder pattern for GameEventProducer construction.

Provides a fluent API for constructing GameEventProducer instances with
clear, readable configuration. Supports reading from ProducerConfig or
building with custom parameters.
"""

from typing import Dict, Optional

from src.producer.core.config import ProducerConfig
from src.producer.kafka.producer import GameEventProducer


class GameEventProducerBuilder:
    """Fluent builder for GameEventProducer instances.

    Implements the Builder pattern to provide a clean, readable API for
    constructing Kafka producers with complex configurations. Supports
    reading from ProducerConfig or custom programmatic configuration.

    Example:
        >>> # From config
        >>> config = ProducerConfig()
        >>> producer = GameEventProducerBuilder.from_config(config).build()

        >>> # Custom configuration
        >>> producer = (
        ...     GameEventProducerBuilder()
        ...     .with_bootstrap_servers("localhost:29092")
        ...     .with_topic_mapping({"init": "init_events"})
        ...     .with_acks("all")
        ...     .build()
        ... )

        >>> # Mixed: config with overrides
        >>> producer = (
        ...     GameEventProducerBuilder.from_config(config)
        ...     .with_retries(5)
        ...     .build()
        ... )
    """

    def __init__(self):
        """Initialize builder with default values."""
        self._bootstrap_servers: Optional[str] = None
        self._topic_mapping: Optional[Dict[str, str]] = None
        self._acks: str = "all"
        self._retries: int = 3
        self._compression_type: Optional[str] = "snappy"

    @classmethod
    def from_config(cls, config: ProducerConfig) -> "GameEventProducerBuilder":
        """Create builder initialized from ProducerConfig.

        Reads Kafka-related configuration from ProducerConfig and initializes
        the builder with those values. The topic mapping is constructed from
        the individual topic configurations.

        Args:
            config: ProducerConfig instance to read settings from.

        Returns:
            Builder instance initialized with config values.

        Example:
            >>> config = ProducerConfig()
            >>> builder = GameEventProducerBuilder.from_config(config)
            >>> producer = builder.build()
        """
        topic_mapping = {
            "init": config.init_topic,
            "match": config.match_topic,
            "in-app-purchase": config.purchase_topic,
        }

        return (
            cls()
            .with_bootstrap_servers(config.kafka_bootstrap_servers)
            .with_topic_mapping(topic_mapping)
            .with_acks(config.kafka_acks)
            .with_retries(config.kafka_retries)
            .with_compression(config.kafka_compression_type)
        )

    def with_bootstrap_servers(self, servers: str) -> "GameEventProducerBuilder":
        """Set Kafka bootstrap servers.

        Args:
            servers: Comma-separated list of Kafka broker addresses.
                Format: "host1:port1,host2:port2"

        Returns:
            Self for method chaining.

        Example:
            >>> builder.with_bootstrap_servers("localhost:29092")
        """
        self._bootstrap_servers = servers
        return self

    def with_topic_mapping(
        self, mapping: Dict[str, str]
    ) -> "GameEventProducerBuilder":
        """Set event type to topic mapping.

        Args:
            mapping: Dictionary mapping event types to Kafka topic names.
                Must include keys: "init", "match", "in-app-purchase"

        Returns:
            Self for method chaining.

        Example:
            >>> builder.with_topic_mapping({
            ...     "init": "init_events",
            ...     "match": "match_events",
            ...     "in-app-purchase": "purchase_events"
            ... })
        """
        self._topic_mapping = mapping
        return self

    def with_acks(self, acks: str) -> "GameEventProducerBuilder":
        """Set Kafka acknowledgment mode.

        Args:
            acks: Acknowledgment mode:
                - "all": Wait for all in-sync replicas (highest durability)
                - "1": Wait for leader only (balanced)
                - "0": No acknowledgment (lowest latency)

        Returns:
            Self for method chaining.

        Example:
            >>> builder.with_acks("all")
        """
        self._acks = acks
        return self

    def with_retries(self, retries: int) -> "GameEventProducerBuilder":
        """Set number of automatic retry attempts.

        Args:
            retries: Number of retry attempts for failed sends.
                Must be >= 0.

        Returns:
            Self for method chaining.

        Example:
            >>> builder.with_retries(5)
        """
        self._retries = retries
        return self

    def with_compression(
        self, compression_type: Optional[str]
    ) -> "GameEventProducerBuilder":
        """Set message compression algorithm.

        Args:
            compression_type: Compression algorithm to use.
                Options: "gzip", "snappy", "lz4", "zstd", or None.
                "snappy" offers good balance of speed and compression.

        Returns:
            Self for method chaining.

        Example:
            >>> builder.with_compression("snappy")
            >>> builder.with_compression(None)  # No compression
        """
        self._compression_type = compression_type
        return self

    def build(self) -> GameEventProducer:
        """Build and return the configured GameEventProducer.

        Validates required configuration and constructs the producer instance.

        Returns:
            Configured GameEventProducer instance.

        Raises:
            ValueError: If required configuration is missing.

        Example:
            >>> producer = builder.build()
        """
        if not self._bootstrap_servers:
            raise ValueError(
                "bootstrap_servers is required. "
                "Use with_bootstrap_servers() to set it."
            )

        if not self._topic_mapping:
            raise ValueError(
                "topic_mapping is required. "
                "Use with_topic_mapping() to set it."
            )

        required_topics = {"init", "match", "in-app-purchase"}
        missing_topics = required_topics - set(self._topic_mapping.keys())
        if missing_topics:
            raise ValueError(
                f"topic_mapping must include all event types. "
                f"Missing: {missing_topics}"
            )

        return GameEventProducer(
            bootstrap_servers=self._bootstrap_servers,
            topic_mapping=self._topic_mapping,
            acks=self._acks,
            retries=self._retries,
            compression_type=self._compression_type,
        )
