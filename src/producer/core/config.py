"""
Event Producer Configuration using Pydantic Settings.
"""

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProducerConfig(BaseSettings):
    """Configuration for the event producer application.

    Uses Pydantic Settings for configuration management with environment variable
    support and validation. All settings can be overridden via environment variables
    or a .env file.

    Attributes:
        kafka_bootstrap_servers: Comma-separated list of Kafka broker addresses.
            Use localhost:29092 for host access, kafka:9092 for inter-container.
        init_topic: Topic name for init events (player app opens).
        match_topic: Topic name for match events (completed game matches).
        purchase_topic: Topic name for in-app purchase events.
        kafka_acks: Acknowledgment mode for Kafka sends.
            'all' waits for all replicas, '1' waits for leader, '0' no wait.
        kafka_retries: Number of automatic retry attempts for failed sends.
        kafka_compression_type: Message compression algorithm.
            Options: 'gzip', 'snappy', 'lz4', 'zstd', or None for no compression.
        event_rate_per_second: Target number of events to generate per second.
        batch_size: Number of events to generate and send in each batch.
        init_probability: Relative probability weight for generating init events.
        match_probability: Relative probability weight for generating match events.
        purchase_probability: Relative probability weight for generating purchase events.
        users_pool_size: Number of unique user IDs in the simulation pool.
        schema_dir: Path to directory containing JSON schema files.
            Relative paths are resolved from project root.
        log_level: Logging verbosity level (DEBUG, INFO, WARNING, ERROR, CRITICAL).

    Note:
        Probabilities are automatically normalized, so they represent relative weights
        rather than absolute percentages.
    """

    # Kafka settings
    kafka_bootstrap_servers: str = Field(
        default="localhost:29092", description="Kafka bootstrap servers"
    )
    init_topic: str = Field(
        default="init_events", description="Kafka topic for init events"
    )
    match_topic: str = Field(
        default="match_events", description="Kafka topic for match events"
    )
    purchase_topic: str = Field(
        default="purchase_events", description="Kafka topic for purchase events"
    )
    kafka_acks: str = Field(default="all", description="Kafka acknowledgment mode")
    kafka_retries: int = Field(
        default=3, description="Number of retries for failed sends"
    )
    kafka_compression_type: Optional[str] = Field(
        default=None, description="Compression type for messages"
    )

    event_rate_per_second: int = Field(
        default=100, description="Number of events to generate per second"
    )
    batch_size: int = Field(default=10, description="Batch size for sending events")
    init_probability: float = Field(
        default=0.3, description="Probability of generating an init event"
    )
    match_probability: float = Field(
        default=0.5, description="Probability of generating a match event"
    )
    purchase_probability: float = Field(
        default=0.2, description="Probability of generating a purchase event"
    )

    users_pool_size: int = Field(
        default=1000, description="Size of the user pool for generating events"
    )

    schema_dir: str = Field(
        default="schemas",
        description="Directory containing JSON schemas (relative to project root)",
    )

    log_level: str = Field(default="INFO", description="Logging level")

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False
    )
