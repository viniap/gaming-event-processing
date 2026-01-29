"""
Bronze Layer Ingestion Configuration.

Generic configuration that accepts topic name as parameter,
allowing multiple ingestion jobs for different topics.
"""


from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BronzeIngestionConfig(BaseSettings):
    """Bronze layer ingestion configuration settings.
    
    Configuration for unified bronze ingestion that subscribes to multiple Kafka topics
    simultaneously. This prevents concurrent write conflicts that occur when multiple
    separate Spark streaming jobs try to write to the same Delta table.
    
    All configured topics are consumed in a single streaming job, which ensures proper
    Delta Lake transaction coordination and prevents checksum errors in the transaction log.
    
    Attributes:
        kafka_bootstrap_servers: Kafka broker connection string.
        kafka_topics: Comma-separated list of Kafka topics to consume from.
        kafka_topic: Legacy single topic support (deprecated, use kafka_topics).
        kafka_starting_offsets: Starting offset strategy for consumption.
        storage_base_path: Base directory for all storage paths.
        storage_bronze_path: Delta Lake path for bronze table (shared across topics).
        checkpoint_bronze: Checkpoint path for the unified ingestion job.
        streaming_trigger_interval: Micro-batch processing interval.
        streaming_max_offsets_per_trigger: Maximum offsets per micro-batch.
        spark_app_name: Spark application name for the unified ingestion job.
        spark_log_level: Logging level for Spark operations.
        log_level: Application logging level.
    """
    
    kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Kafka bootstrap servers"
    )
    kafka_topics: str = Field(
        default="init_events,match_events,purchase_events",
        description="Comma-separated list of Kafka topics to consume from"
    )
    kafka_topic: str = Field(
        default="",
        description="Legacy single topic support (deprecated, use kafka_topics)"
    )
    kafka_starting_offsets: str = Field(
        default="latest",
        description="Starting offset for Kafka consumption"
    )
    
    storage_base_path: str = Field(
        default="/opt/bitnami/spark/storage",
        description="Base storage path"
    )
    storage_bronze_path: str = Field(
        default="/opt/bitnami/spark/storage/bronze/events",
        description="Bronze table path"
    )
    
    checkpoint_bronze: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/bronze_ingestion_unified",
        description="Checkpoint location for unified bronze ingestion"
    )
    
    streaming_trigger_interval: str = Field(
        default="10 seconds",
        description="Trigger interval for micro-batches"
    )
    streaming_max_offsets_per_trigger: int = Field(
        default=10000,
        description="Maximum offsets to process per trigger"
    )
    
    spark_app_name: str = Field(
        default="bronze-event-ingestion-unified",
        description="Spark application name for unified ingestion"
    )
    spark_log_level: str = Field(
        default="WARN",
        description="Spark log level"
    )
    
    log_level: str = Field(
        default="INFO",
        description="Application log level"
    )
    
    def get_topics_list(self) -> list[str]:
        """Get list of Kafka topics to subscribe to.
        
        Returns topics from kafka_topics (comma-separated) or falls back to
        kafka_topic for backward compatibility.
        
        Returns:
            List of Kafka topic names.
        """
        # Support legacy single topic configuration
        if self.kafka_topic and not self.kafka_topics:
            return [self.kafka_topic]
        
        # Parse comma-separated topics list
        return [topic.strip() for topic in self.kafka_topics.split(",") if topic.strip()]
    
    def get_checkpoint_path(self) -> str:
        """Get checkpoint path for unified ingestion.
        
        Returns the unified checkpoint path for the single ingestion job
        that subscribes to multiple topics.
        
        Returns:
            Checkpoint path string for unified ingestion.
        """
        return f"{self.checkpoint_bronze}_unified"
    
    def get_app_name(self) -> str:
        """Get Spark application name for unified ingestion.
        
        Returns a descriptive application name for the unified ingestion job
        that processes multiple Kafka topics.
        
        Returns:
            Application name string for unified ingestion.
        """
        return f"{self.spark_app_name}-unified"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
