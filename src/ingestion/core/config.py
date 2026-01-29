"""
Bronze Layer Ingestion Configuration.

Generic configuration that accepts topic name as parameter,
allowing multiple ingestion jobs for different topics.
"""


from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BronzeIngestionConfig(BaseSettings):
    """Bronze layer ingestion configuration settings.
    
    Generic configuration that accepts topic name as a parameter, enabling multiple
    ingestion job instances for different Kafka topics. Each instance consumes from
    a specific topic and writes to a shared bronze Delta table with topic-specific
    checkpoints.
    
    The kafka_topic parameter must be set via the KAFKA_TOPIC environment variable
    for each job instance.
    
    Attributes:
        kafka_bootstrap_servers: Kafka broker connection string.
        kafka_topic: Kafka topic to consume from (required via environment variable).
        kafka_starting_offsets: Starting offset strategy for consumption.
        storage_base_path: Base directory for all storage paths.
        storage_bronze_path: Delta Lake path for bronze table (shared across topics).
        checkpoint_bronze: Base checkpoint path (suffixed with topic name per instance).
        streaming_trigger_interval: Micro-batch processing interval.
        streaming_max_offsets_per_trigger: Maximum offsets per micro-batch.
        spark_app_name: Base Spark application name (suffixed with topic name).
        spark_log_level: Logging level for Spark operations.
        log_level: Application logging level.
    """
    
    kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Kafka bootstrap servers"
    )
    kafka_topic: str = Field(
        description="Kafka topic to consume from (REQUIRED - must be set per job instance)"
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
        default="/opt/bitnami/spark/storage/checkpoints/bronze_ingestion",
        description="Checkpoint location for bronze ingestion (will be suffixed with topic name)"
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
        default="bronze-event-ingestion",
        description="Spark application name (will be suffixed with topic name)"
    )
    spark_log_level: str = Field(
        default="WARN",
        description="Spark log level"
    )
    
    log_level: str = Field(
        default="INFO",
        description="Application log level"
    )
    
    def get_checkpoint_path(self) -> str:
        """Get checkpoint path with topic-specific suffix.
        
        Creates a unique checkpoint path for each topic by appending the topic name
        to the base checkpoint path. This enables multiple ingestion instances to
        maintain independent streaming state.
        
        Returns:
            Checkpoint path string with topic suffix (e.g., '.../bronze_ingestion_init-events').
        """
        topic_suffix = str(self.kafka_topic).replace("_", "-")
        return f"{self.checkpoint_bronze}_{topic_suffix}"
    
    def get_app_name(self) -> str:
        """Get Spark application name with topic suffix.
        
        Creates a descriptive application name by appending the topic name to the
        base app name, making it easier to identify and monitor specific ingestion
        jobs in Spark UI.
        
        Returns:
            Application name string with topic suffix (e.g., 'bronze-event-ingestion-init_events').
        """
        return f"{self.spark_app_name}-{self.kafka_topic}"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
