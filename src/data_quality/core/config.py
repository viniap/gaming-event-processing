"""
Data Quality Configuration.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DataQualityConfig(BaseSettings):
    """Data quality transformation configuration settings.
    
    Configures storage paths, checkpoint locations, Kafka settings, and transformation rules
    for the silver layer data quality pipeline. Settings can be overridden via environment
    variables (case-insensitive) or .env file.
    
    Attributes:
        storage_bronze_path: Delta Lake path for bronze layer multiplex table.
        storage_silver_init_path: Delta Lake path for silver init events.
        storage_silver_match_path: Delta Lake path for silver match events.
        storage_silver_purchase_path: Delta Lake path for silver purchase events.
        checkpoint_silver_init: Checkpoint location for init stream state.
        checkpoint_silver_match: Checkpoint location for match stream state.
        checkpoint_silver_purchase: Checkpoint location for purchase stream state.
        kafka_bootstrap_servers: Kafka broker connection string.
        kafka_silver_topic: Topic name for publishing silver events.
        enable_kafka_output: Flag to enable/disable Kafka publishing.
        rules_dir: Directory containing transformation rule YAML files.
        event_configs_path: Path to event type configuration YAML.
        streaming_trigger_interval: Micro-batch processing interval.
        spark_app_name: Name identifier for the Spark application.
        spark_log_level: Logging level for Spark operations.
        log_level: Application logging level.
    """
    
    storage_bronze_path: str = Field(
        default="/opt/bitnami/spark/storage/bronze/events",
        description="Bronze table path"
    )
    storage_silver_init_path: str = Field(
        default="/opt/bitnami/spark/storage/silver/init",
        description="Silver init table path"
    )
    storage_silver_match_path: str = Field(
        default="/opt/bitnami/spark/storage/silver/match",
        description="Silver match table path"
    )
    storage_silver_purchase_path: str = Field(
        default="/opt/bitnami/spark/storage/silver/purchase",
        description="Silver purchase table path"
    )
    
    checkpoint_silver_init: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/silver_init",
        description="Silver init checkpoint"
    )
    checkpoint_silver_match: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/silver_match",
        description="Silver match checkpoint"
    )
    checkpoint_silver_purchase: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/silver_purchase",
        description="Silver purchase checkpoint"
    )
    
    kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Kafka bootstrap servers"
    )
    kafka_silver_topic: str = Field(
        default="silver_events",
        description="Kafka topic for silver events"
    )
    enable_kafka_output: bool = Field(
        default=True,
        description="Whether to write to Kafka"
    )
    
    rules_dir: str = Field(
        default="/opt/bitnami/spark/jobs/src/data_quality/config/rules",
        description="Directory containing transformation rules"
    )
    event_configs_path: str = Field(
        default="/opt/bitnami/spark/jobs/src/data_quality/config/event_configs.yml",
        description="Path to event type configurations YAML file"
    )
    
    streaming_trigger_interval: str = Field(
        default="10 seconds",
        description="Trigger interval for micro-batches"
    )
    
    spark_app_name: str = Field(
        default="silver-data-quality",
        description="Spark application name"
    )
    spark_log_level: str = Field(
        default="WARN",
        description="Spark log level"
    )
    
    log_level: str = Field(
        default="INFO",
        description="Application log level"
    )
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
