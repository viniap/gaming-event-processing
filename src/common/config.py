"""
Configuration management using Pydantic Settings.

This module provides base configuration classes that can be extended by
all components in the system. It supports loading configuration from
environment variables with proper type validation.
"""

from typing import Optional, List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseSettings):
    """Kafka connection configuration."""
    
    bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Kafka bootstrap servers"
    )
    topic: str = Field(
        default="raw_events",
        description="Kafka topic name"
    )
    group_id: Optional[str] = Field(
        default=None,
        description="Consumer group ID"
    )
    
    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        case_sensitive=False
    )


class StorageConfig(BaseSettings):
    """Delta Lake storage configuration."""
    
    base_path: str = Field(
        default="/opt/bitnami/spark/storage",
        description="Base path for all storage"
    )
    bronze_path: str = Field(
        default="/opt/bitnami/spark/storage/bronze/events",
        description="Bronze layer path"
    )
    silver_init_path: str = Field(
        default="/opt/bitnami/spark/storage/silver/init",
        description="Silver layer init events path"
    )
    silver_match_path: str = Field(
        default="/opt/bitnami/spark/storage/silver/match",
        description="Silver layer match events path"
    )
    silver_purchase_path: str = Field(
        default="/opt/bitnami/spark/storage/silver/purchase",
        description="Silver layer purchase events path"
    )
    gold_daily_users_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/daily_users",
        description="Gold layer daily users aggregation path"
    )
    gold_minute_purchases_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_purchases",
        description="Gold layer minute purchases aggregation path"
    )
    gold_minute_matches_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_matches",
        description="Gold layer minute matches aggregation path"
    )
    
    model_config = SettingsConfigDict(
        env_prefix="STORAGE_",
        case_sensitive=False
    )


class CheckpointConfig(BaseSettings):
    """Spark streaming checkpoint configuration."""
    
    base_path: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints",
        description="Base path for checkpoints"
    )
    bronze: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/bronze_ingestion",
        description="Bronze ingestion checkpoint"
    )
    silver_init: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/silver_init",
        description="Silver init checkpoint"
    )
    silver_match: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/silver_match",
        description="Silver match checkpoint"
    )
    silver_purchase: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/silver_purchase",
        description="Silver purchase checkpoint"
    )
    gold_purchases: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/gold_purchases",
        description="Gold purchases checkpoint"
    )
    gold_matches: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/gold_matches",
        description="Gold matches checkpoint"
    )
    
    model_config = SettingsConfigDict(
        env_prefix="CHECKPOINT_",
        case_sensitive=False
    )


class SparkConfig(BaseSettings):
    """Spark configuration."""
    
    app_name: str = Field(
        default="gaming-event-processing",
        description="Spark application name"
    )
    master: str = Field(
        default="spark://spark-master:7077",
        description="Spark master URL"
    )
    log_level: str = Field(
        default="WARN",
        description="Spark log level"
    )
    
    model_config = SettingsConfigDict(
        env_prefix="SPARK_",
        case_sensitive=False
    )


class StreamingConfig(BaseSettings):
    """Spark Structured Streaming configuration."""
    
    trigger_interval: str = Field(
        default="10 seconds",
        description="Streaming trigger interval"
    )
    max_offsets_per_trigger: int = Field(
        default=10000,
        description="Maximum offsets to process per trigger"
    )
    watermark_delay: str = Field(
        default="10 minutes",
        description="Watermark delay for late data"
    )
    
    model_config = SettingsConfigDict(
        env_prefix="STREAMING_",
        case_sensitive=False
    )


class LoggingConfig(BaseSettings):
    """Logging configuration."""
    
    level: str = Field(
        default="INFO",
        description="Log level"
    )
    format: str = Field(
        default="json",
        description="Log format (json or text)"
    )
    
    model_config = SettingsConfigDict(
        env_prefix="LOG_",
        case_sensitive=False
    )
