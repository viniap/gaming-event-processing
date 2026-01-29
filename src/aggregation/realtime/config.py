"""
Real-Time Aggregation Configuration.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RealtimeAggregationConfig(BaseSettings):
    """Real-time aggregation configuration settings.
    
    Configures storage paths, checkpoint locations, streaming parameters, and Spark settings
    for real-time aggregation jobs using Spark Structured Streaming. Settings can be
    overridden via environment variables (case-insensitive) or .env file.
    
    Attributes:
        storage_silver_init_path: Delta Lake path for silver layer init events.
        storage_silver_match_path: Delta Lake path for silver layer match events.
        storage_silver_purchase_path: Delta Lake path for silver layer purchase events.
        storage_gold_minute_purchases_path: Output path for global purchase aggregations.
        storage_gold_minute_matches_path: Output path for global match aggregations.
        storage_gold_minute_purchases_by_country_path: Output path for purchase by country.
        storage_gold_minute_matches_by_country_path: Output path for matches by country.
        checkpoint_gold_purchases: Checkpoint location for global purchase stream.
        checkpoint_gold_matches: Checkpoint location for global match stream.
        checkpoint_gold_purchases_by_country: Checkpoint location for country purchase stream.
        checkpoint_gold_matches_by_country: Checkpoint location for country match stream.
        streaming_trigger_interval: Micro-batch trigger interval for processing.
        streaming_watermark_delay: Maximum delay for late-arriving events.
        window_duration: Time window size for aggregations.
        spark_app_name: Name identifier for the Spark application.
        spark_log_level: Logging level for Spark operations.
        log_level: Application logging level.
    """
    
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
    
    storage_gold_minute_purchases_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_purchases",
        description="Gold minute purchases aggregation path"
    )
    storage_gold_minute_matches_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_matches",
        description="Gold minute matches aggregation path"
    )
    storage_gold_minute_purchases_by_country_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_purchases_by_country",
        description="Gold minute purchases by country aggregation path"
    )
    storage_gold_minute_matches_by_country_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_matches_by_country",
        description="Gold minute matches by country aggregation path"
    )
    
    checkpoint_gold_purchases: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/gold_purchases",
        description="Gold purchases checkpoint"
    )
    checkpoint_gold_matches: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/gold_matches",
        description="Gold matches checkpoint"
    )
    checkpoint_gold_purchases_by_country: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/gold_purchases_by_country",
        description="Gold purchases by country checkpoint"
    )
    checkpoint_gold_matches_by_country: str = Field(
        default="/opt/bitnami/spark/storage/checkpoints/gold_matches_by_country",
        description="Gold matches by country checkpoint"
    )
    
    streaming_trigger_interval: str = Field(
        default="1 minute",
        description="Trigger interval for aggregations"
    )
    streaming_watermark_delay: str = Field(
        default="10 minutes",
        description="Watermark delay for late data"
    )
    window_duration: str = Field(
        default="1 minute",
        description="Window duration for aggregations"
    )
    
    spark_app_name: str = Field(
        default="gold-realtime-aggregation",
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
