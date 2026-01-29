"""
Batch Aggregation Configuration.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BatchAggregationConfig(BaseSettings):
    """Batch aggregation configuration settings.
    
    Configures storage paths, Spark settings, and logging for batch aggregation jobs.
    Settings can be overridden via environment variables (case-insensitive) or .env file.
    
    Attributes:
        storage_silver_init_path: Delta Lake path for silver layer init events.
        storage_gold_daily_users_path: Delta Lake path for gold layer daily user aggregations.
        spark_app_name: Name identifier for the Spark application.
        spark_log_level: Logging level for Spark operations (DEBUG, INFO, WARN, ERROR).
        log_level: Application logging level.
    """
    
    storage_silver_init_path: str = Field(
        default="/opt/bitnami/spark/storage/silver/init",
        description="Silver init table path"
    )
    
    storage_gold_daily_users_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/daily_users",
        description="Gold daily users aggregation path"
    )
    
    spark_app_name: str = Field(
        default="gold-batch-aggregation",
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
