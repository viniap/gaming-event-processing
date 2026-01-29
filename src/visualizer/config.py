"""Visualizer Configuration Module.

Provides configuration management for the data visualizer application using
Pydantic settings. Manages Delta Lake storage paths, display settings, Spark
configuration, and logging preferences.

Classes:
    VisualizerConfig: Configuration settings for data visualization.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class VisualizerConfig(BaseSettings):
    """Configuration settings for data visualization.
    
    Manages all configuration parameters for the data visualizer application,
    including Delta Lake storage paths for all medallion layers, display preferences,
    Spark session settings, and logging configuration.
    
    Attributes:
        storage_bronze_path: Path to bronze layer Delta table.
        storage_silver_init_path: Path to silver init events Delta table.
        storage_silver_match_path: Path to silver match events Delta table.
        storage_silver_purchase_path: Path to silver purchase events Delta table.
        storage_gold_daily_users_path: Path to gold daily users aggregation.
        storage_gold_minute_purchases_path: Path to gold minute purchases aggregation.
        storage_gold_minute_matches_path: Path to gold minute matches aggregation.
        storage_gold_minute_purchases_by_country_path: Path to country purchase aggregations.
        storage_gold_minute_matches_by_country_path: Path to country match aggregations.
        num_rows: Number of sample rows to display in visualizations.
        spark_app_name: Name identifier for Spark application.
        spark_log_level: Spark logging verbosity level.
        log_level: Application logging verbosity level.
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
    storage_gold_daily_users_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/daily_users",
        description="Gold daily users table path"
    )
    storage_gold_minute_purchases_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_purchases",
        description="Gold minute purchases table path"
    )
    storage_gold_minute_matches_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_matches",
        description="Gold minute matches table path"
    )
    storage_gold_minute_purchases_by_country_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_purchases_by_country",
        description="Gold minute purchases by country table path"
    )
    storage_gold_minute_matches_by_country_path: str = Field(
        default="/opt/bitnami/spark/storage/gold/minute_matches_by_country",
        description="Gold minute matches by country table path"
    )
    
    num_rows: int = Field(
        default=10,
        description="Number of rows to display"
    )
    
    spark_app_name: str = Field(
        default="data-visualizer",
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
