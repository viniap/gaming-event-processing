"""
Spark utilities for creating and configuring Spark sessions.

Provides factory methods for creating properly configured Spark sessions
with Delta Lake support and optimal settings for streaming workloads.
"""

import os
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from src.common.config import SparkConfig
from src.common.logger import StructuredLogger


logger = StructuredLogger.get_logger(__name__)


class SparkSessionFactory:
    """Factory for creating configured Spark sessions."""

    @staticmethod
    def create_session(
        app_name: str,
        config: Optional[SparkConfig] = None,
        enable_delta: bool = True,
        enable_kafka: bool = False,
        enable_auto_compact: Optional[bool] = None,
    ) -> SparkSession:
        """
        Create a Spark session with proper configuration.

        Args:
            app_name: Name of the Spark application
            config: Optional SparkConfig instance
            enable_delta: Whether to enable Delta Lake support
            enable_kafka: Whether to enable Kafka support
            enable_auto_compact: Whether to enable Delta auto compaction.
                               If None, reads from DELTA_AUTO_COMPACT env var (default: true)

        Returns:
            Configured SparkSession instance
        """
        if config is None:
            config = SparkConfig()

        # Determine auto compact setting
        if enable_auto_compact is None:
            enable_auto_compact = os.getenv("DELTA_AUTO_COMPACT", "true").lower() == "true"

        logger.info(f"Creating Spark session: {app_name}")

        # Build Spark configuration
        builder: SparkSession.Builder = SparkSession.builder.appName(app_name)

        # Add Delta Lake support
        if enable_delta:
            builder = (
                builder.config("spark.hadoop.fs.permissions.umask-mode", "000")
                .config("spark.sql.warehouse.dir.permission", "777")
                .config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config("spark.databricks.delta.optimizeWrite.enabled", "true")
                .config("spark.databricks.delta.autoCompact.enabled", str(enable_auto_compact).lower())
                .config("spark.cores.max", "1")
                .config("spark.executor.cores", "1")
                .config("spark.executor.memory", "1g")
            )

        # Streaming optimizations
        builder = (
            builder.config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.streaming.fileSource.log.deletion", "true")
            .config("spark.sql.streaming.fileSource.log.compactInterval", "10")
            .config("spark.sql.streaming.minBatchesToRetain", "10")
            .config("spark.sql.shuffle.partitions", "8")
        )

        # Create session
        spark = builder.getOrCreate()

        # Set log level
        spark.sparkContext.setLogLevel(config.log_level)

        logger.info(
            f"Spark session created successfully",
            extra={
                "app_name": app_name,
                "spark_version": spark.version,
                "master": spark.sparkContext.master,
            },
        )

        return spark

    @staticmethod
    def create_streaming_session(
        app_name: str, config: Optional[SparkConfig] = None
    ) -> SparkSession:
        """
        Create a Spark session optimized for streaming.

        Args:
            app_name: Name of the Spark application
            config: Optional SparkConfig instance

        Returns:
            Configured SparkSession for streaming
        """
        return SparkSessionFactory.create_session(
            app_name=app_name, config=config, enable_delta=True, enable_kafka=True
        )

    @staticmethod
    def create_batch_session(
        app_name: str, config: Optional[SparkConfig] = None
    ) -> SparkSession:
        """
        Create a Spark session optimized for batch processing.

        Args:
            app_name: Name of the Spark application
            config: Optional SparkConfig instance

        Returns:
            Configured SparkSession for batch processing
        """
        return SparkSessionFactory.create_session(
            app_name=app_name, config=config, enable_delta=True, enable_kafka=False
        )
