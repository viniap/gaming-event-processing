"""
Silver Layer Data Quality Main Entry Point.

Applies data quality transformations to events from bronze layer
and writes to silver layer Delta tables and Kafka topics.

Usage:
    python -m src.data_quality.main
"""

import sys

from src.common.logger import StructuredLogger
from src.common.spark_utils import SparkSessionFactory
from src.data_quality.core.config import DataQualityConfig
from src.data_quality.core.pipeline import SilverDataQualityPipeline


def main():
    """Main entry point for silver data quality pipeline.
    
    Initializes the data quality transformation pipeline that reads events from the bronze
    layer, applies configurable transformations based on YAML rules, and writes the cleaned
    data to both silver layer Delta tables and Kafka topics for downstream consumption.
    
    The pipeline processes three event types: init, match, and purchase events, applying
    type-specific transformations while maintaining extensibility for future event types.
    
    Exits with code 1 on configuration errors or runtime failures.
    """
    try:
        config = DataQualityConfig()
    except (ValueError, KeyError, OSError) as e:
        print(f"ERROR: Failed to load configuration: {e}")
        sys.exit(1)
    
    logger = StructuredLogger.get_logger(__name__, level=config.log_level)
    
    logger.info(
        "Starting silver data quality pipeline",
        extra={
            "bronze_path": config.storage_bronze_path,
            "rules_dir": config.rules_dir
        }
    )
    
    spark = SparkSessionFactory.create_streaming_session(
        app_name=config.spark_app_name
    )
    spark.sparkContext.setLogLevel(config.spark_log_level)
    
    pipeline = SilverDataQualityPipeline(spark, config)
    
    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("Silver data quality pipeline interrupted by user")
        sys.exit(0)
    except (RuntimeError, IOError, ValueError) as e:
        logger.error("Silver data quality pipeline failed: %s", str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
