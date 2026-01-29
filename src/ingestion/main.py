"""
Bronze Layer Ingestion Main Entry Point.

Unified ingestion job that reads from multiple configured Kafka topics
and writes to the bronze Delta Lake table. This approach prevents concurrent
write conflicts that occur when multiple separate streaming jobs write to the
same Delta table.

Usage:
    Set KAFKA_TOPICS environment variable to specify comma-separated topics to consume from.
    If not set, defaults to: init_events,match_events,purchase_events
    
Example:
    KAFKA_TOPICS=init_events,match_events,purchase_events python -m src.ingestion.main
"""

import sys
from src.common.logger import StructuredLogger
from src.ingestion.core.config import BronzeIngestionConfig
from src.ingestion.storage.bronze_writer import BronzeEventIngestion
from src.common.spark_utils import SparkSessionFactory


def main():
    """Main entry point for unified bronze ingestion job.
    
    Initializes the bronze layer ingestion pipeline that reads raw events from multiple
    configured Kafka topics and writes them to the bronze Delta Lake table with minimal
    transformation. Implements the multiplex pattern where all event types share a
    single bronze table.
    
    This unified approach ensures proper Delta Lake transaction coordination by using
    a single Spark streaming job instead of multiple concurrent jobs.
    
    The KAFKA_TOPICS environment variable can be set to specify which topics to consume
    (comma-separated). If not set, defaults to all event topics.
    
    Exits with code 1 on configuration errors or runtime failures.
    """
    try:
        config = BronzeIngestionConfig()
    except (ValueError, KeyError, OSError) as e:
        print(f"ERROR: Failed to load configuration: {e}")
        print("Make sure KAFKA_TOPICS environment variable is set (or uses default).")
        sys.exit(1)
    
    logger = StructuredLogger.get_logger(__name__, level=config.log_level)
    
    # Get list of topics to ingest
    topics = config.get_topics_list()
    
    logger.info(
        "Starting unified bronze ingestion job",
        extra={
            "kafka_topics": topics,
            "num_topics": len(topics),
            "bronze_path": config.storage_bronze_path,
            "checkpoint_path": config.get_checkpoint_path()
        }
    )
    
    # Create Spark session
    spark = SparkSessionFactory.create_streaming_session(
        app_name=config.get_app_name()
    )
    spark.sparkContext.setLogLevel(config.spark_log_level)
    
    # Create and run ingestion job
    ingestion = BronzeEventIngestion(spark, config)
    
    try:
        ingestion.run()
    except KeyboardInterrupt:
        logger.info("Bronze ingestion interrupted by user")
        sys.exit(0)
    except (RuntimeError, IOError, ValueError) as e:
        logger.error("Bronze ingestion failed: %s", str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
