"""
Bronze Layer Ingestion Main Entry Point.

Generic ingestion job that reads from a configured Kafka topic
and writes to the bronze Delta Lake table.

Usage:
    Set KAFKA_TOPIC environment variable to specify which topic to consume from.
    Multiple instances can run simultaneously for different topics.
    
Example:
    KAFKA_TOPIC=init_events python -m src.ingestion.main
    KAFKA_TOPIC=match_events python -m src.ingestion.main
    KAFKA_TOPIC=purchase_events python -m src.ingestion.main
"""

import sys
from src.common.logger import StructuredLogger
from src.ingestion.core.config import BronzeIngestionConfig
from src.ingestion.builders.job_builder import BronzeIngestionJobBuilder


def main():
    """Main entry point for bronze ingestion job.
    
    Initializes the bronze layer ingestion pipeline that reads raw events from a
    configured Kafka topic and writes them to the bronze Delta Lake table with minimal
    transformation. Implements the multiplex pattern where all event types share a
    single bronze table.
    
    The KAFKA_TOPIC environment variable must be set to specify which topic to consume.
    Multiple instances can run simultaneously for different topics, each with its own
    checkpoint and application name.
    
    Exits with code 1 on configuration errors or runtime failures.
    """
    try:
        config = BronzeIngestionConfig()
    except (ValueError, KeyError, OSError) as e:
        print(f"ERROR: Failed to load configuration: {e}")
        print("Make sure KAFKA_TOPIC environment variable is set.")
        sys.exit(1)
    
    logger = StructuredLogger.get_logger(__name__, level=config.log_level)
    
    logger.info(
        "Starting bronze ingestion job",
        extra={
            "kafka_topic": config.kafka_topic,
            "bronze_path": config.storage_bronze_path,
            "checkpoint_path": config.get_checkpoint_path()
        }
    )
    
    job = BronzeIngestionJobBuilder.from_config(config).build()
    
    try:
        job.run()
    except KeyboardInterrupt:
        logger.info("Bronze ingestion interrupted by user")
        sys.exit(0)
    except (RuntimeError, IOError, ValueError) as e:
        logger.error("Bronze ingestion failed: %s", str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
