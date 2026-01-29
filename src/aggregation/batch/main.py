"""
Batch Aggregation Main Entry Point.
"""

from typing import Optional

from src.aggregation.batch.aggregator import BatchAggregator
from src.aggregation.batch.config import BatchAggregationConfig
from src.common.spark_utils import SparkSessionFactory
from src.common.logger import StructuredLogger


def main(date: Optional[str] = None):
    """Main entry point for batch aggregation.
    
    Initializes configuration, creates a Spark session, and executes the batch
    aggregation pipeline. Processes init events to generate daily user metrics
    aggregated by country and platform.
    
    Args:
        date: Optional date to process in YYYY-MM-DD format. If None, processes yesterday.
    """
    config = BatchAggregationConfig()
    
    logger = StructuredLogger.get_logger(__name__, level=config.log_level)
    
    logger.info("Initializing batch aggregation application")
    
    spark = SparkSessionFactory.create_batch_session(
        app_name=config.spark_app_name
    )
    spark.sparkContext.setLogLevel(config.spark_log_level)
    
    aggregator = BatchAggregator(spark, config)
    aggregator.run(date)


if __name__ == "__main__":
    import sys
    
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(date_arg)
