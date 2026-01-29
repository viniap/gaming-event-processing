"""
Real-Time Aggregation Main Entry Point.
"""

from src.aggregation.realtime.aggregator import RealtimeAggregator
from src.aggregation.realtime.config import RealtimeAggregationConfig
from src.common.spark_utils import SparkSessionFactory
from src.common.logger import StructuredLogger


def main():
    """Main entry point for real-time aggregation.
    
    Initializes configuration, creates a Spark streaming session, and executes the
    real-time aggregation pipeline. Processes silver layer streaming events to generate
    minute-level metrics in the gold layer for both global and country-level aggregations.
    
    The application runs indefinitely until terminated externally or an error occurs.
    """
    config = RealtimeAggregationConfig()
    
    logger = StructuredLogger.get_logger(__name__, level=config.log_level)
    
    logger.info("Initializing real-time aggregation application")
    
    spark = SparkSessionFactory.create_streaming_session(
        app_name=config.spark_app_name
    )
    spark.sparkContext.setLogLevel(config.spark_log_level)
    
    aggregator = RealtimeAggregator(spark, config)
    aggregator.run()


if __name__ == "__main__":
    main()
