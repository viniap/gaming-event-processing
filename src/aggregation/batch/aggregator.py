"""
Batch Aggregator for Gold Layer.

Implements daily aggregations from silver tables to gold tables
for reporting and analytics.
"""

from typing import Optional
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, to_date, from_unixtime, current_timestamp
)

from src.aggregation.batch.config import BatchAggregationConfig
from src.common.logger import StructuredLogger, log_exception


logger = StructuredLogger.get_logger(__name__)


class BatchAggregator:
    """Batch daily aggregations for gold layer.
    
    Processes silver layer data to create daily aggregated metrics in the gold layer,
    following the medallion architecture pattern. Aggregations include:
    - Daily distinct users by country and platform
    
    Attributes:
        spark: Active SparkSession instance for data processing.
        config: Configuration object containing storage paths and Spark settings.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[BatchAggregationConfig] = None
    ):
        """Initialize batch aggregator.
        
        Args:
            spark: SparkSession instance for executing Spark operations.
            config: Optional configuration instance. If None, uses default values.
        """
        self.spark = spark
        self.config = config or BatchAggregationConfig()
        
        logger.info("Batch aggregator initialized")
    
    def aggregate_daily_users(self, date: Optional[str] = None):
        """Aggregate distinct users by country and platform per day.
        
        Reads init events from the silver layer, groups by event date, country, and platform,
        and calculates the count of distinct users for each combination. Results are written
        to the gold layer with partitioning by event_date for efficient querying.
        
        Args:
            date: Date to process in YYYY-MM-DD format. If None, defaults to yesterday.
            
        Returns:
            DataFrame containing aggregated metrics with columns:
                - event_date: The date of the events
                - country_name: User's country
                - platform: Platform (IOS, ANDROID, WEB)
                - distinct_users: Count of unique users
                - aggregation_timestamp: Timestamp when aggregation was performed
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info("Starting daily user aggregation for date: %s", date)
        
        init_df = (self.spark
                  .read
                  .format("delta")
                  .load(self.config.storage_silver_init_path))
        
        init_df = init_df.withColumn(
            "event_date",
            to_date(from_unixtime(col("time") / 1000))
        )
        
        daily_df = init_df.filter(col("event_date") == date)
        
        agg_df = (daily_df
                 .groupBy("event_date", "country_name", "platform")
                 .agg(
                     countDistinct("user-id").alias("distinct_users")
                 )
                 .withColumn("aggregation_timestamp", current_timestamp()))
        
        result_count = agg_df.count()
        logger.info(
            "Daily aggregation completed for %s",
            date,
            extra={
                "date": date,
                "result_count": result_count
            }
        )
        
        (agg_df
         .write
         .format("delta")
         .mode("append")
         .partitionBy("event_date")
         .save(self.config.storage_gold_daily_users_path))
        
        logger.info(
            "Daily user aggregation written to gold table",
            extra={
                "path": self.config.storage_gold_daily_users_path,
                "date": date
            }
        )
        
        return agg_df
    
    def run(self, date: Optional[str] = None):
        """Run batch aggregation pipeline for a specific date.
        
        Executes all batch aggregation jobs for the specified date. Currently includes
        daily user aggregation. Provides comprehensive error handling and logging.
        
        Args:
            date: Date to process in YYYY-MM-DD format. If None, processes yesterday.
            
        Raises:
            Exception: Re-raises any exception that occurs during aggregation after logging.
        """
        try:
            logger.info("Starting batch aggregation pipeline")
            
            self.aggregate_daily_users(date)
            
            logger.info("Batch aggregation completed successfully")
            
        except Exception as e:
            log_exception(
                logger,
                e,
                context={"component": "batch_aggregation", "date": date}
            )
            raise
