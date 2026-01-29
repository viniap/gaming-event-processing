"""
Real-Time Aggregator for Gold Layer.

Implements minute-level aggregations from silver tables to gold tables
using Spark Structured Streaming with watermarking for late data.
"""

from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, sum as _sum, approx_count_distinct,
    current_timestamp, to_timestamp, from_unixtime, expr
)

from src.aggregation.realtime.config import RealtimeAggregationConfig
from src.common.logger import StructuredLogger, log_exception


logger = StructuredLogger.get_logger(__name__)


class RealtimeAggregator:
    """Real-time minute-level aggregations for gold layer using Spark Structured Streaming.
    
    Processes silver layer streaming data to create minute-level aggregated metrics
    in the gold layer. Implements watermarking for late data handling and stream-stream
    joins for country enrichment. Aggregations include:
    
    - Global purchase metrics: count, revenue, distinct users
    - Global match metrics: count, distinct users
    - Purchase metrics by country: revenue, count, distinct users
    - Match metrics by country: count, distinct users
    
    Country information is enriched by joining purchase and match events with init events
    using stream-stream joins with time range constraints and watermarking for robustness.
    
    Attributes:
        spark: Active SparkSession instance with streaming capabilities.
        config: Configuration object containing paths, checkpoints, and streaming settings.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[RealtimeAggregationConfig] = None
    ):
        """Initialize real-time aggregator.
        
        Args:
            spark: SparkSession instance configured for structured streaming.
            config: Optional configuration instance. If None, uses default values.
        """
        self.spark = spark
        self.config = config or RealtimeAggregationConfig()
        
        logger.info(
            "Real-time aggregator initialized",
            extra={
                "window_duration": self.config.window_duration,
                "watermark_delay": self.config.streaming_watermark_delay
            }
        )
    
    def aggregate_purchases(self):
        """Aggregate purchase metrics per minute globally.
        
        Reads streaming purchase events from silver layer, applies watermarking for late
        data handling, and aggregates metrics within 1-minute windows. Results are written
        to the gold layer using append mode for incremental updates.
        
        Metrics:
            - purchase_count: Total number of purchases
            - total_revenue: Sum of all purchase values
            - distinct_users: Approximate count of unique users making purchases
        
        Returns:
            StreamingQuery: Active streaming query handle that can be used to monitor
                or terminate the stream.
        """
        logger.info("Starting global purchase aggregation")
        
        purchase_df = (self.spark
                      .readStream
                      .format("delta")
                      .load(self.config.storage_silver_purchase_path))
        
        purchase_df = purchase_df.withColumn(
            "event_timestamp",
            to_timestamp(from_unixtime(col("time") / 1000))
        )
        
        purchase_df = purchase_df.withWatermark("event_timestamp", self.config.streaming_watermark_delay)
        
        agg_df = (purchase_df
                 .groupBy(
                     window(col("event_timestamp"), self.config.window_duration)
                 )
                 .agg(
                     count("*").alias("purchase_count"),
                     _sum("purchase_value").alias("total_revenue"),
                     approx_count_distinct("user-id").alias("distinct_users")
                 )
                 .select(
                     col("window.start").alias("window_start"),
                     col("window.end").alias("window_end"),
                     col("purchase_count"),
                     col("total_revenue"),
                     col("distinct_users")
                 )
                 .withColumn("aggregation_timestamp", current_timestamp()))
        
        query = (agg_df
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", self.config.checkpoint_gold_purchases)
                .option("mergeSchema", "true")
                .trigger(processingTime=self.config.streaming_trigger_interval)
                .start(self.config.storage_gold_minute_purchases_path))
        
        logger.info(
            "Global purchase aggregation started",
            extra={
                "query_id": query.id,
                "path": self.config.storage_gold_minute_purchases_path
            }
        )
        
        return query
    
    def aggregate_purchases_by_country(self):
        """Aggregate purchase revenue by country per minute.
        
        Enriches purchase data with country information from init events using stream-stream
        joins with time range constraints. Performs a left join to include purchases even if
        user country is not found. The join condition ensures init events are within 24 hours
        before the purchase event.
        
        Metrics per country:
            - country_revenue: Sum of purchase values
            - purchase_count: Total number of purchases
            - distinct_users: Approximate count of unique users
        
        Returns:
            StreamingQuery: Active streaming query handle for monitoring or termination.
        """
        logger.info("Starting purchase aggregation by country")
        
        purchase_df = (self.spark
                      .readStream
                      .format("delta")
                      .load(self.config.storage_silver_purchase_path))
        
        init_df = (self.spark
                  .readStream
                  .format("delta")
                  .load(self.config.storage_silver_init_path))
        
        purchase_df = purchase_df.withColumn(
            "event_timestamp",
            to_timestamp(from_unixtime(col("time") / 1000))
        )
        
        init_df = init_df.withColumn(
            "init_timestamp",
            to_timestamp(from_unixtime(col("time") / 1000))
        )
        
        purchase_df = purchase_df.withWatermark("event_timestamp", self.config.streaming_watermark_delay)
        init_df = init_df.withWatermark("init_timestamp", self.config.streaming_watermark_delay)
        
        enriched_df = purchase_df.join(
            init_df.select(
                col("user-id").alias("init_user_id"),
                col("country_name"),
                col("init_timestamp")
            ),
            (purchase_df["user-id"] == col("init_user_id")) &
            (col("init_timestamp") <= purchase_df["event_timestamp"]) &
            (col("init_timestamp") >= purchase_df["event_timestamp"] - expr("INTERVAL 24 HOURS")),
            how="left"
        ).drop("init_user_id", "init_timestamp")
        
        agg_df = (enriched_df
                 .groupBy(
                     window(col("event_timestamp"), self.config.window_duration),
                     col("country_name")
                 )
                 .agg(
                     _sum("purchase_value").alias("country_revenue"),
                     count("*").alias("purchase_count"),
                     approx_count_distinct("user-id").alias("distinct_users")
                 )
                 .select(
                     col("window.start").alias("window_start"),
                     col("window.end").alias("window_end"),
                     col("country_name"),
                     col("country_revenue"),
                     col("purchase_count"),
                     col("distinct_users")
                 )
                 .withColumn("aggregation_timestamp", current_timestamp()))
        
        query = (agg_df
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", self.config.checkpoint_gold_purchases_by_country)
                .option("mergeSchema", "true")
                .trigger(processingTime=self.config.streaming_trigger_interval)
                .start(self.config.storage_gold_minute_purchases_by_country_path))
        
        logger.info(
            "Purchase aggregation by country started",
            extra={
                "query_id": query.id,
                "path": self.config.storage_gold_minute_purchases_by_country_path
            }
        )
        
        return query
    
    def aggregate_matches(self):
        """Aggregate match metrics per minute globally.
        
        Reads streaming match events from silver layer, applies watermarking, and aggregates
        metrics within 1-minute windows. Tracks distinct users for both user-a and user-b
        separately as each match involves two players.
        
        Metrics:
            - match_count: Total number of matches
            - distinct_users_a: Approximate count of unique users in user-a position
            - distinct_users_b: Approximate count of unique users in user-b position
        
        Returns:
            StreamingQuery: Active streaming query handle for monitoring or termination.
        """
        logger.info("Starting global match aggregation")
        
        match_df = (self.spark
                   .readStream
                   .format("delta")
                   .load(self.config.storage_silver_match_path))
        
        match_df = match_df.withColumn(
            "event_timestamp",
            to_timestamp(from_unixtime(col("time") / 1000))
        )
        
        match_df = match_df.withWatermark("event_timestamp", self.config.streaming_watermark_delay)
        
        agg_df = (match_df
                 .groupBy(
                     window(col("event_timestamp"), self.config.window_duration)
                 )
                 .agg(
                     count("*").alias("match_count"),
                     approx_count_distinct("user-a").alias("distinct_users_a"),
                     approx_count_distinct("user-b").alias("distinct_users_b")
                 )
                 .select(
                     col("window.start").alias("window_start"),
                     col("window.end").alias("window_end"),
                     col("match_count"),
                     col("distinct_users_a"),
                     col("distinct_users_b")
                 )
                 .withColumn("aggregation_timestamp", current_timestamp()))
        
        query = (agg_df
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", self.config.checkpoint_gold_matches)
                .option("mergeSchema", "true")
                .trigger(processingTime=self.config.streaming_trigger_interval)
                .start(self.config.storage_gold_minute_matches_path))
        
        logger.info(
            "Global match aggregation started",
            extra={
                "query_id": query.id,
                "path": self.config.storage_gold_minute_matches_path
            }
        )
        
        return query
    
    def aggregate_matches_by_country(self):
        """Aggregate match count by country per minute.
        
        Enriches match data with country information from init events using stream-stream
        joins with time range constraints. Uses user-a as the primary user for country
        attribution. Performs a left join to include matches even if user country is not found.
        The join condition ensures init events are within 24 hours before the match event.
        
        Metrics per country:
            - match_count: Total number of matches
            - distinct_users: Approximate count of unique users (based on user-a)
        
        Returns:
            StreamingQuery: Active streaming query handle for monitoring or termination.
        """
        logger.info("Starting match aggregation by country")
        
        match_df = (self.spark
                   .readStream
                   .format("delta")
                   .load(self.config.storage_silver_match_path))
        
        init_df = (self.spark
                  .readStream
                  .format("delta")
                  .load(self.config.storage_silver_init_path))
        
        match_df = match_df.withColumn(
            "event_timestamp",
            to_timestamp(from_unixtime(col("time") / 1000))
        )
        
        init_df = init_df.withColumn(
            "init_timestamp",
            to_timestamp(from_unixtime(col("time") / 1000))
        )
        
        match_df = match_df.withWatermark("event_timestamp", self.config.streaming_watermark_delay)
        init_df = init_df.withWatermark("init_timestamp", self.config.streaming_watermark_delay)
        
        init_for_join = init_df.select(
            col("user-id").alias("user_a_id"),
            col("country_name"),
            col("init_timestamp")
        )
        
        enriched_df = match_df.join(
            init_for_join,
            (match_df["user-a"] == init_for_join["user_a_id"]) &
            (init_for_join["init_timestamp"] <= match_df["event_timestamp"]) &
            (init_for_join["init_timestamp"] >= match_df["event_timestamp"] - expr("INTERVAL 24 HOURS")),
            how="left"
        ).drop("user_a_id", "init_timestamp")
        
        agg_df = (enriched_df
                 .groupBy(
                     window(col("event_timestamp"), self.config.window_duration),
                     col("country_name")
                 )
                 .agg(
                     count("*").alias("match_count"),
                     approx_count_distinct("user-a").alias("distinct_users")
                 )
                 .select(
                     col("window.start").alias("window_start"),
                     col("window.end").alias("window_end"),
                     col("country_name"),
                     col("match_count"),
                     col("distinct_users")
                 )
                 .withColumn("aggregation_timestamp", current_timestamp()))
        
        query = (agg_df
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", self.config.checkpoint_gold_matches_by_country)
                .option("mergeSchema", "true")
                .trigger(processingTime=self.config.streaming_trigger_interval)
                .start(self.config.storage_gold_minute_matches_by_country_path))
        
        logger.info(
            "Match aggregation by country started",
            extra={
                "query_id": query.id,
                "path": self.config.storage_gold_minute_matches_by_country_path
            }
        )
        
        return query
    
    def run(self):
        """Run all real-time aggregation streams.
        
        Starts all streaming queries for both global and country-level aggregations:
        - Global purchase aggregation
        - Global match aggregation
        - Purchase aggregation by country
        - Match aggregation by country
        
        The method blocks until one of the queries terminates (normally runs indefinitely).
        Provides comprehensive error handling and logging for all streaming operations.
        
        Raises:
            Exception: Re-raises any exception that occurs during stream execution after logging.
        """
        try:
            logger.info("Starting real-time aggregation pipeline")
            
            purchase_query = self.aggregate_purchases()
            match_query = self.aggregate_matches()
            
            purchase_by_country_query = self.aggregate_purchases_by_country()
            match_by_country_query = self.aggregate_matches_by_country()
            
            queries = [
                purchase_query,
                match_query,
                purchase_by_country_query,
                match_by_country_query
            ]
            
            logger.info(
                "All real-time aggregation streams started",
                extra={
                    "streams": [
                        "purchases_global",
                        "matches_global",
                        "purchases_by_country",
                        "matches_by_country"
                    ],
                    "query_count": len(queries)
                }
            )
            
            queries[0].awaitTermination()
            
        except Exception as e:
            log_exception(
                logger,
                e,
                context={"component": "realtime_aggregation"}
            )
            raise
