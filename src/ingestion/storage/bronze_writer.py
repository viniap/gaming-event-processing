"""
Bronze Layer Event Ingestion.

Reads raw events from Kafka and writes them to a Delta Lake bronze table
with minimal transformation (multiplex pattern).
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp

from src.ingestion.core.config import BronzeIngestionConfig
from src.common.spark_utils import SparkSessionFactory
from src.common.logger import StructuredLogger, log_exception


logger = StructuredLogger.get_logger(__name__)

class BronzeEventIngestion:
    """Bronze layer ingestion from Kafka to Delta Lake.
    
    Implements the multiplex ingestion pattern where all event types from different
    Kafka topics are written to a single bronze Delta table with minimal transformation.
    This approach simplifies the bronze layer architecture and enables downstream
    processing to filter and parse events as needed.
    
    The ingestion applies only essential transformations:
    - Cast Kafka binary values to string (JSON)
    - Add ingestion timestamp for tracking
    - Preserve Kafka metadata (topic, partition, offset, timestamp)
    
    Attributes:
        spark: Active SparkSession instance for streaming operations.
        config: Configuration object containing Kafka and storage settings.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[BronzeIngestionConfig] = None
    ):
        """Initialize bronze ingestion.
        
        Args:
            spark: SparkSession instance configured for structured streaming.
            config: Optional configuration instance. If None, uses default values.
        """
        self.spark = spark
        self.config = config or BronzeIngestionConfig()
        
        # Get list of topics to subscribe to
        self.topics = self.config.get_topics_list()
        
        logger.info(
            "Bronze ingestion initialized",
            extra={
                "kafka_topics": self.topics,
                "num_topics": len(self.topics),
                "bronze_path": self.config.storage_bronze_path,
                "checkpoint": self.config.checkpoint_bronze
            }
        )
    
    def read_from_kafka(self) -> DataFrame:
        """Read streaming events from multiple Kafka topics.
        
        Configures Kafka consumer with:
        - Subscription to multiple topics using pattern or explicit list
        - Configurable starting offsets (earliest/latest)
        - Rate limiting via maxOffsetsPerTrigger
        - Graceful handling of data loss scenarios
        
        This unified approach prevents concurrent write conflicts that occur when
        multiple separate streaming jobs write to the same Delta table.
        
        Returns:
            Streaming DataFrame with Kafka messages including key, value, topic,
            partition, offset, and timestamp columns.
        """
        # Convert topics list to comma-separated string for Kafka subscribe
        topics_str = ",".join(self.topics)
        
        logger.info(
            "Reading from Kafka",
            extra={
                "bootstrap_servers": self.config.kafka_bootstrap_servers,
                "topics": topics_str,
                "num_topics": len(self.topics),
                "starting_offsets": self.config.kafka_starting_offsets
            }
        )
        
        df = (self.spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
              .option("subscribe", topics_str)
              .option("startingOffsets", self.config.kafka_starting_offsets)
              .option("maxOffsetsPerTrigger", self.config.streaming_max_offsets_per_trigger)
              .option("failOnDataLoss", "false")
              .load())
        
        return df
    
    def transform_to_bronze(self, kafka_df: DataFrame) -> DataFrame:
        """Transform Kafka messages to bronze table format.
        
        Applies minimal transformation following the bronze layer philosophy:
        - Cast binary Kafka value to string (assumes JSON format)
        - Add ingestion timestamp for lineage tracking
        - Preserve all Kafka metadata for debugging and reprocessing
        
        The transformation maintains data fidelity and enables downstream
        silver layer processing to parse and validate the JSON content.
        
        Args:
            kafka_df: Raw streaming DataFrame from Kafka with binary values.
            
        Returns:
            Transformed DataFrame with columns: event_json, ingestion_timestamp,
            kafka_timestamp, kafka_topic, kafka_partition, kafka_offset.
        """
        bronze_df = (kafka_df
                    .selectExpr(
                        "CAST(value AS STRING) as event_json",
                        "timestamp as kafka_timestamp",
                        "topic as kafka_topic",
                        "partition as kafka_partition",
                        "offset as kafka_offset"
                    )
                    .withColumn("ingestion_timestamp", current_timestamp())
                    .select(
                        "event_json",
                        "ingestion_timestamp",
                        "kafka_timestamp",
                        "kafka_topic",
                        "kafka_partition",
                        "kafka_offset"
                    ))
        
        return bronze_df
    
    def write_to_bronze(self, bronze_df: DataFrame):
        """Write streaming data to Delta Lake bronze table.
        
        Configures streaming write with:
        - Append mode for continuous ingestion
        - Checkpointing for exactly-once processing semantics
        - Schema evolution support for flexible schema changes
        - Micro-batch triggering for controlled processing
        
        Args:
            bronze_df: Transformed DataFrame ready for bronze persistence.
            
        Returns:
            StreamingQuery handle for monitoring, control, and termination.
        """
        logger.info(
            "Writing to bronze table",
            extra={
                "path": self.config.storage_bronze_path,
                "checkpoint": self.config.checkpoint_bronze,
                "trigger_interval": self.config.streaming_trigger_interval
            }
        )
        
        query = (bronze_df
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", self.config.checkpoint_bronze)
                .option("mergeSchema", "true")
                .trigger(processingTime=self.config.streaming_trigger_interval)
                .start(self.config.storage_bronze_path))
        
        logger.info(
            "Bronze ingestion stream started",
            extra={"query_id": query.id, "query_name": query.name}
        )
        
        return query
    
    def run(self):
        """Execute the complete bronze ingestion pipeline.
        
        Orchestrates the end-to-end ingestion workflow:
        1. Read streaming data from Kafka
        2. Transform to bronze format
        3. Write to Delta Lake
        4. Monitor streaming query
        
        The method blocks until the streaming query is terminated externally
        or an error occurs. Provides comprehensive error handling and logging.
        
        Raises:
            Exception: Re-raises any exception after logging for upstream handling.
        """
        try:
            logger.info("Starting bronze layer ingestion")
            
            kafka_df = self.read_from_kafka()
            
            bronze_df = self.transform_to_bronze(kafka_df)
            
            query = self.write_to_bronze(bronze_df)
            
            logger.info("Bronze ingestion running, waiting for termination...")
            query.awaitTermination()
            
        except Exception as e:
            log_exception(
                logger,
                e,
                context={"component": "bronze_ingestion"}
            )
            raise


def main():
    """Main entry point for bronze ingestion.
    
    Initializes configuration, creates Spark session, and executes the bronze
    ingestion pipeline. This entry point is used when running the bronze_writer
    module directly, though the recommended approach is using the main.py entry
    point with BronzeIngestionJobBuilder.
    """
    config = BronzeIngestionConfig()
    
    module_logger = StructuredLogger.get_logger(__name__, level=config.log_level)
    
    module_logger.info("Initializing bronze ingestion application")
    
    spark = SparkSessionFactory.create_streaming_session(
        app_name=config.spark_app_name
    )
    spark.sparkContext.setLogLevel(config.spark_log_level)
    
    ingestion = BronzeEventIngestion(spark, config)
    ingestion.run()


if __name__ == "__main__":
    main()
