"""
Abstract Base Class for Bronze Ingestion Jobs.

Uses Template Method design pattern to provide a generic ingestion framework
that can be configured for different Kafka topics while maintaining common logic.
"""

from abc import ABC, abstractmethod
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp

from src.common.logger import StructuredLogger
from src.common.spark_utils import SparkSessionFactory


logger = StructuredLogger.get_logger(__name__)


class BronzeIngestionJob(ABC):
    """Abstract base class for bronze layer ingestion jobs.
    
    Implements the Template Method design pattern to provide a generic ingestion framework
    with a well-defined algorithm that can be configured for different Kafka topics while
    maintaining common processing logic.
    
    The template method run() defines the ingestion workflow:
    1. Initialize Spark session
    2. Read from Kafka stream
    3. Parse and transform data
    4. Write to bronze Delta table
    5. Start and monitor streaming query
    
    Subclasses customize behavior by overriding specific abstract methods (e.g.,
    _get_trigger_interval()) or hook methods (e.g., _transform_data()) without
    changing the overall algorithm structure.
    
    Attributes:
        kafka_bootstrap_servers: Kafka broker connection string.
        kafka_topic: Kafka topic to consume from.
        checkpoint_location: Path for maintaining streaming state.
        bronze_table_path: Delta Lake path for bronze table writes.
        app_name: Spark application identifier.
        spark: SparkSession instance (initialized during run()).
    """
    
    def __init__(
        self,
        *,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        checkpoint_location: str,
        bronze_table_path: str,
        app_name: str = "BronzeIngestion"
    ):
        """Initialize the bronze ingestion job.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses (e.g., 'kafka:9092').
            kafka_topic: Kafka topic name to read events from.
            checkpoint_location: Path for streaming checkpoint storage.
            bronze_table_path: Delta Lake path to write bronze table.
            app_name: Spark application name for identification and monitoring.
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.checkpoint_location = checkpoint_location
        self.bronze_table_path = bronze_table_path
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        
        logger.info(
            "Bronze ingestion job initialized",
            extra={
                "kafka_topic": kafka_topic,
                "checkpoint_location": checkpoint_location,
                "bronze_table_path": bronze_table_path
            }
        )
    
    def run(self):
        """Execute the ingestion pipeline using template method pattern.
        
        Orchestrates the complete ingestion workflow by calling methods in a specific
        order to ensure consistent processing. This method should not be overridden;
        instead, customize behavior by overriding specific hook or abstract methods.
        
        Workflow steps:
        1. Create and configure Spark session
        2. Read streaming data from Kafka
        3. Parse Kafka binary messages to strings
        4. Transform parsed data (add metadata)
        5. Write to bronze Delta table
        6. Await termination (blocks indefinitely)
        
        The method includes error handling and cleanup in a finally block to ensure
        resources are properly released even if an error occurs.
        
        Raises:
            Exception: Re-raises any exception that occurs during ingestion after logging.
        """
        try:
            logger.info("Starting bronze ingestion from topic: %s", self.kafka_topic)
            
            self.spark = self._create_spark_session()
            
            kafka_stream = self._read_kafka_stream()
            
            parsed_stream = self._parse_kafka_messages(kafka_stream)
            transformed_stream = self._transform_data(parsed_stream)
            
            query = self._write_to_bronze(transformed_stream)
            
            logger.info("Bronze ingestion query started successfully")
            query.awaitTermination()
            
        except Exception as e:
            logger.error("Bronze ingestion failed: %s", str(e), exc_info=True)
            raise
        finally:
            self._cleanup()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session for streaming.
        
        Hook method that can be overridden by subclasses to customize Spark configuration
        (e.g., adding custom Spark properties, changing resource allocation).
        
        Returns:
            Configured SparkSession with streaming capabilities.
        """
        return SparkSessionFactory.create_streaming_session(
            app_name=self.app_name
        )
    
    def _read_kafka_stream(self) -> DataFrame:
        """Read streaming data from configured Kafka topic.
        
        Configures Kafka consumer to read from the earliest available offset with
        failOnDataLoss disabled to handle topic deletions gracefully.
        
        Returns:
            Streaming DataFrame with raw Kafka messages containing key, value, topic,
            partition, offset, and timestamp columns.
        """
        logger.info("Reading from Kafka topic: %s", self.kafka_topic)
        
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
    
    def _parse_kafka_messages(self, kafka_df: DataFrame) -> DataFrame:
        """Parse Kafka messages from binary to string format.
        
        Converts the binary value column to string (JSON) and selects relevant
        metadata fields for downstream processing.
        
        Args:
            kafka_df: Raw Kafka streaming DataFrame with binary values.
            
        Returns:
            DataFrame with parsed string values and selected Kafka metadata fields.
        """
        return kafka_df.selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as value",
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp"
        )
    
    def _transform_data(self, parsed_df: DataFrame) -> DataFrame:
        """Transform parsed data before writing to bronze.
        
        Hook method that can be overridden by subclasses to add custom transformations.
        The default implementation adds an ingestion timestamp for tracking when data
        entered the bronze layer.
        
        Args:
            parsed_df: DataFrame with parsed Kafka messages.
            
        Returns:
            Transformed DataFrame with additional metadata columns.
        """
        return parsed_df.withColumn("ingestion_timestamp", current_timestamp())
    
    @abstractmethod
    def _get_trigger_interval(self) -> str:
        """Get the streaming trigger interval.
        
        Abstract method that must be implemented by concrete subclasses to specify
        the micro-batch processing interval.
        
        Returns:
            Trigger interval string (e.g., '10 seconds', '1 minute').
        """
        raise NotImplementedError("Subclasses must implement _get_trigger_interval")
    
    def _write_to_bronze(self, df: DataFrame):
        """Write transformed data to bronze Delta Lake table.
        
        Configures streaming write with append mode, checkpointing for fault tolerance,
        and schema evolution support. Uses micro-batch triggering for controlled processing.
        
        Args:
            df: Transformed DataFrame ready for bronze layer persistence.
            
        Returns:
            StreamingQuery object for monitoring and control.
        """
        logger.info("Writing to bronze table: %s", self.bronze_table_path)
        
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_location)
            .option("mergeSchema", "true")
            .trigger(processingTime=self._get_trigger_interval())
            .start(self.bronze_table_path)
        )
    
    def _cleanup(self):
        """Cleanup resources after ingestion completes or fails.
        
        Hook method that can be overridden by subclasses to add custom cleanup logic
        (e.g., closing connections, releasing locks). Default implementation stops
        the Spark session if it was initialized.
        """
        if self.spark:
            logger.info("Stopping Spark session")
            self.spark.stop()


class ConfigurableBronzeIngestion(BronzeIngestionJob):
    """Concrete implementation of BronzeIngestionJob with full parameter injection.
    
    Provides a ready-to-use ingestion job that accepts all configuration via constructor
    parameters, eliminating the need for subclassing. This implementation follows the
    Strategy pattern where configuration is injected at runtime.
    
    This class is ideal for creating multiple ingestion instances for different topics
    through configuration rather than inheritance, promoting flexibility and reusability.
    
    Attributes:
        trigger_interval: Configured micro-batch processing interval.
    """
    
    def __init__(
        self,
        *,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        checkpoint_location: str,
        bronze_table_path: str,
        trigger_interval: str = "10 seconds",
        app_name: str = "BronzeIngestion"
    ):
        """Initialize configurable bronze ingestion job.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses (e.g., 'kafka:9092').
            kafka_topic: Kafka topic name to consume from.
            checkpoint_location: Path for streaming checkpoint storage.
            bronze_table_path: Delta Lake path for bronze table writes.
            trigger_interval: Micro-batch processing interval (default: '10 seconds').
            app_name: Spark application name for identification.
        """
        super().__init__(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic=kafka_topic,
            checkpoint_location=checkpoint_location,
            bronze_table_path=bronze_table_path,
            app_name=app_name
        )
        self.trigger_interval = trigger_interval
    
    def _get_trigger_interval(self) -> str:
        """Return the configured trigger interval."""
        return self.trigger_interval
