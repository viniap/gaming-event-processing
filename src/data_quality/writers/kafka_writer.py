"""
Kafka Writer.

Handles publishing streaming DataFrames to Kafka topics.
"""

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, to_json
from pyspark.sql.streaming import StreamingQuery

from src.common.logger import StructuredLogger
from src.data_quality.processors.base import EventTypeConfig

logger = StructuredLogger.get_logger(__name__)


class KafkaWriter:
    """Writes streaming DataFrames to Kafka topics.
    
    Publishes silver layer events to Kafka for downstream consumers. Converts
    DataFrame rows to JSON format and streams to a configured Kafka topic.
    Can be disabled via configuration for environments without Kafka.
    
    Attributes:
        bootstrap_servers: Kafka broker connection string.
        topic: Target Kafka topic name.
        trigger_interval: Micro-batch processing interval.
        enabled: Flag to enable/disable Kafka publishing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        trigger_interval: str = "10 seconds",
        enabled: bool = True
    ):
        """Initialize Kafka writer.
        
        Args:
            bootstrap_servers: Kafka broker connection string (e.g., 'kafka:9092').
            topic: Kafka topic name for publishing silver events.
            trigger_interval: Micro-batch trigger interval (e.g., '10 seconds').
            enabled: Whether Kafka output is enabled. Set to False to disable publishing.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.trigger_interval = trigger_interval
        self.enabled = enabled
    
    def write(
        self,
        df: DataFrame,
        event_config: EventTypeConfig
    ) -> Optional[StreamingQuery]:
        """Write streaming DataFrame to Kafka topic.
        
        Converts DataFrame rows to JSON and publishes to Kafka. Uses a separate
        checkpoint location (suffixed with '_kafka') to maintain independent state
        from Delta writes.
        
        Args:
            df: Transformed silver DataFrame to publish.
            event_config: Configuration containing checkpoint location and event type info.
            
        Returns:
            StreamingQuery handle for monitoring, or None if Kafka output is disabled.
        """
        if not self.enabled:
            return None
        
        kafka_df = df.select(to_json(struct("*")).alias("value"))
        query = (kafka_df
            .writeStream
            .format("kafka")
            .outputMode("append")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("topic", self.topic)
            .option("checkpointLocation", f"{event_config.checkpoint_path}_kafka")
            .trigger(processingTime=self.trigger_interval)
            .start())
        
        logger.info(
            "%s events -> Kafka",
            event_config.event_type,
            extra={
                "query_id": query.id,
                "topic": self.topic
            }
        )
        return query
