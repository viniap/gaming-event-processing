"""
Delta Lake Writer.

Handles writing streaming DataFrames to Delta Lake tables.
"""

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from src.common.logger import StructuredLogger
from src.data_quality.processors.base import EventTypeConfig

logger = StructuredLogger.get_logger(__name__)


class DeltaWriter:
    """Writes streaming DataFrames to Delta Lake tables.
    
    Handles writing silver layer events to Delta Lake with proper configuration
    for streaming operations including checkpointing and schema evolution.
    
    Attributes:
        trigger_interval: Micro-batch processing interval for streaming queries.
    """
    
    def __init__(self, trigger_interval: str = "10 seconds"):
        """Initialize Delta writer.
        
        Args:
            trigger_interval: Micro-batch trigger interval (e.g., '10 seconds', '1 minute').
        """
        self.trigger_interval = trigger_interval
    
    def write(
        self,
        df: DataFrame,
        event_config: EventTypeConfig
    ) -> StreamingQuery:
        """Write streaming DataFrame to Delta Lake table.
        
        Configures the streaming write with:
        - Append mode for incremental updates
        - Checkpointing for fault tolerance
        - Schema evolution support for schema changes
        - Micro-batch triggering for controlled processing
        
        Args:
            df: Transformed silver DataFrame to write.
            event_config: Configuration containing output path and checkpoint location.
            
        Returns:
            StreamingQuery handle for monitoring and control.
        """
        query = (df
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", event_config.checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(processingTime=self.trigger_interval)
            .start(event_config.silver_path))
        
        logger.info(
            "%s events -> Delta",
            event_config.event_type,
            extra={
                "query_id": query.id,
                "path": event_config.silver_path
            }
        )
        return query
