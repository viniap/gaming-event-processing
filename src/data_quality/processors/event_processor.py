"""
Event Processor.

Handles bronze data reading, JSON parsing, filtering, and preprocessing
for different event types.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json

from src.common.logger import StructuredLogger
from src.data_quality.processors.base import EventTypeConfig
from src.data_quality.processors.schema import EventSchemaProvider
from src.data_quality.transformations.loader import TransformationLoader

logger = StructuredLogger.get_logger(__name__)


class EventProcessor:
    """Processes events from bronze layer with data quality transformations.
    
    Handles the complete event processing workflow including:
    - Reading streaming data from bronze Delta table
    - Parsing JSON event payloads
    - Filtering events by type
    - Applying optional preprocessing (e.g., flattening nested structures)
    - Applying configurable data quality transformations from YAML rules
    
    Attributes:
        spark: Active SparkSession instance.
        transformation_loader: Loader for applying YAML-defined transformation rules.
        schema_provider: Provider for event JSON schema definitions.
    """
    
    def __init__(self, spark: SparkSession, transformation_loader: TransformationLoader):
        """Initialize event processor.
        
        Args:
            spark: SparkSession instance for Spark operations.
            transformation_loader: Loader for applying transformation rules.
        """
        self.spark = spark
        self.transformation_loader = transformation_loader
        self.schema_provider = EventSchemaProvider()
    
    def read_bronze(self, bronze_path: str) -> DataFrame:
        """Read streaming data from bronze Delta table.
        
        Args:
            bronze_path: Path to bronze layer Delta table containing multiplex events.
            
        Returns:
            Streaming DataFrame with bronze table schema including value, ingestion_timestamp,
            and kafka_timestamp columns.
        """
        logger.info("Reading from bronze: %s", bronze_path)
        
        df = (self.spark
              .readStream
              .format("delta")
              .load(bronze_path))
        
        return df
    
    def parse_and_filter(
        self,
        bronze_df: DataFrame,
        event_config: EventTypeConfig
    ) -> DataFrame:
        """Parse JSON events and filter by event type.
        
        Implements the first step of the template method pattern: parsing the JSON payload
        from the bronze table value column and filtering for a specific event type. Optionally
        applies preprocessing for complex event structures.
        
        Args:
            bronze_df: Streaming DataFrame from bronze table with JSON in value column.
            event_config: Configuration specifying event filter and optional preprocessing.
            
        Returns:
            Parsed and filtered DataFrame with expanded JSON fields and original timestamps.
        """
        parsed_df = (bronze_df
            .withColumn("event_data", from_json(col("value"), self.schema_provider.get_event_schema()))
            .select("event_data.*", "ingestion_timestamp", "kafka_timestamp")
            .filter(col("event-type") == event_config.event_filter))
        
        if event_config.preprocess_fn:
            parsed_df = event_config.preprocess_fn(parsed_df)
        
        return parsed_df
    
    def apply_transformations(
        self,
        df: DataFrame,
        event_config: EventTypeConfig
    ) -> DataFrame:
        """Apply data quality transformations and add processing metadata.
        
        Implements the second step of the template method pattern: applying configured
        transformations from YAML rules (e.g., uppercase, mapping) and adding a processing
        timestamp for audit and debugging purposes.
        
        Args:
            df: Input DataFrame with parsed event fields.
            event_config: Configuration containing transform_key for rule lookup.
            
        Returns:
            Transformed DataFrame with data quality improvements and processing_timestamp.
        """
        transformed_df = self.transformation_loader.apply_transformations(
            df,
            event_config.transform_key
        )
        return transformed_df.withColumn("processing_timestamp", current_timestamp())
    
    @staticmethod
    def flatten_match_fields(df: DataFrame) -> DataFrame:
        """Flatten nested match event structures.
        
        Match events contain nested postmatch info structures for both players that need
        to be flattened into top-level columns for easier transformation, querying, and
        downstream consumption.
        
        Extracts fields from user-a-postmatch-info and user-b-postmatch-info nested
        structures into flattened columns with prefixes for clarity.
        
        Args:
            df: DataFrame containing match events with nested postmatch structures.
            
        Returns:
            DataFrame with flattened fields: user_a_coin_balance, user_a_level,
            user_a_device, user_a_platform_orig, and corresponding user_b fields.
        """
        return (df
            .withColumn("user_a_coin_balance", col("user-a-postmatch-info.coin-balance-after-match"))
            .withColumn("user_a_level", col("user-a-postmatch-info.level-after-match"))
            .withColumn("user_a_device", col("user-a-postmatch-info.device"))
            .withColumn("user_a_platform_orig", col("user-a-postmatch-info.platform"))
            .withColumn("user_b_coin_balance", col("user-b-postmatch-info.coin-balance-after-match"))
            .withColumn("user_b_level", col("user-b-postmatch-info.level-after-match"))
            .withColumn("user_b_device", col("user-b-postmatch-info.device"))
            .withColumn("user_b_platform_orig", col("user-b-postmatch-info.platform")))
