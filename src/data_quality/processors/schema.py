"""
Event Schema Definitions.

Provides Spark schema definitions for parsing event data from bronze layer.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


class EventSchemaProvider:
    """Provides Spark schema definitions for parsing event data.
    
    Centralizes event schema definitions used for parsing JSON payloads from the
    bronze layer. The schema supports all event types (init, match, purchase) with
    optional fields, enabling efficient JSON parsing with proper type inference.
    """
    
    @staticmethod
    def get_event_schema() -> StructType:
        """Get a unified schema for parsing all event types.
        
        Returns a flexible schema that accommodates init, match, and purchase events
        with their respective fields. All fields are optional (nullable=True) to handle
        the different structures of each event type gracefully.
        
        Schema includes:
        - Common fields: event-type, time, user-id, country, platform
        - Match-specific: user-a, user-b, winner, game-tier, duration, postmatch-info
        - Purchase-specific: purchase_value, product-id
        
        Returns:
            StructType schema compatible with from_json() for parsing event JSON.
        """
        return StructType([
            StructField("event-type", StringType(), True),
            StructField("time", LongType(), True),
            StructField("user-id", StringType(), True),
            StructField("country", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("user-a", StringType(), True),
            StructField("user-b", StringType(), True),
            StructField("winner", StringType(), True),
            StructField("game-tier", IntegerType(), True),
            StructField("duration", IntegerType(), True),
            StructField("purchase_value", DoubleType(), True),
            StructField("product-id", StringType(), True),
            StructField("user-a-postmatch-info", StructType([
                StructField("coin-balance-after-match", IntegerType(), True),
                StructField("level-after-match", IntegerType(), True),
                StructField("device", StringType(), True),
                StructField("platform", StringType(), True)
            ]), True),
            StructField("user-b-postmatch-info", StructType([
                StructField("coin-balance-after-match", IntegerType(), True),
                StructField("level-after-match", IntegerType(), True),
                StructField("device", StringType(), True),
                StructField("platform", StringType(), True)
            ]), True)
        ])
