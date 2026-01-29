"""
Uppercase Transformation.

Converts string field values to uppercase.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import upper, col

from src.data_quality.transformations.base import Transformation


class UppercaseTransformation(Transformation):
    """Transform string field values to uppercase.
    
    Simple transformation for standardizing string fields to uppercase,
    commonly used for platform identifiers, codes, and other fields
    where case-insensitive matching is desired.
    """
    
    def apply(self, df: DataFrame, field: str, params: Dict[str, Any]) -> DataFrame:
        """Convert field to uppercase.
        
        Args:
            df: Input DataFrame to transform.
            field: Target field name where uppercase value will be stored.
            params: Transformation parameters containing:
                - source_field: str - Source field name (defaults to target field)
            
        Returns:
            DataFrame with uppercase field added or updated.
        """
        source_field = params.get('source_field', field)
        return df.withColumn(field, upper(col(source_field)))
    
    def get_name(self) -> str:
        """Return transformation name."""
        return "uppercase"
