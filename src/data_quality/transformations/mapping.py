"""
Mapping Transformation.

Maps field values from one value to another using a dictionary.
Useful for ID to name conversions, standardization, etc.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

from src.data_quality.transformations.base import Transformation


class MappingTransformation(Transformation):
    """Map field values using a dictionary lookup.
    
    Transforms values based on a mapping dictionary, commonly used for:
    - ID to name conversions (e.g., country_id to country_name)
    - Value standardization (e.g., normalizing platform names)
    - Data enrichment with lookup tables
    
    Supports source and target fields being different, enabling field creation
    while preserving original values.
    """
    
    def apply(self, df: DataFrame, field: str, params: Dict[str, Any]) -> DataFrame:
        """Apply value mapping to a field using dictionary lookup.
        
        Builds a Spark when-otherwise expression chain for efficient mapping.
        Supports optional default values for unmapped entries.
        
        Args:
            df: Input DataFrame to transform.
            field: Target field name where mapped values will be stored.
            params: Transformation parameters containing:
                - mapping: Dict[str, str] - Value mapping dictionary (key -> value)
                - source_field: str - Source field name (defaults to target field)
                - default: str - Optional default value for unmapped entries
            
        Returns:
            DataFrame with mapped field added or updated.
            
        Example:
            params = {
                'mapping': {'US': 'United States', 'UK': 'United Kingdom'},
                'source_field': 'country_code',
                'default': 'Unknown'
            }
        """
        mapping = params.get('mapping', {})
        source_field = params.get('source_field', field)
        default_value = params.get('default', None)
        
        mapping_expr = None
        for key, value in mapping.items():
            condition = col(source_field) == lit(key)
            if mapping_expr is None:
                mapping_expr = when(condition, lit(value))
            else:
                mapping_expr = mapping_expr.when(condition, lit(value))
        
        if default_value:
            mapping_expr = mapping_expr.otherwise(lit(default_value))
        elif source_field != field:
            mapping_expr = mapping_expr.otherwise(lit(default_value) if default_value else lit(None))
        else:
            mapping_expr = mapping_expr.otherwise(col(source_field))
        
        return df.withColumn(field, mapping_expr)
    
    def get_name(self) -> str:
        """Return transformation name."""
        return "mapping"
