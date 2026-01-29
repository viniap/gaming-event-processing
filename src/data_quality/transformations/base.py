"""
Base Transformation Interface.

Defines the abstract base class for all data quality transformations
following the Strategy design pattern.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict
from pyspark.sql import DataFrame


class Transformation(ABC):
    """Abstract base class for data quality transformations.
    
    Defines the interface for all transformation implementations using the Strategy
    design pattern. Each transformation is a pluggable strategy that can be applied
    to DataFrame fields based on YAML configuration.
    
    All concrete transformation classes must implement the apply() and get_name() methods.
    Transformations are registered in TransformationRegistry for runtime lookup.
    """
    
    @abstractmethod
    def apply(self, df: DataFrame, field: str, params: Dict[str, Any]) -> DataFrame:
        """Apply transformation to a DataFrame field.
        
        Args:
            df: Input DataFrame to transform.
            field: Name of the target field (output field name).
            params: Transformation-specific parameters from YAML rules, which may include
                source_field for input-output field mapping and other configuration.
            
        Returns:
            Transformed DataFrame with the specified field modified or added.
        """
    
    @abstractmethod
    def get_name(self) -> str:
        """Return the unique identifier name of this transformation.
        
        The name is used for:
        - Registration in TransformationRegistry
        - Lookup from YAML transformation rules
        - Logging and debugging
        
        Returns:
            Transformation name (e.g., 'uppercase', 'mapping').
        """
    
    def __str__(self) -> str:
        """String representation of the transformation."""
        return f"{self.__class__.__name__}(name={self.get_name()})"
