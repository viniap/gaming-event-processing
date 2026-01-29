"""Data Quality Package.

Provides configurable data quality transformations for the silver layer of the data
lakehouse, implementing the Template Method pattern for extensible event processing.

The package enables:
- Configuration-driven transformations via YAML rules
- Extensible transformation registry using Strategy pattern
- Stream processing from bronze to silver layer
- Dual output to Delta Lake and Kafka

Key components:
- DataQualityConfig: Configuration settings and paths
- SilverDataQualityPipeline: Main orchestrator implementing Template Method pattern
"""

from src.data_quality.core.config import DataQualityConfig
from src.data_quality.core.pipeline import SilverDataQualityPipeline

__all__ = [
    "DataQualityConfig",
    "SilverDataQualityPipeline",
]
