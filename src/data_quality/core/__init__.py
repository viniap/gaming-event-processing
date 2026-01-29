"""
Core Package.

Contains configuration and main pipeline orchestration.
"""

from src.data_quality.core.config import DataQualityConfig
from src.data_quality.core.pipeline import SilverDataQualityPipeline

__all__ = [
    "DataQualityConfig",
    "SilverDataQualityPipeline",
]
