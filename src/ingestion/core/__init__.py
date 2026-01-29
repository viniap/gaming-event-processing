"""Core ingestion components - base classes and configuration."""

from src.ingestion.core.base import BronzeIngestionJob, ConfigurableBronzeIngestion
from src.ingestion.core.config import BronzeIngestionConfig

__all__ = [
    "BronzeIngestionJob",
    "ConfigurableBronzeIngestion",
    "BronzeIngestionConfig",
]
