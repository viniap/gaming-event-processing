"""Bronze layer ingestion package for raw event ingestion from Kafka to Delta Lake.

The package uses a unified ingestion approach where a single streaming job subscribes
to multiple Kafka topics to prevent Delta Lake concurrent write conflicts.

Main components:
- BronzeEventIngestion: Unified ingestion class (in storage/bronze_writer.py)
- BronzeIngestionConfig: Configuration with multi-topic support

Deprecated components (kept for backward compatibility):
- BronzeIngestionJob: Template Method base class
- ConfigurableBronzeIngestion: Old single-topic implementation
- BronzeIngestionJobBuilder: Builder pattern (no longer used)
"""

from src.ingestion.core.base import BronzeIngestionJob, ConfigurableBronzeIngestion
from src.ingestion.core.config import BronzeIngestionConfig
from src.ingestion.builders.job_builder import BronzeIngestionJobBuilder
from src.ingestion.storage.bronze_writer import BronzeEventIngestion

__all__ = [
    "BronzeEventIngestion",  # Current unified implementation
    "BronzeIngestionConfig",
    # Deprecated but kept for backward compatibility:
    "BronzeIngestionJob",
    "ConfigurableBronzeIngestion",
    "BronzeIngestionJobBuilder",
]
