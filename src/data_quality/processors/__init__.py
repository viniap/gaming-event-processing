"""
Event Processors Package.

Handles event-specific processing including schema definition,
parsing, filtering, and preprocessing.
"""

from src.data_quality.processors.base import EventTypeConfig
from src.data_quality.processors.event_processor import EventProcessor
from src.data_quality.processors.schema import EventSchemaProvider

__all__ = [
    "EventTypeConfig",
    "EventProcessor",
    "EventSchemaProvider",
]
