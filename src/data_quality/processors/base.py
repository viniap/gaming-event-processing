"""
Base Classes and Configuration for Event Processing.

Defines the EventTypeConfig dataclass that encapsulates all parameters
needed to process an event type from bronze to silver.
"""

from dataclasses import dataclass
from typing import Callable, Optional

from pyspark.sql import DataFrame


@dataclass
class EventTypeConfig:
    """Configuration for processing a specific event type.
    
    Encapsulates all parameters required to process an event type from bronze to silver
    layer, following data-driven design principles. This configuration enables the
    template method pattern by providing event-specific parameters to the generic
    processing workflow.
    
    Attributes:
        event_type: Human-readable event type name (e.g., "init", "match", "purchase").
        event_filter: Value to filter on in the event-type column of bronze table.
        silver_path: Delta Lake path for writing silver layer events.
        checkpoint_path: Checkpoint location for maintaining streaming state.
        transform_key: Key for looking up transformation rules in YAML files.
        preprocess_fn: Optional preprocessing function to apply before transformations.
    """
    event_type: str
    event_filter: str
    silver_path: str
    checkpoint_path: str
    transform_key: str
    preprocess_fn: Optional[Callable[[DataFrame], DataFrame]] = None
