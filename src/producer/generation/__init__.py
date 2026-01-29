"""Event generation components.

Provides event generation orchestrator and factory pattern implementations
for creating realistic gaming events.
"""

from src.producer.generation.event_generator import EventGenerator
from src.producer.generation.factories import (
    EventFactory,
    InitEventFactory,
    MatchEventFactory,
    PurchaseEventFactory,
)

__all__ = [
    "EventGenerator",
    "EventFactory",
    "InitEventFactory",
    "MatchEventFactory",
    "PurchaseEventFactory",
]
