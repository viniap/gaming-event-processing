"""Event factory implementations using the Factory Method pattern.

This module provides concrete factory classes for creating different types
of gaming events. Each factory is responsible for creating one event type.

Exports:
    EventFactory: Abstract base class for all factories
    InitEventFactory: Creates init events (player opens game)
    MatchEventFactory: Creates match events (completed matches)
    PurchaseEventFactory: Creates purchase events (in-app purchases)
"""

from src.producer.generation.factories.base import EventFactory
from src.producer.generation.factories.init_factory import InitEventFactory
from src.producer.generation.factories.match_factory import MatchEventFactory
from src.producer.generation.factories.purchase_factory import PurchaseEventFactory

__all__ = [
    "EventFactory",
    "InitEventFactory",
    "MatchEventFactory",
    "PurchaseEventFactory",
]
