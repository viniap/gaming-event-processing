"""Abstract base factory for event creation.

Defines the interface for all event factories using the Factory Method pattern.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class EventFactory(ABC):
    """Abstract factory for creating gaming events.

    Defines the interface for event creation. Concrete factories implement
    the create() method to generate specific event types.

    This implements the Factory Method pattern, allowing subclasses to decide
    which type of event to create while maintaining a consistent interface.
    """

    @abstractmethod
    def create(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Create an event with the given context.

        Args:
            context: Dictionary containing necessary data for event creation.
                Required keys depend on the specific event type:
                - user_ids: List of available user IDs
                - countries: List of country codes
                - platforms: List of platform names
                - devices: List of device models (for match events)
                - product_ids: List of product IDs (for purchase events)
                - product_prices: Dict mapping product IDs to prices (for purchase)

        Returns:
            Dictionary containing the generated event.
        """
