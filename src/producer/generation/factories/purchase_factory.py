"""Factory for creating purchase events."""

import random
import time
from typing import Any, Dict

from src.producer.generation.factories.base import EventFactory


class PurchaseEventFactory(EventFactory):
    """Factory for creating in-app purchase events.

    Purchase events track monetization with product details and pricing.
    """

    def create(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Create a purchase event.

        Args:
            context: Must contain 'user_ids', 'product_ids', and 'product_prices'.

        Returns:
            Purchase event dictionary with product and pricing information.
        """
        product_id = random.choice(context["product_ids"])
        purchase_value = context["product_prices"].get(product_id, 0.99)

        return {
            "event-type": "in-app-purchase",
            "time": int(time.time() * 1000),
            "purchase_value": purchase_value,
            "user-id": random.choice(context["user_ids"]),
            "product-id": product_id,
        }
