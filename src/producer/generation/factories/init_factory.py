"""Factory for creating init events."""

import random
import time
from typing import Any, Dict

from src.producer.generation.factories.base import EventFactory


class InitEventFactory(EventFactory):
    """Factory for creating init events.

    Init events represent a player opening the game and contain basic
    player location and platform information.
    """

    def create(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Create an init event.

        Args:
            context: Must contain 'user_ids', 'countries', and 'platforms'.

        Returns:
            Init event dictionary with event-type, time, user-id, country, platform.
        """
        return {
            "event-type": "init",
            "time": int(time.time() * 1000),
            "user-id": random.choice(context["user_ids"]),
            "country": random.choice(context["countries"]),
            "platform": random.choice(context["platforms"]),
        }
