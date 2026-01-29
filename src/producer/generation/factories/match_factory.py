"""Factory for creating match events."""

import random
import time
from typing import Any, Dict

from src.producer.generation.factories.base import EventFactory


class MatchEventFactory(EventFactory):
    """Factory for creating match events.

    Match events represent completed games between two players with
    comprehensive post-match statistics.
    """

    def create(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Create a match event.

        Args:
            context: Must contain 'user_ids', 'platforms', and 'devices'.

        Returns:
            Match event dictionary with both players' data and match outcome.
        """
        user_a, user_b = random.sample(context["user_ids"], 2)
        winner = random.choice([user_a, user_b])

        user_a_info = {
            "coin-balance-after-match": random.randint(0, 100000),
            "level-after-match": random.randint(1, 100),
            "device": random.choice(context["devices"]),
            "platform": random.choice(context["platforms"]),
        }

        user_b_info = {
            "coin-balance-after-match": random.randint(0, 100000),
            "level-after-match": random.randint(1, 100),
            "device": random.choice(context["devices"]),
            "platform": random.choice(context["platforms"]),
        }

        return {
            "event-type": "match",
            "time": int(time.time() * 1000),
            "user-a": user_a,
            "user-b": user_b,
            "user-a-postmatch-info": user_a_info,
            "user-b-postmatch-info": user_b_info,
            "winner": winner,
            "game-tier": random.randint(1, 5),
            "duration": random.randint(30, 600),
        }
