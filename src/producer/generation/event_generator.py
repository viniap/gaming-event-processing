"""Event Generator orchestrating event creation using Factory Method pattern.

Coordinates event generation using specialized factory classes. The generator
maintains context and delegates event creation to appropriate factories.
"""

import random
from typing import Any, Dict, List

from faker import Faker

from src.common.logger import StructuredLogger
from src.producer.generation.factories import (
    EventFactory,
    InitEventFactory,
    MatchEventFactory,
    PurchaseEventFactory,
)

logger = StructuredLogger.get_logger(__name__)


class EventGenerator:
    """Generates realistic gaming events using the Factory Method pattern.

    Orchestrates event creation using specialized factory classes for each event type.
    Maintains a pool of user IDs to simulate realistic multi-user behavior and provides
    context data for factories to create events.

    Uses the Factory Method pattern to delegate event creation to specialized factories:
    - InitEventFactory: Creates init events (player opens game)
    - MatchEventFactory: Creates match events (completed matches)
    - PurchaseEventFactory: Creates purchase events (in-app purchases)

    Class Attributes:
        COUNTRIES: List of country codes for player locations (US, UK, BR, etc.).
        PLATFORMS: Supported platforms (iOS, Android, Web).
        DEVICES: Device models for realistic device distribution.
        PRODUCT_IDS: Available in-app purchase product identifiers.
        PRODUCT_PRICES: Price mapping for each product ID.

    Attributes:
        faker: Faker instance for generating realistic data.
        users_pool_size: Number of unique users in the simulation.
        user_ids: Pre-generated list of user identifiers.
        factories: Dictionary mapping event types to their factories.

    Example:
        >>> generator = EventGenerator(users_pool_size=1000)
        >>> init_event = generator.generate_init_event()
        >>> match_event = generator.generate_match_event()
        >>> events = generator.generate_events_batch(count=10)
    """

    # Static data for realistic generation
    COUNTRIES = ["US", "UK", "BR", "JP", "DE", "FR", "IN", "CN", "CA", "AU"]
    PLATFORMS = ["iOS", "Android", "Web"]
    DEVICES = [
        "iPhone 15 Pro",
        "iPhone 14",
        "iPhone 13",
        "Samsung Galaxy S24",
        "Samsung Galaxy S23",
        "Google Pixel 8",
        "OnePlus 11",
        "iPad Pro",
        "MacBook Pro",
        "Windows PC",
        "Chrome Browser",
    ]
    PRODUCT_IDS = [
        "PROD001",
        "PROD002",
        "PROD003",
        "PROD004",
        "PROD005",
        "PROD006",
        "PROD007",
        "PROD008",
        "PROD009",
        "PROD010",
    ]
    PRODUCT_PRICES = {
        "PROD001": 0.99,
        "PROD002": 2.99,
        "PROD003": 4.99,
        "PROD004": 9.99,
        "PROD005": 19.99,
        "PROD006": 29.99,
        "PROD007": 49.99,
        "PROD008": 0.99,
        "PROD009": 4.99,
        "PROD010": 14.99,
    }

    def __init__(self, users_pool_size: int = 1000):
        """Initialize the event generator with factories and user pool.

        Creates factory instances for each event type and initializes the user pool.
        The factories are registered in a dictionary for easy lookup by event type.

        Args:
            users_pool_size: Number of unique users in the simulation pool.
                Larger pools reduce ID collision probability but use more memory.
                Default is 1000 users.

        Note:
            User IDs are formatted as 'user_XXXXXX' with zero-padded numbers.
        """
        self.faker = Faker()
        self.users_pool_size = users_pool_size
        self.user_ids = [f"user_{i:06d}" for i in range(users_pool_size)]
        
        self.factories: Dict[str, EventFactory] = {
            "init": InitEventFactory(),
            "match": MatchEventFactory(),
            "purchase": PurchaseEventFactory(),
        }
        
        logger.info("Event generator initialized with %d users", users_pool_size)

    def _get_context(self) -> Dict[str, Any]:
        """Get context dictionary for event factories.

        Provides all necessary data that factories need to create events.
        This centralizes context creation and makes it easy to extend.

        Returns:
            Dictionary containing all context data for event creation:
                - user_ids: List of user identifiers
                - countries: List of country codes
                - platforms: List of platform names
                - devices: List of device models
                - product_ids: List of product identifiers
                - product_prices: Product price mapping
        """
        return {
            "user_ids": self.user_ids,
            "countries": self.COUNTRIES,
            "platforms": self.PLATFORMS,
            "devices": self.DEVICES,
            "product_ids": self.PRODUCT_IDS,
            "product_prices": self.PRODUCT_PRICES,
        }

    def generate_init_event(self) -> Dict[str, Any]:
        """Generate an init event using the InitEventFactory.

        Init events are the first events in a player session and contain basic
        information about the player's device and location. These events are used
        to track daily active users and platform distribution.

        Returns:
            Dictionary containing the init event with the following fields:
                - event-type: Always "init"
                - time: Unix timestamp in milliseconds
                - user-id: Unique player identifier from the user pool
                - country: Two-letter country code
                - platform: Player's platform (iOS, Android, or Web)

        Example:
            >>> event = generator.generate_init_event()
            >>> print(event['event-type'])
            'init'
        """
        return self.factories["init"].create(self._get_context())

    def generate_match_event(self) -> Dict[str, Any]:
        """Generate a match event using the MatchEventFactory.

        Match events contain comprehensive data about both players including their
        post-match state (coins, level), devices, and match outcome. The winner is
        randomly selected from the two players.

        Returns:
            Dictionary containing the match event with the following fields:
                - event-type: Always "match"
                - time: Unix timestamp in milliseconds
                - user-a: First player's user ID
                - user-b: Second player's user ID (different from user-a)
                - user-a-postmatch-info: Player A's state after match
                    - coin-balance-after-match: Remaining coins
                    - level-after-match: Current level
                    - device: Device model used
                    - platform: Platform used (iOS, Android, Web)
                - user-b-postmatch-info: Player B's state (same structure as A)
                - winner: User ID of the match winner (either user-a or user-b)
                - game-tier: Match difficulty tier (1-5)
                - duration: Match duration in seconds (30-600)

        Note:
            Two different users are always selected to prevent self-matches.

        Example:
            >>> event = generator.generate_match_event()
            >>> assert event['user-a'] != event['user-b']
        """
        return self.factories["match"].create(self._get_context())

    def generate_purchase_event(self) -> Dict[str, Any]:
        """Generate an in-app purchase event using the PurchaseEventFactory.

        Purchase events track monetization and include the product purchased,
        its price, and the player who made the purchase. Product prices are
        predefined in the PRODUCT_PRICES mapping.

        Returns:
            Dictionary containing the purchase event with the following fields:
                - event-type: Always "in-app-purchase"
                - time: Unix timestamp in milliseconds
                - purchase_value: Price in USD (0.99 to 49.99)
                - user-id: Unique player identifier from the user pool
                - product-id: Product identifier (PROD001-PROD010)

        Note:
            Product prices range from $0.99 (small items) to $49.99 (premium items).

        Example:
            >>> event = generator.generate_purchase_event()
            >>> assert 0.99 <= event['purchase_value'] <= 49.99
        """
        return self.factories["purchase"].create(self._get_context())

    def generate_event(
        self,
        init_prob: float = 0.3,
        match_prob: float = 0.5,
        purchase_prob: float = 0.2,
    ) -> Dict[str, Any]:
        """Generate a random event based on probability distribution.

        Randomly selects one of the three event types based on the provided
        probability weights. Probabilities are automatically normalized to sum to 1.0,
        so they represent relative weights rather than absolute percentages.

        Args:
            init_prob: Relative weight for generating init events.
                Higher values increase init event frequency.
            match_prob: Relative weight for generating match events.
                Higher values increase match event frequency.
            purchase_prob: Relative weight for generating purchase events.
                Higher values increase purchase event frequency.

        Returns:
            Generated event dictionary. The type depends on the random selection
            and will be one of: init event, match event, or purchase event.

        Example:
            >>> event = generator.generate_event(init_prob=0.5, match_prob=0.3, purchase_prob=0.2)
            >>> event_type = event['event-type']
            >>> assert event_type in ['init', 'match', 'in-app-purchase']

        Note:
            Probabilities (0.3, 0.5, 0.2) are equivalent to (3, 5, 2) or (30, 50, 20)
            since they are normalized internally.
        """
        total = init_prob + match_prob + purchase_prob
        init_prob /= total
        match_prob /= total
        purchase_prob /= total

        rand = random.random()

        if rand < init_prob:
            return self.generate_init_event()
        if rand < init_prob + match_prob:
            return self.generate_match_event()
        return self.generate_purchase_event()

    def generate_events_batch(
        self,
        count: int,
        init_prob: float = 0.3,
        match_prob: float = 0.5,
        purchase_prob: float = 0.2,
    ) -> List[Dict[str, Any]]:
        """Generate a batch of random events.

        Creates multiple events in a single call using the specified probability
        distribution. Each event is independently generated, so the actual count
        of each event type will vary based on the random selection.

        Args:
            count: Number of events to generate in the batch.
                Must be positive.
            init_prob: Relative weight for generating init events.
            match_prob: Relative weight for generating match events.
            purchase_prob: Relative weight for generating purchase events.

        Returns:
            List of generated event dictionaries. Each element is one of:
            init event, match event, or purchase event.

        Example:
            >>> events = generator.generate_events_batch(
            ...     count=100,
            ...     init_prob=0.3,
            ...     match_prob=0.5,
            ...     purchase_prob=0.2
            ... )
            >>> assert len(events) == 100
            >>> init_count = sum(1 for e in events if e['event-type'] == 'init')
            >>> assert 15 <= init_count <= 45  # Approximately 30%
        """
        return [
            self.generate_event(init_prob, match_prob, purchase_prob)
            for _ in range(count)
        ]
