"""Event Producer Module.

This module provides a complete event production system for the game.
It generates realistic gaming events (init, match, in-app-purchase) using Faker,
validates them against JSON schemas, and publishes them to Kafka topics.

Main Components:
    EventProducerApp: Main application orchestrating event generation and publishing
    EventGenerator: Generates realistic gaming events with proper data distributions
    GameEventProducer: Kafka producer wrapper with topic routing and error handling
    SchemaLoader: Loads and validates events against JSON schemas
    ProducerConfig: Configuration management using Pydantic

Example:
    >>> from src.producer.main import EventProducerApp
    >>> app = EventProducerApp()
    >>> app.run()
"""

from src.producer.core import EventProducerApp, ProducerConfig
from src.producer.generation import EventGenerator
from src.producer.kafka import GameEventProducer, GameEventProducerBuilder
from src.producer.validation import SchemaLoader

__all__ = [
    "EventProducerApp",
    "ProducerConfig",
    "EventGenerator",
    "GameEventProducer",
    "GameEventProducerBuilder",
    "SchemaLoader",
]
