"""Kafka integration components.

Provides Kafka producer wrapper and builder for publishing gaming events
to Kafka topics.
"""

from src.producer.kafka.producer import GameEventProducer
from src.producer.kafka.producer_builder import GameEventProducerBuilder

__all__ = ["GameEventProducer", "GameEventProducerBuilder"]
