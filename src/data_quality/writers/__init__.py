"""
Writers Package.

Handles output writing to Delta Lake and Kafka.
"""

from src.data_quality.writers.delta_writer import DeltaWriter
from src.data_quality.writers.kafka_writer import KafkaWriter

__all__ = [
    "DeltaWriter",
    "KafkaWriter",
]
