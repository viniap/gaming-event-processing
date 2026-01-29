"""Core application components.

Provides the main application orchestrator and configuration classes
for the event producer.
"""

from src.producer.core.app import EventProducerApp
from src.producer.core.config import ProducerConfig

__all__ = ["EventProducerApp", "ProducerConfig"]
