"""
Kafka Producer wrapper for publishing gaming events.

Provides a high-level interface for publishing events to Kafka with
proper error handling, retries, and monitoring.
"""

import json
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer

from src.common.logger import StructuredLogger, log_exception

logger = StructuredLogger.get_logger(__name__)


class GameEventProducer:
    """Kafka producer wrapper for publishing gaming events with automatic topic routing.

    Provides a high-level interface for publishing gaming events to Kafka with:
    - Automatic topic routing based on event-type field
    - Comprehensive error handling with callbacks
    - Automatic retries for transient failures
    - Configurable compression and acknowledgment modes
    - Built-in statistics tracking

    The producer routes events to different topics based on the event-type field:
    - "init" events → init_topic
    - "match" events → match_topic
    - "in-app-purchase" events → purchase_topic

    Attributes:
        topic_mapping: Dictionary mapping event types to Kafka topic names.
        producer: Underlying kafka-python KafkaProducer instance.

    Example:
        >>> topic_mapping = {
        ...     "init": "init_events",
        ...     "match": "match_events",
        ...     "in-app-purchase": "purchase_events"
        ... }
        >>> producer = GameEventProducer(
        ...     bootstrap_servers="localhost:29092",
        ...     topic_mapping=topic_mapping
        ... )
        >>> event = {"event-type": "init", "user-id": "user_000001"}
        >>> producer.publish_event(event)
        >>> producer.close()

    Note:
        For host-to-container communication, use localhost:29092.
        For inter-container communication, use kafka:9092.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic_mapping: Dict[str, str],
        *,
        acks: str = "all",
        retries: int = 3,
        compression_type: str = "snappy",
    ):
        """Initialize the Kafka producer with connection and routing configuration.

        Creates a KafkaProducer instance with optimized settings for reliable
        event delivery. Automatically verifies connectivity by fetching metadata
        for all configured topics.

        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses.
                Format: "host1:port1,host2:port2". Use localhost:29092 for
                host access or kafka:9092 for inter-container communication.
            topic_mapping: Dictionary mapping event type strings to topic names.
                Must include mappings for all event types that will be published.
                Example: {"init": "init_events", "match": "match_events",
                         "in-app-purchase": "purchase_events"}
            acks: Kafka acknowledgment mode controlling durability guarantees:
                - "all": Wait for all in-sync replicas (highest durability)
                - "1": Wait for leader acknowledgment only (balanced)
                - "0": No acknowledgment (lowest latency, lowest durability)
            retries: Number of automatic retry attempts for failed sends.
                Transient errors like network issues are retried automatically.
            compression_type: Message compression algorithm to reduce bandwidth.
                Options: "gzip", "snappy", "lz4", "zstd", or None for no compression.
                "snappy" offers good balance of speed and compression.

        Raises:
            KafkaError: If connection to Kafka brokers fails or configuration is invalid.

        Note:
            Connection verification fetches metadata for each topic in topic_mapping.
            Topics are created automatically if they don't exist (with default settings).
        """
        self.topic_mapping = topic_mapping
        
        logger.info(
            "Initializing Kafka producer",
            extra={
                "bootstrap_servers": bootstrap_servers,
                "topic_mapping": topic_mapping,
            },
        )
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=acks,
                retries=retries,
                compression_type=compression_type,
                max_in_flight_requests_per_connection=5,
                request_timeout_ms=10000,
                retry_backoff_ms=100,
                api_version_auto_timeout_ms=5000,
                metadata_max_age_ms=300000,
                max_block_ms=10000,
            )
            
            logger.info("Verifying Kafka connection...")
            for topic in topic_mapping.values():
                try:
                    self.producer.partitions_for(topic)
                    logger.info("Topic accessible: %s", topic)
                except Exception:  # pylint: disable=broad-exception-caught
                    logger.warning("Topic %s not yet available", topic)
            
        except Exception as exc:
            logger.error("Failed to initialize Kafka producer: %s", exc)
            raise

        logger.info(
            "Kafka producer initialized successfully",
            extra={
                "bootstrap_servers": bootstrap_servers,
                "topic_mapping": topic_mapping,
                "acks": acks,
                "compression": compression_type,
            },
        )

        self._success_count = 0
        self._error_count = 0

    def _on_send_success(self, record_metadata):
        """Handle successful message delivery callback.

        Increments the success counter and logs progress every 1000 successful sends
        to provide visibility into producer throughput without excessive logging.

        Args:
            record_metadata: Kafka RecordMetadata containing:
                - topic: The topic the message was sent to
                - partition: The partition number
                - offset: The offset assigned to the message

        Note:
            This callback is invoked asynchronously by the Kafka producer.
        """
        self._success_count += 1
        if self._success_count % 1000 == 0:
            logger.info(
                "Successfully sent %d events",
                self._success_count,
                extra={
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset,
                },
            )

    def _on_send_error(self, exception):
        """Handle failed message delivery callback.

        Increments the error counter and logs the exception with context.
        This is called after all automatic retries have been exhausted.

        Args:
            exception: The exception that caused the send failure.
                Common exceptions include KafkaTimeoutError, NotLeaderForPartitionError.

        Note:
            This callback is invoked asynchronously by the Kafka producer.
            The producer has already retried based on the configured retry count.
        """
        self._error_count += 1
        log_exception(logger, exception, {"error_count": self._error_count})

    def publish_event(self, event: Dict[str, Any], topic: Optional[str] = None) -> bool:
        """Publish a single event to Kafka with automatic topic routing.

        Routes the event to the appropriate topic based on its event-type field.
        If a topic is explicitly provided, it overrides the automatic routing.
        The send is asynchronous with callbacks for success/failure tracking.

        Args:
            event: Event dictionary to publish. Must contain an 'event-type' field
                with a hyphen (e.g., "in-app-purchase") unless topic is specified.
            topic: Optional topic name to override automatic routing.
                If provided, the event is sent to this topic regardless of event-type.

        Returns:
            True if the event was successfully queued for sending, False if:
                - Event is missing the event-type field (when topic not specified)
                - No topic mapping exists for the event type
                - An exception occurred during send

        Note:
            Returning True means the event was queued, not that it was delivered.
            Check callbacks or call get_stats() for actual delivery status.

        Example:
            >>> event = {"event-type": "init", "user-id": "user_001"}
            >>> success = producer.publish_event(event)
            >>> if success:
            ...     producer.flush()  # Ensure delivery
        """
        if topic:
            target_topic = topic
        else:
            event_type = event.get("event-type")
            if not event_type:
                logger.error("Event missing event-type field", extra={"event": event})
                return False

            target_topic = self.topic_mapping.get(event_type)
            if not target_topic:
                logger.error(
                    "No topic mapping for event type",
                    extra={
                        "event_type": event_type,
                        "available_mappings": list(self.topic_mapping.keys()),
                    },
                )
                return False

        try:
            future = self.producer.send(target_topic, value=event)
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            return True
        except Exception as exc:  # pylint: disable=broad-exception-caught
            log_exception(logger, exc, {"event": event, "topic": target_topic})
            return False

    def publish_batch(
        self, events: List[Dict[str, Any]], topic: Optional[str] = None
    ) -> int:
        """Publish a batch of events to Kafka efficiently.

        Publishes multiple events and flushes to ensure they are all sent before
        returning. If topic is not specified, each event is routed to its
        appropriate topic based on its event-type field.

        Args:
            events: List of event dictionaries to publish. Each event must contain
                an 'event-type' field if topic is not specified.
            topic: Optional topic name to send all events to.
                If provided, automatic routing is bypassed and all events go to this topic.

        Returns:
            Number of events successfully queued for sending. May be less than
            len(events) if some events failed validation or routing.

        Example:
            >>> events = [
            ...     {"event-type": "init", "user-id": "user_001"},
            ...     {"event-type": "match", "user-a": "user_001", "user-b": "user_002"},
            ...     {"event-type": "in-app-purchase", "user-id": "user_001"}
            ... ]
            >>> queued = producer.publish_batch(events)
            >>> assert queued == 3

        Note:
            This method calls flush() automatically to ensure all events are sent
            before returning, which may block briefly.
        """
        success_count = 0

        for event in events:
            if self.publish_event(event, topic):
                success_count += 1

        self.producer.flush()

        return success_count

    def flush(self):
        """Flush all buffered messages and wait for delivery confirmation.

        Blocks until all messages in the producer's buffer have been sent to
        Kafka brokers and acknowledged according to the acks configuration.
        This ensures all queued events are actually delivered.

        Note:
            This method can block for several seconds if many messages are buffered
            or if network latency is high. Use sparingly in high-throughput scenarios.

        Example:
            >>> producer.publish_event(event1)
            >>> producer.publish_event(event2)
            >>> producer.flush()  # Ensure both are delivered
        """
        self.producer.flush()
        logger.info("Producer flushed")

    def close(self):
        """Close the Kafka producer and release all resources.

        Flushes any pending messages, closes connections to Kafka brokers,
        and logs final statistics. This method should always be called before
        the application exits to ensure no messages are lost.

        Example:
            >>> producer = GameEventProducer("localhost:29092", topic_mapping)
            >>> try:
            ...     producer.publish_event(event)
            ... finally:
            ...     producer.close()  # Always close in finally block

        Note:
            After calling close(), this producer instance cannot be reused.
            Create a new instance if you need to send more events.
        """
        self.producer.close()
        logger.info(
            "Producer closed",
            extra={
                "total_success": self._success_count,
                "total_errors": self._error_count,
            },
        )

    def get_stats(self) -> Dict[str, int]:
        """Get producer delivery statistics.

        Returns a snapshot of the producer's current statistics including
        successful and failed message deliveries. Useful for monitoring
        producer health and throughput.

        Returns:
            Dictionary containing:
                - success_count: Total number of successfully delivered messages
                - error_count: Total number of failed deliveries (after all retries)

        Example:
            >>> stats = producer.get_stats()
            >>> success_rate = stats['success_count'] / (
            ...     stats['success_count'] + stats['error_count']
            ... )
            >>> print(f"Success rate: {success_rate:.2%}")
        """
        return {"success_count": self._success_count, "error_count": self._error_count}
