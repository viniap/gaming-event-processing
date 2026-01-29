"""
Schema validation utilities for gaming events.

Provides JSON schema loading and validation for all event types.
"""

import json
import os
from typing import Any, Dict

from jsonschema import ValidationError, validate

from src.common.logger import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class SchemaLoader:
    """Loads and caches JSON schemas for event validation.

    Provides centralized schema management for validating gaming events against
    their JSON schemas. Schemas are loaded once at initialization and cached
    for efficient repeated validation.

    Supports three event types:
    - init: Player opens the game
    - match: Match completion between two players
    - in-app-purchase: Player makes a purchase

    Attributes:
        schema_dir: Absolute path to the directory containing schema files.

    Example:
        >>> loader = SchemaLoader("schemas")
        >>> event = {"event-type": "init", "user-id": "user_001"}
        >>> is_valid = loader.validate_event(event)
        >>> if not is_valid:
        ...     print("Invalid event")

    Note:
        Relative schema_dir paths are automatically resolved relative to the
        project root directory.
    """

    def __init__(self, schema_dir: str):
        """Initialize the schema loader and load all schemas.

        Resolves the schema directory path (converting relative paths to absolute)
        and loads all required schema files into memory for fast validation.

        Args:
            schema_dir: Directory containing JSON schema files.
                Can be absolute or relative path. Relative paths are resolved
                from the project root (two directories up from this file).

        Raises:
            FileNotFoundError: If schema directory or required schema files don't exist.
            JSONDecodeError: If schema files contain invalid JSON.

        Note:
            Expected schema files: init.json, match.json, in-app-purchase.json.
            All three files must exist in the schema directory.
        """
        if not os.path.isabs(schema_dir):
            if os.path.exists(schema_dir):
                schema_dir = os.path.abspath(schema_dir)
            else:
                current_dir = os.path.dirname(os.path.abspath(__file__))
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
                schema_dir = os.path.join(project_root, schema_dir)

        self.schema_dir = schema_dir
        self._schemas: Dict[str, dict] = {}

        logger.info("Schema directory resolved to: %s", self.schema_dir)
        self._load_schemas()

    def _load_schemas(self):
        """Load all JSON schemas from the schema directory into memory.

        Reads schema files for all supported event types and stores them in
        the internal cache. This method is called automatically during
        initialization.

        Raises:
            FileNotFoundError: If any required schema file is missing.
            JSONDecodeError: If any schema file contains invalid JSON.

        Note:
            Required schema files:
            - init.json: Schema for init events
            - match.json: Schema for match events
            - in-app-purchase.json: Schema for purchase events
        """
        schema_files = {
            "init": "init.json",
            "match": "match.json",
            "in-app-purchase": "in-app-purchase.json",
        }

        for event_type, filename in schema_files.items():
            schema_path = os.path.join(self.schema_dir, filename)
            try:
                with open(schema_path, "r", encoding="utf-8") as f:
                    self._schemas[event_type] = json.load(f)
                logger.info("Loaded schema for event type: %s", event_type)
            except Exception as exc:
                logger.error(
                    "Failed to load schema: %s", filename, extra={"error": str(exc)}
                )
                raise

    def get_schema(self, event_type: str) -> dict:
        """Get the JSON schema for a specific event type.

        Retrieves the cached schema dictionary for validation. Schemas are
        loaded once during initialization for fast repeated access.

        Args:
            event_type: Type of event to get schema for.
                Must be one of: "init", "match", "in-app-purchase".

        Returns:
            JSON schema dictionary conforming to JSON Schema specification.
            Can be used with jsonschema.validate().

        Raises:
            KeyError: If no schema exists for the specified event type.
                This indicates an unsupported event type.

        Example:
            >>> schema = loader.get_schema("init")
            >>> assert "properties" in schema
            >>> assert "required" in schema
        """
        if event_type not in self._schemas:
            raise KeyError(f"No schema found for event type: {event_type}")
        return self._schemas[event_type]

    def validate_event(self, event: Dict[str, Any]) -> bool:
        """Validate an event against its JSON schema.

        Extracts the event type from the event and validates it against the
        corresponding schema. Logs detailed error information if validation fails.

        Args:
            event: Event dictionary to validate. Must contain an 'event-type' field
                with value "init", "match", or "in-app-purchase".

        Returns:
            True if the event is valid and conforms to its schema.
            False if validation fails due to:
                - Missing event-type field
                - Unknown event type
                - Schema validation failure (missing required fields, wrong types, etc.)

        Example:
            >>> event = {
            ...     "event-type": "init",
            ...     "time": 1234567890000,
            ...     "user-id": "user_001",
            ...     "country": "US",
            ...     "platform": "iOS"
            ... }
            >>> assert loader.validate_event(event)

            >>> bad_event = {"event-type": "init"}  # Missing required fields
            >>> assert not loader.validate_event(bad_event)

        Note:
            Validation errors are logged with detailed information about what failed.
            This method does not raise exceptions; it returns False on any error.
        """
        event_type = event.get("event-type")
        if not event_type:
            logger.error("Event missing 'event-type' field")
            return False

        try:
            schema = self.get_schema(event_type)
            validate(instance=event, schema=schema)
            return True
        except ValidationError as exc:
            logger.error(
                "Schema validation failed for %s",
                event_type,
                extra={"error": str(exc), "event": event},
            )
            return False
        except KeyError:
            logger.error("Unknown event type: %s", event_type)
            return False
