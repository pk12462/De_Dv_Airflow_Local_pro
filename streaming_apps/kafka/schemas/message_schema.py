"""
Message Schema Definitions
===========================
Avro and JSON schema definitions for Kafka messages.
Provides a SchemaRegistry client wrapper for schema validation.
"""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Avro Schema Definitions ───────────────────────────────────────────────────

RAW_EVENT_SCHEMA = {
    "type": "record",
    "name": "RawEvent",
    "namespace": "com.dedv.events",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "source_system", "type": "string"},
        {"name": "timestamp_ms", "type": "long"},
        {"name": "payload", "type": {"type": "map", "values": "string"}},
        {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None},
    ],
}

PROCESSED_EVENT_SCHEMA = {
    "type": "record",
    "name": "ProcessedEvent",
    "namespace": "com.dedv.events",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "source_system", "type": "string"},
        {"name": "timestamp_ms", "type": "long"},
        {"name": "processed_at_ms", "type": "long"},
        {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["SUCCESS", "FAILED", "SKIPPED"]}},
        {"name": "payload", "type": {"type": "map", "values": "string"}},
    ],
}

DLQ_EVENT_SCHEMA = {
    "type": "record",
    "name": "DlqEvent",
    "namespace": "com.dedv.events",
    "fields": [
        {"name": "original_topic", "type": "string"},
        {"name": "original_partition", "type": "int"},
        {"name": "original_offset", "type": "long"},
        {"name": "original_value", "type": ["null", "string"], "default": None},
        {"name": "error", "type": "string"},
        {"name": "timestamp_ms", "type": "long"},
    ],
}

SCHEMA_REGISTRY: dict[str, dict] = {
    "raw.events": RAW_EVENT_SCHEMA,
    "processed.events": PROCESSED_EVENT_SCHEMA,
    "dead.letter.events": DLQ_EVENT_SCHEMA,
}


class SchemaRegistry:
    """
    Local schema registry for validation.
    Can be extended to connect to Confluent Schema Registry HTTP API.
    """

    def __init__(self, registry: dict[str, dict] | None = None) -> None:
        self._registry = registry or SCHEMA_REGISTRY
        logger.info("SchemaRegistry initialized with %d schemas", len(self._registry))

    def get_schema(self, topic: str) -> dict | None:
        """Return the Avro schema dict for a given topic."""
        schema = self._registry.get(topic)
        if schema is None:
            logger.warning("No schema registered for topic=%s", topic)
        return schema

    def register_schema(self, topic: str, schema: dict) -> None:
        """Register a new schema for a topic."""
        self._registry[topic] = schema
        logger.info("Schema registered for topic=%s", topic)

    def validate(self, topic: str, message: dict) -> bool:
        """
        Validate message fields against the registered schema.
        Returns True if valid, raises ValueError if invalid.
        """
        schema = self.get_schema(topic)
        if schema is None:
            logger.warning("Skipping validation — no schema for topic=%s", topic)
            return True

        required_fields = {f["name"] for f in schema.get("fields", [])}
        missing = required_fields - set(message.keys())
        if missing:
            raise ValueError(f"Message missing required fields {missing} for topic={topic}")

        logger.debug("Message validated for topic=%s", topic)
        return True

    def list_topics(self) -> list[str]:
        """Return all registered topic names."""
        return list(self._registry.keys())

