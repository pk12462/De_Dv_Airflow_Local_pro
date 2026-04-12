"""
Consumer Configuration
======================
Pydantic model for Kafka consumer settings.
Loaded from environment variables or a YAML config file.
"""
from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field, field_validator
import os


class KafkaConsumerConfig(BaseModel):
    """Kafka consumer configuration model."""

    # ── Broker ────────────────────────────────────────────────────────────────
    bootstrap_servers: str = Field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        description="Comma-separated list of broker host:port pairs",
    )

    # ── Consumer Group ────────────────────────────────────────────────────────
    group_id: str = Field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP_ID", "de-dv-consumer-group"),
        description="Consumer group ID for offset coordination",
    )

    # ── Topics ────────────────────────────────────────────────────────────────
    topics: List[str] = Field(
        default_factory=lambda: [os.getenv("KAFKA_SOURCE_TOPIC", "raw.events")],
        description="List of topics to subscribe to",
    )
    dlq_topic: str = Field(
        default_factory=lambda: os.getenv("KAFKA_DLQ_TOPIC", "dead.letter.events"),
        description="Dead-letter queue topic for failed messages",
    )

    # ── Offset Policy ─────────────────────────────────────────────────────────
    auto_offset_reset: str = Field(
        default="earliest",
        description="What to do when there is no initial offset: earliest | latest | none",
    )
    enable_auto_commit: bool = Field(
        default=False,
        description="Disable auto-commit; use manual commits for exactly-once semantics",
    )

    # ── Security ──────────────────────────────────────────────────────────────
    security_protocol: str = Field(
        default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        description="PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL",
    )
    sasl_mechanism: Optional[str] = Field(
        default_factory=lambda: os.getenv("KAFKA_SASL_MECHANISM"),
        description="PLAIN | GSSAPI | OAUTHBEARER | SCRAM-SHA-256 | SCRAM-SHA-512",
    )
    sasl_plain_username: Optional[str] = Field(
        default_factory=lambda: os.getenv("KAFKA_SASL_USERNAME"),
    )
    sasl_plain_password: Optional[str] = Field(
        default_factory=lambda: os.getenv("KAFKA_SASL_PASSWORD"),
    )

    # ── Schema Registry ───────────────────────────────────────────────────────
    schema_registry_url: Optional[str] = Field(
        default_factory=lambda: os.getenv("KAFKA_SCHEMA_REGISTRY_URL"),
        description="Confluent Schema Registry URL for Avro deserialization",
    )

    # ── Retry ─────────────────────────────────────────────────────────────────
    max_retries: int = Field(default=3, ge=0, description="Max retry attempts on processing error")
    retry_backoff_ms: int = Field(default=1000, ge=0, description="Backoff between retries in ms")

    # ── Poll ──────────────────────────────────────────────────────────────────
    poll_timeout_ms: int = Field(default=1000, ge=0, description="Max time to block on poll")
    max_poll_records: int = Field(default=500, ge=1, description="Max records per poll")

    @field_validator("auto_offset_reset")
    @classmethod
    def validate_offset_reset(cls, v: str) -> str:
        allowed = {"earliest", "latest", "none"}
        if v not in allowed:
            raise ValueError(f"auto_offset_reset must be one of {allowed}")
        return v

    def to_kafka_python_config(self) -> dict:
        """Convert to kafka-python consumer kwargs."""
        cfg: dict = {
            "bootstrap_servers": self.bootstrap_servers.split(","),
            "group_id": self.group_id,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": self.enable_auto_commit,
            "security_protocol": self.security_protocol,
            "max_poll_records": self.max_poll_records,
        }
        if self.sasl_mechanism:
            cfg["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_plain_username:
            cfg["sasl_plain_username"] = self.sasl_plain_username
        if self.sasl_plain_password:
            cfg["sasl_plain_password"] = self.sasl_plain_password
        return cfg

