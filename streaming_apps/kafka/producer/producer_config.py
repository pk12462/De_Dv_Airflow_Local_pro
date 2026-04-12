"""
Producer Configuration
======================
Pydantic model for Kafka producer settings.
"""
from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field
import os


class KafkaProducerConfig(BaseModel):
    """Kafka producer configuration model."""

    # ── Broker ────────────────────────────────────────────────────────────────
    bootstrap_servers: str = Field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )

    # ── Target Topic ──────────────────────────────────────────────────────────
    sink_topic: str = Field(
        default_factory=lambda: os.getenv("KAFKA_SINK_TOPIC", "processed.events"),
    )

    # ── Reliability ───────────────────────────────────────────────────────────
    acks: str = Field(
        default="all",
        description="all | 0 | 1 — 'all' for strongest durability guarantee",
    )
    retries: int = Field(default=3, ge=0)
    retry_backoff_ms: int = Field(default=200, ge=0)

    # ── Batching & Throughput ─────────────────────────────────────────────────
    batch_size: int = Field(default=16384, description="Batch size in bytes")
    linger_ms: int = Field(default=5, description="Wait time before flushing a partial batch")
    buffer_memory: int = Field(default=33554432, description="Total memory for buffering in bytes")

    # ── Compression ───────────────────────────────────────────────────────────
    compression_type: str = Field(
        default="snappy",
        description="none | gzip | snappy | lz4 | zstd",
    )

    # ── Security ──────────────────────────────────────────────────────────────
    security_protocol: str = Field(
        default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    )
    sasl_mechanism: Optional[str] = Field(
        default_factory=lambda: os.getenv("KAFKA_SASL_MECHANISM"),
    )
    sasl_plain_username: Optional[str] = Field(
        default_factory=lambda: os.getenv("KAFKA_SASL_USERNAME"),
    )
    sasl_plain_password: Optional[str] = Field(
        default_factory=lambda: os.getenv("KAFKA_SASL_PASSWORD"),
    )

    def to_kafka_python_config(self) -> dict:
        """Convert to kafka-python producer kwargs."""
        cfg: dict = {
            "bootstrap_servers": self.bootstrap_servers.split(","),
            "acks": self.acks,
            "retries": self.retries,
            "retry_backoff_ms": self.retry_backoff_ms,
            "batch_size": self.batch_size,
            "linger_ms": self.linger_ms,
            "buffer_memory": self.buffer_memory,
            "compression_type": self.compression_type,
            "security_protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            cfg["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_plain_username:
            cfg["sasl_plain_username"] = self.sasl_plain_username
        if self.sasl_plain_password:
            cfg["sasl_plain_password"] = self.sasl_plain_password
        return cfg

