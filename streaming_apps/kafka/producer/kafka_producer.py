"""
Kafka Producer Application
===========================
Concrete producer with async batching, JSON serialization,
delivery callbacks, and graceful shutdown.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from streaming_apps.kafka.producer.base_producer import BaseKafkaProducer
from streaming_apps.kafka.producer.producer_config import KafkaProducerConfig

try:
    from kafka import KafkaProducer as _KafkaProducer
except ImportError:
    _KafkaProducer = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


class KafkaProducerApp(BaseKafkaProducer):
    """
    Production-ready Kafka producer.

    Usage::

        config = KafkaProducerConfig()
        producer = KafkaProducerApp(config)
        producer.send(key="user-123", value={"event": "login", "ts": 1234567890})
        producer.flush()
        producer.close()
    """

    def __init__(self, config: KafkaProducerConfig | None = None) -> None:
        self._config = config or KafkaProducerConfig()
        super().__init__(self._config.model_dump())
        self._producer: Any = None
        self._connect()

    def _connect(self) -> None:
        """Initialize the underlying kafka-python producer."""
        if _KafkaProducer is None:
            raise ImportError("kafka-python is not installed. Run: pip install kafka-python")
        kwargs = self._config.to_kafka_python_config()
        kwargs["value_serializer"] = self.serialize
        kwargs["key_serializer"] = lambda k: k.encode("utf-8") if k else None
        self._producer = _KafkaProducer(**kwargs)
        logger.info(
            "KafkaProducer connected to broker(s): %s | topic: %s",
            self._config.bootstrap_servers,
            self._config.sink_topic,
        )

    # ── Abstract implementations ──────────────────────────────────────────────

    def serialize(self, value: Any) -> bytes:
        """Serialize value to JSON bytes."""
        try:
            return json.dumps(value, default=str).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Serialization failed for value={value}: {exc}") from exc

    def send(self, key: str | None, value: Any, topic: str | None = None) -> None:
        """
        Asynchronously send a message to Kafka.

        Args:
            key: Partition key (used for partitioning strategy)
            value: Message payload (dict, str, list, etc.)
            topic: Override the default sink topic
        """
        target_topic = topic or self._config.sink_topic
        try:
            future = self._producer.send(topic=target_topic, key=key, value=value)
            future.add_callback(self._on_success, topic=target_topic)
            future.add_errback(self._on_failure, topic=target_topic)
            logger.debug("Message enqueued for topic=%s key=%s", target_topic, key)
        except Exception as exc:
            logger.error("Failed to enqueue message for topic=%s: %s", target_topic, exc)
            raise

    def send_batch(self, messages: list[dict], topic: str | None = None) -> None:
        """
        Send a batch of messages. Each item should be a dict with optional 'key' and required 'value'.
        """
        target_topic = topic or self._config.sink_topic
        for msg in messages:
            self.send(key=msg.get("key"), value=msg["value"], topic=target_topic)
        self.flush()
        logger.info("Batch of %d messages sent to topic=%s", len(messages), target_topic)

    def flush(self) -> None:
        """Block until all buffered messages are delivered."""
        if self._producer:
            self._producer.flush()
            logger.debug("Producer flush complete")

    def close(self) -> None:
        """Flush remaining messages and close the producer."""
        self.flush()
        if self._producer:
            self._producer.close()
            logger.info("KafkaProducer closed")

    # ── Delivery callbacks ────────────────────────────────────────────────────

    def _on_success(self, record_metadata: Any, topic: str = "") -> None:
        logger.debug(
            "Delivered to topic=%s partition=%d offset=%d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def _on_failure(self, exc: Exception, topic: str = "") -> None:
        logger.error("Delivery FAILED for topic=%s: %s", topic, exc)


# ── CLI entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka Producer App — sends sample events")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()

    cfg = KafkaProducerConfig()
    app = KafkaProducerApp(cfg)
    for i in range(args.count):
        payload = {"event_id": i, "event_type": "sample", "ts": int(time.time() * 1000)}
        app.send(key=f"key-{i}", value=payload, topic=args.topic)
        time.sleep(0.1)
    app.flush()
    app.close()
    print(f"Sent {args.count} messages successfully.")

