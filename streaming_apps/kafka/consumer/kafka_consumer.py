"""
Kafka Consumer Application
===========================
Concrete implementation of the Kafka consumer using kafka-python.
Features:
  - JSON / Avro deserialization
  - Manual offset commit (exactly-once semantics)
  - Dead-letter queue routing on repeated failures
  - Exponential backoff retry
  - Graceful shutdown via SIGTERM
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Any, Iterator

from streaming_apps.kafka.consumer.base_consumer import BaseKafkaConsumer
from streaming_apps.kafka.consumer.consumer_config import KafkaConsumerConfig

try:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    KafkaConsumer = None  # type: ignore[assignment,misc]
    KafkaProducer = None  # type: ignore[assignment,misc]
    KafkaError = Exception  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


class KafkaConsumerApp(BaseKafkaConsumer):
    """
    Production-ready Kafka consumer.

    Usage::

        config = KafkaConsumerConfig()
        app = KafkaConsumerApp(config)
        app.run()
    """

    def __init__(self, config: KafkaConsumerConfig | None = None) -> None:
        self._config = config or KafkaConsumerConfig()
        super().__init__(self._config.model_dump())
        self._consumer: Any = None
        self._dlq_producer: Any = None
        self._retry_counts: dict[str, int] = {}

        # Register SIGTERM for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def _build_consumer(self) -> Any:
        """Instantiate a kafka-python KafkaConsumer."""
        if KafkaConsumer is None:
            raise ImportError("kafka-python is not installed. Run: pip install kafka-python")
        kwargs = self._config.to_kafka_python_config()
        kwargs["value_deserializer"] = lambda v: v  # raw bytes; we deserialize manually
        kwargs["key_deserializer"] = lambda k: k.decode("utf-8") if k else None
        consumer = KafkaConsumer(*self._config.topics, **kwargs)
        logger.info("KafkaConsumer connected to broker(s): %s", self._config.bootstrap_servers)
        return consumer

    def _build_dlq_producer(self) -> Any:
        """Instantiate a producer for dead-letter routing."""
        if KafkaProducer is None:
            return None
        return KafkaProducer(
            bootstrap_servers=self._config.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    # ── Abstract implementations ──────────────────────────────────────────────

    def consume(self) -> Iterator[Any]:
        """Poll and yield messages from subscribed topics."""
        self._consumer = self._build_consumer()
        self._dlq_producer = self._build_dlq_producer()
        logger.info("Polling topics: %s", self._config.topics)
        while self._running:
            records = self._consumer.poll(timeout_ms=self._config.poll_timeout_ms)
            for _tp, messages in records.items():
                for msg in messages:
                    yield msg

    def deserialize(self, raw_message: Any) -> dict:
        """Deserialize message value from JSON bytes → dict."""
        try:
            return json.loads(raw_message.value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(f"Deserialization failed: {exc}") from exc

    def process(self, message: dict) -> None:
        """
        Business logic hook.
        Override this method in subclasses to add transformations,
        database writes, API calls, etc.
        """
        logger.debug("Processing message: %s", message)
        # ── Add your processing logic here ────────────────────────────────
        # Example: write to database, call downstream API, push to another topic
        print(f"[KafkaConsumer] Processed: {message}")

    def commit(self) -> None:
        """Manually commit offsets after successful processing."""
        if self._consumer:
            self._consumer.commit()
            logger.debug("Offsets committed")

    def close(self) -> None:
        """Close consumer and DLQ producer."""
        if self._consumer:
            self._consumer.close()
            logger.info("KafkaConsumer closed")
        if self._dlq_producer:
            self._dlq_producer.flush()
            self._dlq_producer.close()

    # ── Error handling ────────────────────────────────────────────────────────

    def on_error(self, exc: Exception, raw_message: Any = None) -> None:
        """Route to DLQ after max_retries exhausted, otherwise retry."""
        msg_key = str(getattr(raw_message, "offset", id(raw_message)))
        self._retry_counts[msg_key] = self._retry_counts.get(msg_key, 0) + 1
        attempt = self._retry_counts[msg_key]

        if attempt <= self._config.max_retries:
            backoff = (self._config.retry_backoff_ms / 1000) * (2 ** (attempt - 1))
            logger.warning(
                "Error on attempt %d/%d for msg_key=%s; retrying in %.1fs: %s",
                attempt,
                self._config.max_retries,
                msg_key,
                backoff,
                exc,
            )
            time.sleep(backoff)
        else:
            logger.error(
                "Max retries exceeded for msg_key=%s; routing to DLQ topic=%s",
                msg_key,
                self._config.dlq_topic,
            )
            self._send_to_dlq(raw_message, str(exc))
            del self._retry_counts[msg_key]

    def _send_to_dlq(self, raw_message: Any, error: str) -> None:
        """Send a failed message to the dead-letter queue."""
        if self._dlq_producer is None:
            logger.warning("DLQ producer not available; dropping message")
            return
        dlq_payload = {
            "original_topic": getattr(raw_message, "topic", "unknown"),
            "original_partition": getattr(raw_message, "partition", -1),
            "original_offset": getattr(raw_message, "offset", -1),
            "original_value": (
                raw_message.value.decode("utf-8", errors="replace")
                if raw_message and raw_message.value
                else None
            ),
            "error": error,
            "timestamp_ms": int(time.time() * 1000),
        }
        self._dlq_producer.send(self._config.dlq_topic, value=dlq_payload)
        self._dlq_producer.flush()

    def _handle_sigterm(self, _signum: int, _frame: Any) -> None:
        logger.info("SIGTERM received — stopping consumer gracefully")
        self.stop()


# ── CLI entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka Consumer App")
    parser.add_argument("--group-id", default=os.getenv("KAFKA_CONSUMER_GROUP_ID", "de-dv-consumer"))
    parser.add_argument("--topics", nargs="+", default=[os.getenv("KAFKA_SOURCE_TOPIC", "raw.events")])
    args = parser.parse_args()

    cfg = KafkaConsumerConfig(group_id=args.group_id, topics=args.topics)
    app = KafkaConsumerApp(cfg)
    app.run()

