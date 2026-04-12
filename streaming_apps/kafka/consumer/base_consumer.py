"""
Base Kafka Consumer
===================
Abstract base class defining the Kafka consumer contract.
All concrete consumers must implement the abstract methods.
"""
from __future__ import annotations

import abc
import logging
from typing import Any, Iterator

logger = logging.getLogger(__name__)


class BaseKafkaConsumer(abc.ABC):
    """
    Abstract base class for Kafka consumers.

    Subclasses must implement:
        - consume(): Generator that yields raw messages
        - deserialize(raw_message): Deserialize raw bytes → domain object
        - process(message): Apply business logic to a deserialized message
        - commit(): Commit offsets after successful processing
        - close(): Release all resources
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self._running = False
        logger.info("BaseKafkaConsumer initialized with config keys: %s", list(config.keys()))

    @abc.abstractmethod
    def consume(self) -> Iterator[Any]:
        """Continuously poll and yield raw messages from Kafka."""

    @abc.abstractmethod
    def deserialize(self, raw_message: bytes) -> Any:
        """Deserialize raw bytes into a domain object."""

    @abc.abstractmethod
    def process(self, message: Any) -> None:
        """Apply business logic to a deserialized message."""

    @abc.abstractmethod
    def commit(self) -> None:
        """Commit offsets after successful processing."""

    @abc.abstractmethod
    def close(self) -> None:
        """Close the consumer and release resources."""

    def on_error(self, exc: Exception, raw_message: Any = None) -> None:
        """
        Error handler called when processing fails.
        Override to implement custom DLQ routing or alerting.
        """
        logger.error(
            "Consumer error processing message=%s: %s",
            raw_message,
            exc,
            exc_info=True,
        )

    def run(self) -> None:
        """
        Main consume loop — calls consume(), deserialize(), process(), commit()
        in order. Handles errors via on_error() and always calls close().
        """
        self._running = True
        logger.info("Starting consumer loop")
        try:
            for raw_msg in self.consume():
                if not self._running:
                    break
                try:
                    message = self.deserialize(raw_msg)
                    self.process(message)
                    self.commit()
                except Exception as exc:  # noqa: BLE001
                    self.on_error(exc, raw_msg)
        finally:
            self.close()
            logger.info("Consumer loop stopped")

    def stop(self) -> None:
        """Signal the consume loop to exit gracefully."""
        self._running = False
        logger.info("Stop signal sent to consumer")

