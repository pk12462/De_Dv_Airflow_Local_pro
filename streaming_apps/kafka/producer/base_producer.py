"""
Base Kafka Producer
===================
Abstract base class defining the Kafka producer contract.
"""
from __future__ import annotations

import abc
import logging
from typing import Any

logger = logging.getLogger(__name__)


class BaseKafkaProducer(abc.ABC):
    """
    Abstract base class for Kafka producers.

    Subclasses must implement:
        - send(key, value): Serialize and send a message
        - serialize(value): Convert domain object → bytes
        - flush(): Block until all pending messages are delivered
        - close(): Release all resources
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        logger.info("BaseKafkaProducer initialized")

    @abc.abstractmethod
    def send(self, key: str | None, value: Any, topic: str | None = None) -> None:
        """Serialize and send a single message to Kafka."""

    @abc.abstractmethod
    def serialize(self, value: Any) -> bytes:
        """Convert a domain object to bytes for transmission."""

    @abc.abstractmethod
    def flush(self) -> None:
        """Wait for all outstanding produce requests to complete."""

    @abc.abstractmethod
    def close(self) -> None:
        """Flush and close the producer."""

    def on_delivery(self, err: Any, msg: Any) -> None:
        """
        Delivery callback invoked by the producer after send.
        Override to implement custom success/failure handling.
        """
        if err:
            logger.error("Message delivery FAILED topic=%s err=%s", getattr(msg, "topic", "?"), err)
        else:
            logger.debug(
                "Message delivered topic=%s partition=%s offset=%s",
                getattr(msg, "topic", "?"),
                getattr(msg, "partition", "?"),
                getattr(msg, "offset", "?"),
            )

