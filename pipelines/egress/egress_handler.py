"""
Egress Handler
==============
Enforces egress rules at pipeline runtime.
Blocks unauthorized outbound destinations and logs violations.
"""
from __future__ import annotations

import fnmatch
import logging
import os
from pathlib import Path
from typing import Any

from pipelines.base.pipeline_utils import PipelineUtils

logger = logging.getLogger(__name__)

DEFAULT_RULES_PATH = Path(__file__).parent / "egress_rules.yaml"


class EgressHandler:
    """
    Runtime enforcement of egress rules from egress_rules.yaml.

    Usage::

        handler = EgressHandler()
        handler.check("kafka-spark-streaming-pipeline", "kafka", 9092)
        # Raises EgressViolationError if blocked
    """

    def __init__(self, rules_path: str | Path | None = None) -> None:
        path = Path(rules_path or os.getenv("EGRESS_CONFIG_PATH", str(DEFAULT_RULES_PATH)))
        self._config = PipelineUtils.load_yaml(path)
        self._rules: list[dict[str, Any]] = self._config.get("rules", [])
        self._blocked: list[dict[str, Any]] = self._config.get("blocked", [])
        self._default_policy: str = self._config.get("default_policy", "deny")
        self._violation_action: str = self._config.get(
            "violation_handling", {}
        ).get("action", "log_and_block")
        logger.info("EgressHandler loaded %d rules (default_policy=%s)", len(self._rules), self._default_policy)

    def check(self, pipeline_name: str, host: str, port: int) -> None:
        """
        Check if an outbound connection is permitted.

        Args:
            pipeline_name: Name of the calling pipeline.
            host: Destination hostname or IP.
            port: Destination port.

        Raises:
            EgressViolationError: If the destination is blocked.
        """
        for rule in self._rules:
            if self._matches_rule(rule, pipeline_name, host, port):
                logger.debug(
                    "Egress ALLOWED by rule '%s': pipeline=%s host=%s port=%d",
                    rule["name"],
                    pipeline_name,
                    host,
                    port,
                )
                return

        # No matching allow rule found
        self._handle_violation(pipeline_name, host, port)

    def _matches_rule(self, rule: dict, pipeline_name: str, host: str, port: int) -> bool:
        """Check if a rule permits the given connection."""
        # Pipeline filter
        allowed_pipelines = rule.get("allowed_pipelines", [])
        if "*" not in allowed_pipelines and pipeline_name not in allowed_pipelines:
            return False

        # Host filter
        dest_hosts = rule.get("destination", {}).get("hosts", [])
        host_matched = any(fnmatch.fnmatch(host, h) or h == host for h in dest_hosts)
        if not host_matched:
            return False

        # Port filter
        allowed_ports = rule.get("destination", {}).get("ports", [])
        if allowed_ports and port not in allowed_ports:
            return False

        return True

    def _handle_violation(self, pipeline_name: str, host: str, port: int) -> None:
        """Log and optionally block an egress violation."""
        msg = (
            f"Egress BLOCKED: pipeline={pipeline_name} attempted connection to "
            f"host={host}:{port} which is not in the allowed egress rules."
        )
        if self._violation_action in ("log_and_block", "alert_and_block"):
            logger.error(msg)
            raise EgressViolationError(msg)
        else:
            logger.warning(msg)

    def list_allowed_hosts(self, pipeline_name: str) -> list[str]:
        """Return all hosts allowed for a given pipeline."""
        hosts: list[str] = []
        for rule in self._rules:
            pipelines = rule.get("allowed_pipelines", [])
            if "*" in pipelines or pipeline_name in pipelines:
                hosts.extend(rule.get("destination", {}).get("hosts", []))
        return hosts


class EgressViolationError(RuntimeError):
    """Raised when an outbound connection violates egress rules."""

