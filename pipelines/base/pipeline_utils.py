"""
Pipeline Utilities
==================
Shared helpers: YAML loader, secret resolver, retry decorator, logger factory.
"""
from __future__ import annotations

import functools
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

import yaml

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class PipelineUtils:
    """Static helper methods shared across all pipeline types."""

    # ── YAML Config Loader ────────────────────────────────────────────────────

    @staticmethod
    def load_yaml(path: str | Path) -> dict[str, Any]:
        """
        Load a YAML config file and resolve ${ENV_VAR} placeholders.

        Args:
            path: Absolute or relative path to the YAML file.

        Returns:
            Parsed dict with environment variable substitutions applied.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with path.open() as f:
            raw = f.read()

        resolved = PipelineUtils.resolve_env_vars(raw)
        config = yaml.safe_load(resolved)
        logger.debug("Loaded config from %s", path)
        return config or {}

    @staticmethod
    def resolve_env_vars(text: str) -> str:
        """
        Replace ${VAR_NAME} placeholders with environment variable values.
        Unresolved placeholders are left as-is (empty string substitution for safety).
        """
        pattern = re.compile(r"\$\{([^}]+)\}")
        def replacer(match: re.Match) -> str:
            key = match.group(1)
            value = os.getenv(key, "")
            if not value:
                logger.warning("Environment variable not set: %s", key)
            return value
        return pattern.sub(replacer, text)

    # ── Secret Resolver ───────────────────────────────────────────────────────

    @staticmethod
    def get_secret(key: str, default: str = "") -> str:
        """
        Retrieve a secret from environment variables.
        In production, extend this to call HashiCorp Vault or AWS Secrets Manager.
        """
        value = os.getenv(key, default)
        if not value:
            logger.warning("Secret not found for key: %s", key)
        return value

    # ── Retry Decorator ───────────────────────────────────────────────────────

    @staticmethod
    def retry(
        max_attempts: int = 3,
        delay_seconds: float = 1.0,
        exponential_backoff: bool = True,
        exceptions: tuple[type[Exception], ...] = (Exception,),
    ) -> Callable[[F], F]:
        """
        Decorator that retries a function on specified exceptions.

        Args:
            max_attempts: Maximum number of attempts (including the first).
            delay_seconds: Base delay between retries.
            exponential_backoff: If True, delay doubles each attempt.
            exceptions: Tuple of exception types to catch and retry.
        """
        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                last_exc: Exception | None = None
                for attempt in range(1, max_attempts + 1):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as exc:
                        last_exc = exc
                        if attempt == max_attempts:
                            logger.error(
                                "Function %s failed after %d attempts: %s",
                                func.__name__,
                                max_attempts,
                                exc,
                            )
                            raise
                        wait = delay_seconds * (2 ** (attempt - 1)) if exponential_backoff else delay_seconds
                        logger.warning(
                            "Attempt %d/%d for %s failed: %s — retrying in %.1fs",
                            attempt,
                            max_attempts,
                            func.__name__,
                            exc,
                            wait,
                        )
                        time.sleep(wait)
                raise last_exc  # type: ignore[misc]
            return wrapper  # type: ignore[return-value]
        return decorator

    # ── Logger Factory ────────────────────────────────────────────────────────

    @staticmethod
    def get_logger(name: str, level: str | None = None) -> logging.Logger:
        """
        Create a structured logger for a pipeline component.

        Args:
            name: Logger name (typically __name__).
            level: Log level string (DEBUG/INFO/WARNING/ERROR). Defaults to LOG_LEVEL env var.
        """
        log_level = level or os.getenv("LOG_LEVEL", "INFO")
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)

        module_logger = logging.getLogger(name)
        if not module_logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
            handler.setFormatter(formatter)
            module_logger.addHandler(handler)
        module_logger.setLevel(numeric_level)
        return module_logger

    # ── Misc ──────────────────────────────────────────────────────────────────

    @staticmethod
    def deep_merge(base: dict, override: dict) -> dict:
        """Recursively merge override into base dict."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = PipelineUtils.deep_merge(result[key], value)
            else:
                result[key] = value
        return result

