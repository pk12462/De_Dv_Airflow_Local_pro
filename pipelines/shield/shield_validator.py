"""
Shield Validator
================
Applies masking/validation rules to dict and DataFrame payloads before egress.
"""
from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Any

from pipelines.base.pipeline_utils import PipelineUtils

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path(__file__).parent / "shield_values.yaml"


class ShieldValidator:
    """Applies shield policy rules from `shield_values.yaml`."""

    def __init__(self, config_path: str | None = None, environment: str = "development") -> None:
        cfg_path = config_path or str(DEFAULT_CONFIG_PATH)
        self.config = PipelineUtils.load_yaml(cfg_path)
        self.environment = environment
        self.fields = {f["name"]: f for f in self.config.get("fields", [])}
        self.enforcement = self.config.get("enforcement", {})

    def should_apply(self) -> bool:
        apply_envs = set(self.enforcement.get("apply_in_environments", []))
        skip_envs = set(self.enforcement.get("skip_in_environments", []))
        if self.environment in skip_envs:
            return False
        return self.environment in apply_envs

    def mask_record(self, record: dict[str, Any]) -> dict[str, Any]:
        if not self.should_apply():
            return record
        masked = dict(record)
        for key, value in record.items():
            if key in self.fields:
                masked[key] = self._apply_strategy(value, self.fields[key])
        return masked

    def mask_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self.mask_record(r) for r in records]

    def mask_pandas_df(self, df: Any) -> Any:
        """Mask pandas DataFrame columns if pandas is available."""
        if not self.should_apply():
            return df
        for col, rule in self.fields.items():
            if col in getattr(df, "columns", []):
                df[col] = df[col].apply(lambda v: self._apply_strategy(v, rule))
        return df

    def _apply_strategy(self, value: Any, rule: dict[str, Any]) -> Any:
        if value is None:
            return None
        strategy = rule.get("strategy", "log_only")
        value_str = str(value)

        if strategy == "hash":
            algo = rule.get("hash_algorithm", "sha256").lower()
            h = hashlib.new(algo)
            h.update(value_str.encode("utf-8"))
            return h.hexdigest()

        if strategy == "redact":
            return rule.get("redact_with", "[REDACTED]")

        if strategy == "tokenize":
            prefix = rule.get("token_prefix", "TOK_")
            preserve_last_n = int(rule.get("preserve_last_n", 0))
            if preserve_last_n > 0 and len(value_str) > preserve_last_n:
                return f"{prefix}{'X' * (len(value_str) - preserve_last_n)}{value_str[-preserve_last_n:]}"
            digest = hashlib.sha1(value_str.encode("utf-8")).hexdigest()[:12]
            return f"{prefix}{digest}"

        if strategy == "truncate":
            keep = int(rule.get("keep_chars", 0))
            if "keep_octets" in rule and re.match(r"^\d+\.\d+\.\d+\.\d+$", value_str):
                octets = value_str.split(".")
                keep_octets = int(rule.get("keep_octets", 3))
                return ".".join(octets[:keep_octets] + ["xxx"] * max(0, 4 - keep_octets))
            if keep >= len(value_str):
                return value_str
            pad = rule.get("pad_char", "*")
            return value_str[:keep] + (pad * (len(value_str) - keep))

        if strategy == "generalize":
            granularity = rule.get("granularity", "year")
            if granularity == "year" and len(value_str) >= 4:
                return value_str[:4]
            return value_str

        return value

