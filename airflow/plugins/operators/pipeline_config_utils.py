from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

AIRFLOW_ROOT = Path("/opt/airflow")


def load_pipeline_config(config_path: str) -> dict[str, Any]:
    path = resolve_workspace_path(config_path)
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def resolve_workspace_path(path_value: str) -> Path:
    path_value = _expand_carid(path_value)
    path = Path(path_value)
    if path.is_absolute():
        if path.exists():
            return path
        adls_candidates = _map_adls_to_local(path_value)
        for candidate in adls_candidates:
            if candidate.exists():
                return candidate
        return path
    return AIRFLOW_ROOT / path


def normalize_pipeline_config(cfg: dict[str, Any]) -> dict[str, Any]:
    if "source" in cfg and "targets" in cfg:
        return cfg

    gi = cfg.get("generalInfo", {})
    mode = str(cfg.get("mode") or gi.get("processType", "batch")).lower()
    mode = "streaming" if mode.startswith("stream") else "batch"

    output: dict[str, Any] = {
        "pipelineName": cfg.get("pipelineName") or gi.get("appName", "pipeline"),
        "mode": mode,
        "source": {},
        "targets": [],
        "transforms": [],
        "connections": cfg.get("connections", {}),
        "streaming": cfg.get("streaming", {}),
    }

    transforms = cfg.get("mainFlow", [{}])[0].get("transformationList", [])
    transforms = sorted(transforms, key=lambda t: int(t.get("generalInfo", {}).get("order", 0)))
    for t in transforms:
        t_info = t.get("generalInfo", {})
        t_props = t.get("props", {})
        t_type = str(t_info.get("transformationType", "")).lower()

        if t_type == "source" and not output["source"]:
            src_sys = str(t_props.get("srcSysName", "local_csv")).lower()
            src_props = t_props.get("srcSysProp", {})
            output["source"] = {
                "type": src_sys if src_sys.startswith("local_") else "local_csv",
                "path": src_props.get("filePath")
                or src_props.get("path")
                or src_props.get("localFallbackPath")
                or src_props.get("inputPath"),
            }
            if "delimiter" in src_props:
                output["source"]["delimiter"] = src_props["delimiter"]
            continue

        if t_type == "target":
            tgt_sys = str(t_props.get("tgtSysName", "")).lower()
            tgt_props = t_props.get("tgtSysProp", {})
            if "postgres" in tgt_sys:
                output["targets"].append(
                    {
                        "type": "postgres",
                        "table": tgt_props.get("table") or tgt_props.get("tableName") or "events",
                    }
                )
            elif "cassandra" in tgt_sys:
                output["targets"].append(
                    {
                        "type": "cassandra",
                        "keyspace": tgt_props.get("keyspace", "learning"),
                        "table": tgt_props.get("table") or tgt_props.get("tableName") or "events",
                    }
                )
    return output


def resolve_source_path(config: dict[str, Any]) -> Path:
    config = normalize_pipeline_config(config)
    source = config.get("source", {})
    source_path = source.get("path")
    if not source_path:
        raise ValueError("source.path is required in pipeline config")
    return resolve_workspace_path(str(source_path))


def _expand_carid(path_value: str) -> str:
    carid = os.getenv("CARID", "8999")
    return path_value.replace("${CARID}", carid).replace("{CARID}", carid)


def _map_adls_to_local(path_value: str) -> list[Path]:
    normalized = _expand_carid(path_value).replace("\\", "/")
    if not normalized.startswith("/mnt/"):
        return []

    parts = normalized.split("/")
    relative = parts[4:] if len(parts) > 4 and parts[3] == "inbound" else parts[3:]
    if not relative:
        return []

    local_root = AIRFLOW_ROOT / "etl-project" / "local_source"
    return [local_root.joinpath(*relative), local_root / relative[-1]]


