from __future__ import annotations

from typing import Any

from airflow.models import BaseOperator


class SqlBatchOperator(BaseOperator):
    template_fields = ("config_path",)

    def __init__(self, config_path: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path

    def execute(self, context: dict) -> None:
        import yaml
        from batch_apps.sql_batch.sql_batch_app import SqlBatchApp

        logical_date = context["ds"]
        with open(self.config_path) as f:
            cfg = yaml.safe_load(f)

        sql_cfg = cfg.get("sql", cfg)
        app = SqlBatchApp(sql_cfg, logical_date=logical_date)
        app.run_etl()

