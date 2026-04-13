from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator


class HolidayCalendarOperator(BaseOperator):
    template_fields = ("holidays_file",)

    def __init__(self, holidays_file: str, calendar_key: str = "IN", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.holidays_file = holidays_file
        self.calendar_key = calendar_key

    def execute(self, context: dict[str, Any]) -> bool:
        logical_day = self._resolve_logical_day(context)
        holidays = self._load_holidays(Path(self.holidays_file), self.calendar_key)

        if logical_day in holidays:
            raise AirflowSkipException(f"{logical_day} is a public holiday in calendar={self.calendar_key}")

        self.log.info("%s is not a holiday in calendar=%s", logical_day, self.calendar_key)
        return True

    @staticmethod
    def _resolve_logical_day(context: dict[str, Any]) -> str:
        logical_date = context.get("logical_date")
        if logical_date is None:
            return date.today().isoformat()
        return logical_date.date().isoformat()

    @staticmethod
    def _load_holidays(path: Path, calendar_key: str) -> set[str]:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        if isinstance(payload, dict):
            values = payload.get(calendar_key, [])
        elif isinstance(payload, list):
            values = payload
        else:
            values = []

        return {str(v) for v in values}

