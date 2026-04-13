from airflow.plugins_manager import AirflowPlugin

from operators.file_watcher_operator import FileWatcherOperator
from operators.holiday_calendar_operator import HolidayCalendarOperator
from operators.pipeline_runner_operator import PipelineRunnerOperator
from operators.target_load_check_operator import TargetLoadCheckOperator


class DeDvPlugin(AirflowPlugin):
    name = "de_dv_plugin"
    operators = [
        HolidayCalendarOperator,
        FileWatcherOperator,
        PipelineRunnerOperator,
        TargetLoadCheckOperator,
    ]

