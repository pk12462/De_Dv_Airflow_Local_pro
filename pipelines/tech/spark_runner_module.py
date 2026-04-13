from __future__ import annotations


def get_runner_commands() -> list[str]:
    """Runner/test commands used while validating pipeline config updates."""
    return [
        'Set-Location "C:\\Users\\PAVAN\\Local_pro\\De_Dv_Airflow_Local_pro"',
        "python -m pytest -o addopts=\"\" tests/test_local_batch_streaming_runner.py -q",
        "python -m pytest -o addopts=\"\" tests/test_connection_manager.py tests/test_local_batch_streaming_runner.py -q",
        "python -c \"import json,glob; files=glob.glob('etl-project/appconfig/dev/pipeline_*_pg_cassandra.json'); [json.load(open(f,encoding='utf-8-sig')) for f in files]; print('validated',len(files),'pipeline json files')\"",
    ]

