"""
Airflow DAG for Bank Customer Risk Triggers ETL Pipeline
=========================================================

Application: btot-8999-syn-api-cust-risk-tg
Dataset: cust-risk-triggers

This DAG orchestrates the complete ETL pipeline including:
- Extraction from Synapse/CSV
- Data transformation and validation
- Loading to PostgreSQL
- REST API integration
- Audit logging and monitoring

Author: Data Engineering Team
Version: 1.0.0
Date: 2026-04-12
"""

from datetime import datetime, timedelta
from typing import Any, Dict
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

# ============================================================================
# DAG Configuration
# ============================================================================

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['de-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'pool': 'etl_pool',
    'queue': 'default',
}

DAG_CONFIG = {
    'dag_id': 'btot-8999-syn-api-cust-risk-tg',
    'description': 'Bank Customer Risk Triggers ETL Pipeline',
    'schedule_interval': '0 2 * * *',  # Daily at 2 AM UTC
    'catchup': False,
    'tags': ['etl', 'bank-data', 'risk-triggers', 'production'],
    'max_active_runs': 1,
    'default_view': 'graph',
    'orientation': 'LR',
}

# ============================================================================
# Create DAG
# ============================================================================

dag = DAG(
    **DAG_CONFIG,
    default_args=DEFAULT_ARGS,
)

# ============================================================================
# Python Task Functions
# ============================================================================

def check_data_quality(**context) -> Dict[str, Any]:
    """Check PostgreSQL data quality after load."""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_bank')
        
        # Get metrics
        result = hook.get_first(sql="""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT customerLpid) as unique_customers,
                COUNT(CASE WHEN riskScore IS NULL THEN 1 END) as null_scores,
                COUNT(CASE WHEN riskIndicator = 'HIGH' THEN 1 END) as high_risk
            FROM cust_risk_triggers
            WHERE processDate = %s
        """, parameters=[context['ds']])
        
        if result:
            metrics = {
                'total_records': result[0],
                'unique_customers': result[1],
                'null_scores': result[2],
                'high_risk_count': result[3],
            }
            logger.info(f"Data Quality Check: {metrics}")
            
            # Push to XCom for downstream tasks
            context['task_instance'].xcom_push(
                key='data_quality_metrics',
                value=metrics
            )
            
            # Validate thresholds
            if metrics['null_scores'] > 0:
                logger.warning(f"Found {metrics['null_scores']} null risk scores")
            
            return metrics
        else:
            raise AirflowException("No data found for quality check")
            
    except Exception as e:
        logger.error(f"Data quality check failed: {e}")
        raise


def generate_quality_report(**context) -> str:
    """Generate data quality report."""
    try:
        metrics = context['task_instance'].xcom_pull(
            task_ids='check_data_quality',
            key='data_quality_metrics'
        )
        
        report = f"""
        Data Quality Report
        ===================
        Execution Date: {context['ds']}
        
        Metrics:
        - Total Records: {metrics['total_records']}
        - Unique Customers: {metrics['unique_customers']}
        - Null Risk Scores: {metrics['null_scores']}
        - High Risk Count: {metrics['high_risk_count']}
        
        Quality: {'PASSED' if metrics['null_scores'] == 0 else 'FAILED'}
        """
        
        logger.info(report)
        return report
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        raise


def send_alert(message: str, severity: str = 'INFO') -> None:
    """Send alert to monitoring system."""
    logger.info(f"[{severity}] {message}")
    # Integration with Slack, PagerDuty, etc.


def on_success_callback(context: Dict[str, Any]) -> None:
    """Callback on successful DAG completion."""
    logger.info(f"DAG {context['dag'].dag_id} completed successfully")
    send_alert(
        f"ETL Pipeline {context['dag'].dag_id} completed successfully for {context['ds']}",
        severity='INFO'
    )


def on_failure_callback(context: Dict[str, Any]) -> None:
    """Callback on DAG failure."""
    logger.error(f"DAG {context['dag'].dag_id} failed")
    send_alert(
        f"ETL Pipeline {context['dag'].dag_id} FAILED for {context['ds']}. "
        f"Exception: {context.get('exception', 'Unknown')}",
        severity='ERROR'
    )


# ============================================================================
# Tasks
# ============================================================================

# Start task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Validate environment
validate_env = BashOperator(
    task_id='validate_environment',
    bash_command="""
    echo "Checking environment..."
    echo "SPARK_HOME: $SPARK_HOME"
    echo "PYTHONPATH: $PYTHONPATH"
    echo "JAVA_HOME: $JAVA_HOME"
    
    python -c "import pyspark; print(f'PySpark Version: {pyspark.__version__}')"
    python -c "import yaml; print('YAML module: OK')"
    
    if [ -f "pipelines/configs/bank_cust_risk_triggers_config.yaml" ]; then
        echo "Configuration file: OK"
    else
        echo "Configuration file: NOT FOUND"
        exit 1
    fi
    """,
    dag=dag,
)

# Extract data
extract = BashOperator(
    task_id='extract_data',
    bash_command="""
    cd /workspace
    python batch_apps/spark_batch/bank_cust_risk_triggers_etl.py \
      --config pipelines/configs/bank_cust_risk_triggers_config.yaml \
      --date {{ ds }} \
      --mode normal
    """,
    environment={
        'PYTHONPATH': '/workspace/batch_apps:/workspace/pipelines',
        'SPARK_HOME': '/opt/spark',
    },
    dag=dag,
)

# Check data quality
quality_check = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)

# Generate quality report
quality_report = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    provide_context=True,
    dag=dag,
)

# Validate database integrity
db_integrity = PostgresOperator(
    task_id='validate_db_integrity',
    postgres_conn_id='postgres_bank',
    sql="""
    BEGIN;
    
    -- Check for orphaned records
    WITH orphaned AS (
        SELECT COUNT(*) as orphan_count
        FROM cust_risk_triggers
        WHERE customerLpid IS NULL OR transactionDate IS NULL
    )
    SELECT CASE 
        WHEN orphan_count > 0 THEN 
            RAISE EXCEPTION 'Found % orphaned records', orphan_count
        ELSE 
            'Integrity check passed'
    END as result
    FROM orphaned;
    
    -- Validate unique constraints
    WITH duplicates AS (
        SELECT customerLpid, transactionDate, COUNT(*) as dup_count
        FROM cust_risk_triggers
        WHERE processDate = %s
        GROUP BY customerLpid, transactionDate
        HAVING COUNT(*) > 1
    )
    SELECT CASE
        WHEN COUNT(*) > 0 THEN
            RAISE WARNING 'Found % duplicate key groups', COUNT(*)
        ELSE
            'No duplicates found'
    END as result
    FROM duplicates;
    
    COMMIT;
    """,
    parameters=[lambda: '{{ ds }}'],
    dag=dag,
)

# Archive old data
archive_data = PostgresOperator(
    task_id='archive_old_data',
    postgres_conn_id='postgres_bank',
    sql="""
    -- Archive records older than 90 days (optional)
    -- INSERT INTO cust_risk_triggers_archive
    -- SELECT * FROM cust_risk_triggers
    -- WHERE processDate < CURRENT_DATE - INTERVAL '90 days';
    
    -- For now, just cleanup audit logs
    DELETE FROM data_quality_log
    WHERE createdAt < CURRENT_TIMESTAMP - INTERVAL '180 days';
    
    SELECT 'Data archival completed' as status;
    """,
    dag=dag,
)

# Generate metrics report
metrics_report = BashOperator(
    task_id='generate_metrics_report',
    bash_command="""
    psql postgresql://postgres:postgres@postgres-bank:5432/bank_db <<EOF
    
    -- Daily metrics report
    \echo '=== Daily Risk Triggers Report ==='
    SELECT * FROM v_daily_risk_summary WHERE processDate = '{{ ds }}';
    
    \echo '=== High Risk Customers ==='
    SELECT * FROM v_high_risk_customers LIMIT 10;
    
    \echo '=== Pipeline Execution Stats ==='
    SELECT * FROM pipeline_execution_log 
    WHERE executionDate = '{{ ds }}'
    ORDER BY executionStartTime DESC;
    
    \echo 'Report generated successfully'
    
    EOF
    """,
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# ============================================================================
# Task Dependencies
# ============================================================================

# Parallel branches
start >> validate_env

validate_env >> extract >> quality_check >> quality_report

quality_check >> db_integrity >> archive_data

[quality_report, archive_data] >> metrics_report >> end

# Set callbacks
dag.on_success_callback = on_success_callback
dag.on_failure_callback = on_failure_callback

# ============================================================================
# DAG Configuration
# ============================================================================

if __name__ == "__main__":
    logger.info(f"DAG Configuration:")
    logger.info(f"  DAG ID: {dag.dag_id}")
    logger.info(f"  Schedule: {dag.schedule_interval}")
    logger.info(f"  Timezone: {dag.timezone}")
    logger.info(f"  Start Date: {dag.start_date}")
    logger.info(f"  Tasks: {len(dag.tasks)}")

