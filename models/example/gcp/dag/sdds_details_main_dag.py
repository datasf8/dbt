from datetime import timedelta
from airflow import DAG
import logging
from sdds_details_config import dag_config
from common import dag_config as config
from common.main_template_dag import get_dag_details
JOB_DETAILS = dag_config.JOB_DETAILS
DESTINATION_PATH = dag_config.DESTINATION_PATH


default_args = {
    'owner': 'airflow',
    'depends_on_past': config.depends_on_past,
    'start_date': config.start_date,
    'email': config.email,
    'email_on_failure': config.email_on_failure,
    'email_on_retry': config.email_on_retry,
    'retries': config.retries,
    "scheduler_zombie_task_threshold": timedelta(hours=5),
    'retry_delay': config.retry_delay,
    'execution_timeout': timedelta(hours=config.execution_timeout),
    "dbt_cloud_conn_id": config.DBT_CLOUD_CONN_ID,
    "account_id": dag_config.DBT_CLOUD_ACCOUNT_ID
}

task = None
try:
    DAG_DETAILS = {
        "DATABASE": dag_config.DATABASE,
        "stage_database": dag_config.stage_database,
        "SNOWFLAKE_STAGE_NAME": dag_config.SNOWFLAKE_STAGE_NAME,
        "MAIN_URL": dag_config.MAIN_URL,
        "DBT_CLOUD_JOB_ID": dag_config.DBT_CLOUD_JOB_ID
    }
    dag = DAG(
            JOB_DETAILS.get("JOB_NAME"),
            default_args=default_args,
            description=JOB_DETAILS.get("JOB_DESC"),
            max_active_runs=config.max_active_runs,
            concurrency=config.concurrency,
            tags=JOB_DETAILS.get("tags") if JOB_DETAILS.get("tags") else None,
            catchup=config.catchup,
            schedule_interval=dag_config.schedule_interval,
    )
    task = get_dag_details(dag, JOB_DETAILS, DAG_DETAILS, DESTINATION_PATH)
except IndexError as ex:
    logging.debug("Exception", str(ex))
