from airflow import DAG
from datetime import timedelta
import logging
from variable_pay_sftp_config.dag_config import DagConfig
from sftp_common import dag_config, sftp_hourly_job_template


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': DagConfig.start_date,
    'email': dag_config.email,
    'email_on_failure': dag_config.email_on_failure,
    'email_on_retry': dag_config.email_on_retry,
    'retries': DagConfig.retries,
    'retry_delay': timedelta(minutes=DagConfig.retry_delay),
    'execution_timeout': timedelta(minutes=DagConfig.execution_timeout),
    "dbt_cloud_conn_id": dag_config.DBT_CLOUD_CONN_ID,
    "account_id": DagConfig.DBT_CLOUD_ACCOUNT_ID
}

truncate_snowflake_table = load_to_dbt_cloud = None
try:
    with DAG(
            DagConfig.job_name,
            default_args=default_args,
            description=DagConfig.description,
            tags=DagConfig.tags,
            max_active_runs=DagConfig.max_active_runs,
            concurrency=DagConfig.concurrency,
            catchup=DagConfig.catchup,
            dagrun_timeout=timedelta(minutes=DagConfig.dagrun_timeout),
            schedule_interval=DagConfig.schedule_interval

    ) as dag:
        sftp_hourly_job_template.get_sftp_dag_details(dag, DagConfig)

except IndexError as ex:
    logging.debug("Exception", str(ex))
