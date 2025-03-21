from datetime import datetime,timedelta
from urllib import request
import os
import requests
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
import logging
from google.cloud import storage
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from fetch_power_bi_ips import dag_config as config
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import TaskInstance, DagModel
from airflow import settings,DAG


def store_start_time_in_xcom(**context):
    today_date = datetime.now()
    context["task_instance"].xcom_push(key="job_start_time", value=today_date.strftime("%Y-%m-%d %H:%M:%S"))


def store_start_time_in_snowflake(dag_details, job_name, task_id, task_list, table_name, schema_name, **context):
    dag_id = job_name
    database_name = dag_details.get("DATABASE")
    # schema_name = job_details.get("SCHEMA")
    # stage_database = dag_details.get("stage_database")
    # stage_schema = config.stage_schema
    job_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    job_start_time = context["task_instance"].xcom_pull(key="job_start_time")
    session = settings.Session()
    dag_obj = session.query(DagModel).get(dag_id)
    dag_run_obj = dag_obj.get_last_dagrun()
    execution_date = dag_run_obj.execution_date
    dag_run_id = dag_run_obj.run_id
    print(f"execution_date: {execution_date}")
    main_state = "success"
    queries = [f"USE DATABASE {database_name};",
               f"USE SCHEMA {schema_name};"]

    for task in task_list:
        if task_id != task:
            ti = session.query(TaskInstance).filter(TaskInstance.task_id == task,
                                                    TaskInstance.dag_id == dag_id,
                                                    TaskInstance.run_id == str(dag_run_id)
                                                    ).order_by(
                TaskInstance.end_date.desc()
            ).first()
            task_name = task
            state = ti.current_state()
            end_date = ti.end_date
            print(f"{task_name}: {state}, end_date: {end_date}")
            if state == 'failed':
                main_state = state
                break
    queries.append(f"DELETE FROM \"{database_name}\".\"{schema_name}\".{table_name} WHERE job_name='{dag_id}' and job_start_time='{job_start_time}'")
    queries.append(f"INSERT INTO \"{database_name}\".\"{schema_name}\".{table_name} VALUES('{dag_id}', '{job_start_time}', '{job_end_time}', '{main_state}')")
    snowflake_exe = SnowflakeOperator(
        task_id=task_id + "_cp",
        sql=queries,
        snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
        # do_xcom_push=True,
    )
    snowflake_exe.execute(dict({}))
    if main_state == 'failed':
        raise Exception("Upstream is failed")


def get_upload_ips_in_gcp(path, bucket_name, file_name):
    URL = "https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519"
    page = request.urlopen(URL)

    # parse HTML to get the real link
    soup = BeautifulSoup(page.read(), "html.parser")
    link = soup.find('a', {'data-bi-id': 'downloadretry'})['href']

    # download
    file_download = requests.get(link)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    # destination_path = path
    file_path = os.path.join(path, file_name)
    blob = bucket.blob(file_path)
    blob.upload_from_string(file_download.content)


def get_dag_details(dag, job_details, dag_details, destination_path):
    task_obj = None
    try:
        file_name = job_details.get("FILE_NAME")
        fetch_ips_from_power_bi_server = PythonOperator(
            task_id="fetch_ips_from_power_bi_server",
            python_callable=get_upload_ips_in_gcp,
            op_kwargs={"bucket_name": config.BUCKET_NAME, "path": destination_path,
                       "file_name": file_name},
            provide_context=True,
            dag=dag
        )

        store_start_time_in_xcom_task = PythonOperator(
            task_id="store_start_time_in_xcom",
            python_callable=store_start_time_in_xcom,
            op_kwargs={},
            dag=dag
        )
        schema_name = job_details.get("SCHEMA")
        store_start_time_in_snowflake_task = PythonOperator(
            task_id="store_start_time_in_snowflake",
            python_callable=store_start_time_in_snowflake,
            op_kwargs={"dag_details": dag_details, "task_id": "store_start_time_in_snowflake",
                       "task_list": ["store_start_time_in_xcom",
                                     "fetch_ips_from_power_bi_server"],
                       "table_name": "JOB_EXECUTION_LOG", "schema_name": schema_name,
                       "job_name": job_details.get("JOB_NAME")},
            # do_xcom_push=True,
            provide_context=True,
            dag=dag,
            trigger_rule=TriggerRule.ALL_DONE
        )
        task_obj = store_start_time_in_xcom_task >> fetch_ips_from_power_bi_server >> store_start_time_in_snowflake_task
    except IndexError as ex:
        logging.debug("Exception", str(ex))

    return task_obj


DESTINATION_PATH = config.DESTINATION_PATH


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
    "account_id": config.DBT_CLOUD_ACCOUNT_ID
}

task = None
try:
    DAG_DETAILS = {
        "DATABASE": config.DATABASE,
        "stage_database": config.stage_database,
        "SNOWFLAKE_STAGE_NAME": config.SNOWFLAKE_STAGE_NAME,
        "MAIN_URL": config.MAIN_URL,
        "DBT_CLOUD_JOB_ID": config.DBT_CLOUD_JOB_ID
    }
    job_details = {
        "FILE_NAME": config.FILE_NAME,
        "SCHEMA": config.SCHEMA,
        "JOB_NAME": config.JOB_NAME
    }
    dag = DAG(
            config.JOB_NAME,
            default_args=default_args,
            description=config.JOB_DESC,
            max_active_runs=config.max_active_runs,
            concurrency=config.concurrency,
            tags=None,
            catchup=config.catchup,
            schedule_interval=config.schedule_interval,
    )
    task = get_dag_details(dag, job_details, DAG_DETAILS, DESTINATION_PATH)
except IndexError as ex:
    logging.debug("Exception", str(ex))
