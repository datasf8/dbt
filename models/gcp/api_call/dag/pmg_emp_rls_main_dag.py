from datetime import datetime
import time
import json
import requests
import os
import math
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from google.cloud import storage
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.operators.bash import BashOperator
from pmg_emp_rls_config import dag_config as config
from airflow.models import TaskInstance, DagModel
from airflow import settings
from airflow.utils.trigger_rule import TriggerRule


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


def store_start_time_in_xcom(**context):
    today_date = datetime.now()
    context["task_instance"].xcom_push(key="job_start_time", value=today_date.strftime("%Y-%m-%d %H:%M:%S"))


def store_start_time_in_snowflake(job_name, task_id, task_list, table_name, schema_name, **context):
    dag_id = job_name
    database_name = config.database_name
    # schema_name = job_details.get("SCHEMA")
    # stage_database = dag_details.get("stage_database")
    # stage_schema = config.stage_schema
    job_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    job_start_time = context["task_instance"].xcom_pull(key="job_start_time")
    session = settings.Session()
    dag_run_id = context['run_id']
    print(f"DAG RUN ID: {dag_run_id}")
    print(f"DAG ID: {dag_id}")
    dag_obj = session.query(DagModel).get(dag_id)
    dag_run_obj = dag_obj.get_last_dagrun()
    execution_date = dag_run_obj.execution_date
    dag_run_id1 = dag_run_obj.run_id
    print(f"DAG RUN ID1: {dag_run_id1}")
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
            # for task_obj in ti:
            #     print(f"TASK_ID{task_obj.task_id}")
            #     print(f"DAG_ID{task_obj.dag_id}")
            #     print(f"RUN_ID{task_obj.run_id}")
            print(f"TASK NAME {task}")
            task_name = task
            state = "success"
            if ti:
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


def all_roles_details(bucket_name, path, db_file_name, **context):
    # db_file_name = ""
    modified_date_data = {}
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    # db_file_path = path
    # if bucket.blob(os.path.join(db_file_path, db_file_name)).exists():
    #     db_file_blob = bucket.blob(os.path.join(db_file_path, db_file_name))
    #     with db_file_blob.open("r") as myfile:
    #         for line in myfile:
    #             line = line.strip("\n")
    #             l1 = [line.split(",")]
    #             modified_date_data.update(dict(l1))

    url = config.URL
    auth = config.get_authentication()

    current_time = datetime.utcnow().replace(microsecond=0).isoformat()
    if not modified_date_data:
        modified_date_data = {"EmployeeLastRun": "1900-04-01T00:00:00"}
    # current_time = parse("2022-04-01").isoformat()

    date_filter = f"lastModifiedDate le datetimeoffset'{current_time}'"
    if modified_date_data:
        date_filter += f" and lastModifiedDate gt datetimeoffset'{modified_date_data.get('EmployeeLastRun')}'"

    # url = url + f"&$filter={date_filter}"
    url_file_name = config.url_file_name
    # f = requests.get(url, **auth)
    count = 3
    is_success = False
    msg = ""
    while count > 0:
        try:
            count -= 1
            f = requests.get(url, **auth)
            f.raise_for_status()
            is_success = True
            break
        except Exception as exc:
            time.sleep(120)
            msg = exc.__str__()
            logging.warning(f'Error while loading data from API {url} : {exc.__str__()}')
    if not is_success:
        raise Exception(
            f'Error while loading data from API {url} after 3 retries error: {msg}')
    file_data = f.json()
    employee_url_data = ""
    modified_date_data["EmployeeLastRun"] = current_time
    today_date = datetime.now()
    year = str(today_date.year)
    month = str(today_date.month).zfill(2)
    day = str(today_date.day).zfill(2)
    hour = str(today_date.hour).zfill(2)
    url_file_path = path
    path = os.path.join(path, year, month, day, hour)
    context["task_instance"].xcom_push(key="hour", value=hour)
    file_path = os.path.join(path, "ROLE_DETAILS",
                             f"ROLE_DETAILS_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json")
    logging.info("FILE_PATH: " + file_path)
    blob = bucket.blob(file_path)
    count = 3
    is_success = False
    msg = ""
    while count > 0:
        try:
            count -= 1
            blob.upload_from_string(json.dumps(file_data))
            # blob.upload_from_string(json.dumps(file_data))
            is_success = True
            break
        except Exception as exc:
            time.sleep(120)
            msg = exc.__str__()
            logging.warning(f'Error while uploading data to GCS {file_path} : {exc.__str__()}')
    if not is_success:
        raise Exception(
            f'Error while uploading data to GCS {file_path} after 3 retries error: {msg}')
    group_id_list = []
    for data in file_data["d"]["results"]:
        role_id = data["roleId"]
        if not config.ROLE_ID_LIST or role_id in config.ROLE_ID_LIST:
            sub_url = f"{config.MAIN_URL}/odata/v2/RBPRole({role_id}L)/rules?$format=json&paging=cursor&$expand=accessGroups,targetGroups"
            user_file_name = f"{role_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            # f = requests.get(sub_url, **auth)
            count = 3
            is_success = False
            msg = ""
            while count > 0:
                try:
                    count -= 1
                    f = requests.get(sub_url, **auth)
                    f.raise_for_status()
                    is_success = True
                    break
                except Exception as exc:
                    time.sleep(120)
                    msg = exc.__str__()
                    logging.warning(f'Error while loading data from API {url} : {exc.__str__()}')
            if not is_success:
                raise Exception(
                    f'Error while loading data from API {url} after 3 retries error: {msg}')
            file_data = f.json()

            for content_dict in file_data["d"]["results"]:
                access_groups = content_dict.get("accessGroups")
                target_groups = content_dict.get("targetGroups")
                for access_group in access_groups["results"]:
                    group_id = access_group.get("groupID")
                    if group_id not in group_id_list:
                        user_url = f"GROUP_USER|*|{config.MAIN_URL}/odata/v2/getUsersByDynamicGroup/?$format=json&paging=cursor&groupId={group_id}L"
                        user_url += f"|*|{config.path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|GROUP_USER|*|{group_id}|*|{current_time}|*|{hour}"
                        if employee_url_data:
                            employee_url_data += '\n'
                        employee_url_data += user_url
                        group_id_list.append(group_id)
                for target_group in target_groups["results"]:
                    group_id = target_group.get("groupID")
                    if group_id not in group_id_list:
                        user_url = f"GROUP_USER|*|{config.MAIN_URL}/odata/v2/getUsersByDynamicGroup/?$format=json&paging=cursor&groupId={group_id}L"
                        user_url += f"|*|{config.path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|GROUP_USER|*|{group_id}|*|{current_time}|*|{hour}"
                        if employee_url_data:
                            employee_url_data += '\n'
                        employee_url_data += user_url
                        group_id_list.append(group_id)
            file_path = os.path.join(path, "SF_ROLES_GROUP", user_file_name)
            logging.info("FILE_PATH: " + file_path)
            if file_data["d"]["results"]:
                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(file_path)
                count = 3
                is_success = False
                msg = ""
                while count > 0:
                    try:
                        count -= 1
                        blob.upload_from_string(json.dumps(file_data))
                        # blob.upload_from_string(json.dumps(file_data))
                        is_success = True
                        break
                    except Exception as exc:
                        time.sleep(120)
                        msg = exc.__str__()
                        logging.warning(f'Error while uploading data to GCS {file_path} : {exc.__str__()}')
                if not is_success:
                    raise Exception(
                        f'Error while uploading data to GCS {file_path} after 3 retries error: {msg}')

    # print(f"GROUP ID TOTAL COUNT {len(group_id_list)}")
    file_path = os.path.join(url_file_path, url_file_name)
    logging.info("FILE_PATH: " + file_path)
    no_of_urls = len(employee_url_data.split("\n"))
    context["task_instance"].xcom_push(key="no_of_urls", value=no_of_urls)
    logging.info(f"NUMBER OF URL COUNT: {no_of_urls}")
    blob = bucket.blob(file_path)
    count = 3
    is_success = False
    msg = ""
    while count > 0:
        try:
            count -= 1
            blob.upload_from_string(employee_url_data)
            # blob.upload_from_string(json.dumps(file_data))
            is_success = True
            break
        except Exception as exc:
            time.sleep(120)
            msg = exc.__str__()
            logging.warning(f'Error while uploading data to GCS {file_path} : {exc.__str__()}')
    if not is_success:
        raise Exception(
            f'Error while uploading data to GCS {file_path} after 3 retries error: {msg}')


def get_query_list(hour):
    today_date = datetime.today()
    year = str(today_date.year)
    month = str(today_date.month).zfill(2)
    day = str(today_date.day).zfill(2)
    database_name = config.database_name
    schema_name = config.schema_name
    path = config.path
    stage_database = config.stage_database
    stage_schema = config.stage_schema
    stage_name = config.SNOWFLAKE_STAGE_NAME
    query_list = [f"USE DATABASE {database_name};",
                  f"USE SCHEMA {schema_name};",
                  ]
    for table in config.RLS_TABLE_LIST:
        query = f"""copy into "{database_name}"."{schema_name}"."{table}"
from (
select  $1,metadata$filename from @{stage_database}.{stage_schema}.{stage_name}/{path}/{year}/{month}/{day}/{hour}/{table}
  )
FILE_FORMAT="{stage_database}"."{config.stage_schema}"."{config.FILE_FORMAT}"
  ;"""
        query_list.append(f"TRUNCATE TABLE {database_name}.{schema_name}.{table};")
        query_list.append(query)
    return query_list


def get_task_count(url_count):
    url_count = int(url_count)
    if url_count <= 100:
        return math.ceil(url_count / 20)
    elif 101 < url_count <= 1000:
        return math.ceil(url_count / 100)
    elif 1001 < url_count <= 5000:
        return math.ceil(url_count / 200)
    elif 5001 < url_count <= 10000:
        return math.ceil(url_count / 300)
    elif 10001 < url_count <= 50000:
        return math.ceil(url_count / 400)
    elif 50001 < url_count <= 100000:
        return math.ceil(url_count / 500)
    elif 100001 < url_count <= 500000:
        return math.ceil(url_count / 900)
    else:
        return 1000


def execute_cloudrun_job(job_name, url_count):
    task_count = get_task_count(url_count)
    task_obj = BashOperator(
        task_id="Group_id_to_user_data_cloudrun",
        bash_command=f'''gcloud beta run jobs execute {job_name} \
                                --region  {config.location} --project {config.project_name} \
                                --args rls_process.py \
                                --update-env-vars SOURCE_FILE_PATH={config.path}/employee_rls_list \
                                --tasks {task_count} \
                                --wait''',
        dag=dag)
    task_obj.execute(dict({}))


try:
    with DAG(
            "employee_rls_api_call",
            default_args=default_args,
            description='DAG for copying data from pmgm employee roles and group API',
            max_active_runs=config.max_active_runs,
            tags=['pmgm'],
            concurrency=config.concurrency,
            catchup=config.catchup,
            schedule_interval=config.schedule_interval,
            # schedule_interval=None,
            # schedule_interval='45 6 * * *',
            # schedule_interval='@daily',
    ) as dag:
        task_list = ["store_start_time_in_xcom", "Roles_to_group_data", "Group_id_to_user_data",
                     "gcs_to_snowflake_landing", "load_to_dbt_cloud", "store_job_details_in_snowflake"]
        all_employee_data = PythonOperator(
            task_id="Roles_to_group_data",
            python_callable=all_roles_details,
            op_kwargs={"bucket_name": config.BUCKET_NAME, "path": config.path,
                       "db_file_name": config.DATE_MODIFICATION_FILE_NAME}
        )

        job_name = config.CLOUD_RUN_JOB_NAME
        task_count = config.TASK_COUNT
        api_to_gcs_python = PythonOperator(
            task_id="Group_id_to_user_data",
            python_callable=execute_cloudrun_job,
            op_kwargs={"job_name": job_name, "url_count": '{{task_instance.xcom_pull(key="no_of_urls")}}'},
            provide_context=True
        )

        execute_snowflake_query = SnowflakeOperator(
                task_id="gcs_to_snowflake_landing",
                sql=get_query_list('{{task_instance.xcom_pull(key="hour")}}'),
                snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
                # do_xcom_push=True,
            )

        load_to_dbt_cloud = DbtCloudRunJobOperator(
            task_id="load_to_dbt_cloud",
            job_id=config.DBT_CLOUD_JOB_ID,
            wait_for_termination=True,
            additional_run_config={"threads_override": 8},
            depends_on_past=False,
        )
        store_start_time_in_xcom = PythonOperator(
            task_id="store_start_time_in_xcom",
            python_callable=store_start_time_in_xcom,
            op_kwargs={},
            dag=dag
        )
        schema_name = config.audit_schema_name
        store_job_details_in_snowflake = PythonOperator(
            task_id="store_job_details_in_snowflake",
            python_callable=store_start_time_in_snowflake,
            op_kwargs={"task_id": "store_job_details_in_snowflake", "task_list": task_list,
                       "table_name": "JOB_EXECUTION_LOG", "schema_name": schema_name,
                       "job_name": "employee_rls_api_call"},
            # do_xcom_push=True,
            provide_context=True,
            dag=dag,
            trigger_rule=TriggerRule.ALL_DONE
        )

except IndexError as ex:
    logging.debug("Exception", str(ex))

# all_employee_data
store_start_time_in_xcom >> all_employee_data >> api_to_gcs_python >> execute_snowflake_query >> load_to_dbt_cloud >> store_job_details_in_snowflake
# all_employee_data >> update_snowflake_audit
# update_snowflake_audit
