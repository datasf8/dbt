from datetime import datetime
import json
import requests
import os
import time
import math
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import logging
from google.cloud import storage
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.operators .bash import BashOperator
from sdds_pmg_emp_forms_config import dag_config as config
from pipeline_pmgm.pipeline_pmgm_config import dag_config as pipe_config
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import TaskInstance, DagModel
from airflow import settings


JOB_DETAILS = config.JOB_DETAILS


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
    database_name = config.DATABASE
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
            ti = session.query(TaskInstance).filter(TaskInstance.task_id == task.get("TASK_NAME"),
                                                    TaskInstance.dag_id == dag_id,
                                                    TaskInstance.run_id == str(dag_run_id)
                                                    ).order_by(
                TaskInstance.end_date.desc()
            ).first()
            task_name = task.get("TASK_NAME")
            state = ti.current_state()
            end_date = ti.end_date
            print(f"{task_name}: {state}, end_date: {end_date}")
            if state == 'failed':
                main_state = state
                break
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


def get_last_modified_filter(bucket_name, path, file_name, temp_file_name, **context):
    modified_date_data = {}
    db_file_name = file_name
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    if bucket.blob(os.path.join(path, db_file_name)).exists():
        db_file_blob = bucket.blob(os.path.join(path, db_file_name))
        with db_file_blob.open("r") as myfile:
            for line in myfile:
                line = line.strip("\n")
                l1 = [line.split(",")]
                modified_date_data.update(dict(l1))
    modified_date_data = dict(modified_date_data) if modified_date_data else {}
    current_time = datetime.utcnow().replace(microsecond=0).isoformat()
    last_modified_date = "1900-04-01T00:00:00"
    if modified_date_data:
        last_modified_date = modified_date_data["EmployeeLastRun"]

    modified_date_data["EmployeeLastRun"] = current_time
    today_date = datetime.now()
    hour = str(today_date.hour).zfill(2)
    context["task_instance"].xcom_push(key="hour", value=hour)
    context["task_instance"].xcom_push(key="last_modified_date", value=last_modified_date)
    context["task_instance"].xcom_push(key="current_modified_date", value=current_time)
    db_file_blob = bucket.blob(os.path.join(path, temp_file_name))
    str_modified_date_data = ""
    for line in modified_date_data.items():
        str_modified_date_data += ",".join(line)
        str_modified_date_data += "\n"
    count = 3
    is_success = False
    msg = ""
    while count > 0:
        try:
            count -= 1
            db_file_blob.upload_from_string(str_modified_date_data)
            # blob.upload_from_string(url_data)
            is_success = True
            break
        except Exception as exc:
            time.sleep(120)
            msg = exc.__str__()
            logging.warning(f'Error while uploading data to GCS {temp_file_name} : {exc.__str__()}')
    if not is_success:
        raise Exception(
            f'Error while uploading data to GCS {temp_file_name} after 3 retries error: {msg}')


def get_query_list(hour):
    today_date = datetime.today()
    year = str(today_date.year)
    month = str(today_date.month).zfill(2)
    day = str(today_date.day).zfill(2)
    database_name = config.DATABASE
    schema_name = JOB_DETAILS.get("SCHEMA")
    path = config.DESTINATION_PATH
    stage_database = config.stage_database
    stage_schema = config.stage_schema
    query_list = [f"USE DATABASE {database_name};",
                  f"USE SCHEMA {schema_name};"]
    for table_info in JOB_DETAILS.get("TABLE_LIST"):
        table = table_info.get("TABLE")
        file = table_info.get("FOLDER")
        # if file not in ["FORM_CONTENT", "FORM_HEADER", "FORM_HEADER_DELETED_FULL"]:
        #     file = "/".join(file.split("_", 1))

        query = f"""copy into "{database_name}"."{schema_name}"."{table}"
from (
select  $1,metadata$filename from @{stage_database}.{stage_schema}.{config.SNOWFLAKE_STAGE_NAME}/{path}/{year}/{month}/{day}/{hour}/{file}
  )
FILE_FORMAT="{stage_database}"."{stage_schema}"."{config.FILE_FORMAT}"
  ;"""
        query_list.append(f"TRUNCATE TABLE {database_name}.{schema_name}.{table};")
        query_list.append(query)
    return query_list


def create_url_file_api_data(bucket_name, path, api_details, **context):
    # db_file_name = ""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    main_url = config.MAIN_URL
    url_path = api_details.get("URL")
    main_filter = api_details.get("FILTERS")
    main_order_by = api_details.get("ORDER_BY")
    expand_cols = api_details.get("EXPAND_COLS")
    select_cols = api_details.get("SELECT_COLS")
    sequence_table_name = api_details.get("TABLE")
    url_file_name = api_details.get("URL_FILE_NAME")
    dependant_url_list = api_details.get("SUB_URLS")
    is_store_data = api_details.get("IS_STORE_DATA")
    data_id = api_details.get("JSON_FIELD_NAME")
    page_size = api_details.get("PAGE_SIZE")
    auth = config.get_authentication()
    date_filter = ""
    order_by_str = ""
    current_modified_date = context["task_instance"].xcom_pull(key="current_modified_date")
    last_modified_date = context["task_instance"].xcom_pull(key="last_modified_date")
    for order_by in main_order_by:
        column_name = order_by.get("COLUMN_NAME")
        is_desc = order_by.get("IS_DESC")
        if not order_by_str:
            order_by_str += "&$orderby="
        else:
            order_by_str += ", "
        column_order = "desc" if is_desc else ""
        order_by_str += column_name + column_order
    expand_cols_str = ""

    for expand_col in expand_cols:
        if not expand_cols_str:
            expand_cols_str += "&$expand="
        else:
            expand_cols_str += ", "
        expand_cols_str += expand_col
    page_size_str = ""
    if page_size:
        page_size_str = f"&customPageSize={page_size}"

    for fltr in main_filter:
        column_name = fltr.get("COLUMN_NAME")
        operator = fltr.get("OPERATOR")
        operand = fltr.get("OPERAND")
        value = fltr.get("COLUMN_VALUE")
        if not date_filter:
            date_filter += "&$filter="
        # else:
        if fltr.get("IS_LAST_MODIFIED"):
            last_modified_date = context["task_instance"].xcom_pull(key="last_modified_date")
            date_time = current_modified_date
            if operator == "gt":
                date_time = last_modified_date
            date_filter += column_name + " " + operator + " datetimeoffset'" + date_time + "'"
        else:
            date_filter += column_name + " " + operator + " " + value
        if operand:
            date_filter += " " + operand + " "
    select_cols_str = ""
    if select_cols:
        for select_col in select_cols:
            if not select_cols_str:
                select_cols_str += "&$select="
            else:
                select_cols_str += ", "
            select_cols_str += select_col

    url = main_url + url_path + page_size_str + order_by_str + date_filter + expand_cols_str + select_cols_str
    url = url.format(template_id=config.TEMPLATE_ID)
    id_list = []
    url_data = ""
    today_date = datetime.now()
    year = str(today_date.year)
    month = str(today_date.month).zfill(2)
    day = str(today_date.day).zfill(2)
    hour = context["task_instance"].xcom_pull(key="hour")
    if not hour:
        hour = str(today_date.hour).zfill(2)
        context["task_instance"].xcom_push(key="hour", value=hour)
    num = 1
    data_count = 0
    database_name = config.DATABASE
    schema_name = JOB_DETAILS.get("SCHEMA")
    logging.info("Next URL path: " + str(url))
    while True:
        count = 3
        f = object
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
                logging.warning(f'Error while Retrieving data from Source API {url} : {exc.__str__()}')
        if not is_success:
            raise Exception(f'Error while Retrieving data from Source API {url} after 3 retries error: {msg}')
        print("URL data fetched successfully")
        file_data = f.json()
        if not file_data.get("d"):
            logging.info(f"Header json for above url: {file_data}")
            break
        user_ids = []
        if not dependant_url_list:
            id_list = [1]
        if file_data.get("d") and dependant_url_list:
            for user_data in file_data["d"]["results"]:
                data_count += 1
                data_id_value = user_data.get(data_id)
                for url_dict in dependant_url_list:
                    table_name = url_dict.get("TABLE")
                    sub_url_path = url_dict.get("URL")
                    url_fltrs = url_dict.get("FILTERS")
                    url_order_by = url_dict.get("ORDER_BY")
                    url_expand_cols = url_dict.get("EXPAND_COLS")
                    page_size = url_dict.get("PAGE_SIZE")
                    is_combined_filter = url_dict.get("IS_COMBINED_FILTER")
                    date_filter = ""
                    order_by_str = ""
                    user_ids.append(data_id_value)
                    if not is_combined_filter:
                        for order_by in url_order_by:
                            column_name = order_by.get("COLUMN_NAME")
                            is_desc = order_by.get("IS_DESC")
                            if not order_by_str:
                                order_by_str += "&$orderby="
                            else:
                                order_by_str += ", "
                            column_order = "desc" if is_desc else ""
                            order_by_str += column_name + column_order
                        for expand_col in url_expand_cols:
                            if not expand_cols_str:
                                expand_cols_str += "&$expand="
                            else:
                                expand_cols_str += ", "
                            expand_cols_str += expand_col
                        page_size_str = ""
                        if page_size:
                            page_size_str = f"&customPageSize={page_size}"
                        for fltr in url_fltrs:
                            column_name = fltr.get("COLUMN_NAME")
                            operator = fltr.get("OPERATOR")
                            operand = fltr.get("OPERAND")
                            value = fltr.get("COLUMN_VALUE")
                            if not date_filter:
                                date_filter += "&$filter="
                            # else:

                            if fltr.get("IS_LAST_MODIFIED"):

                                date_time = current_modified_date
                                if operator == "gt":
                                    date_time = last_modified_date
                                date_filter += column_name + " " + operator + " datetimeoffset'" + date_time + "'"
                            else:
                                date_filter += column_name + " " + operator + " '" + value + "'"
                            if operand:
                                date_filter += " " + operand + " "
                        select_cols_str = ""
                        if select_cols:
                            for select_col in select_cols:
                                if not select_cols_str:
                                    select_cols_str += "&$select="
                                else:
                                    select_cols_str += ", "
                                select_cols_str += select_col
                        sub_url = main_url + sub_url_path + page_size_str + order_by_str + date_filter + expand_cols_str + select_cols_str
                        sub_url = sub_url.format(data_id=data_id_value)
                        user_url = f"{table_name}|*|{sub_url}|*|{config.DESTINATION_PATH}" + \
                                   f"|*|{database_name}|*|{schema_name}|*|{table_name}|*|{data_id_value}|*|{current_modified_date}|*|{hour}"
                        id_list.append(user_url)
                        if url_data:
                            url_data += '\n'
                        url_data += user_url
            data_size = 400
            sub_user_ids = [user_ids[i: i + data_size] for i in range(0, 1000, data_size)]
            for n, sub_list in enumerate(sub_user_ids):
                sub_list_str = ",".join(["'" + i + "'" for i in sub_list])
                if sub_list_str:
                    for url_dict in dependant_url_list:
                        table_name = url_dict.get("TABLE")
                        sub_url_path = url_dict.get("URL")
                        url_fltrs = url_dict.get("FILTERS")
                        url_order_by = url_dict.get("ORDER_BY")
                        url_expand_cols = url_dict.get("EXPAND_COLS")
                        page_size = url_dict.get("PAGE_SIZE")
                        is_combined_filter = url_dict.get("IS_COMBINED_FILTER")
                        date_filter = ""
                        order_by_str = ""
                        if is_combined_filter:
                            for order_by in url_order_by:
                                column_name = order_by.get("COLUMN_NAME")
                                is_desc = order_by.get("IS_DESC")
                                if not order_by_str:
                                    order_by_str += "&$orderby="
                                else:
                                    order_by_str += ", "
                                column_order = "desc" if is_desc else ""
                                order_by_str += column_name + column_order
                            expand_cols_str = ""
                            for expand_col in url_expand_cols:
                                if not expand_cols_str:
                                    expand_cols_str += "&$expand="
                                else:
                                    expand_cols_str += ", "
                                expand_cols_str += expand_col
                            page_size_str = ""
                            if page_size:
                                page_size_str = f"&customPageSize={page_size}"
                            for fltr in url_fltrs:
                                column_name = fltr.get("COLUMN_NAME")
                                operator = fltr.get("OPERATOR")
                                operand = fltr.get("OPERAND")
                                value = fltr.get("COLUMN_VALUE")
                                if not date_filter:
                                    date_filter += "&$filter="
                                # else:
                                if fltr.get("IS_LAST_MODIFIED"):
                                    last_modified_date = context["task_instance"].xcom_pull(key="last_modified_date")
                                    date_time = current_modified_date
                                    if operator == "gt":
                                        date_time = last_modified_date
                                    date_filter += column_name + " " + operator + " datetimeoffset'" + date_time + "'"
                                else:
                                    date_filter += column_name + " " + operator + " " + value
                                if operand:
                                    date_filter += " " + operand + " "
                            select_cols_str = ""
                            if select_cols:
                                for select_col in select_cols:
                                    if not select_cols_str:
                                        select_cols_str += "&$select="
                                    else:
                                        select_cols_str += ", "
                                    select_cols_str += select_col
                            sub_url = main_url + sub_url_path + page_size_str + order_by_str + date_filter + expand_cols_str + select_cols_str
                            sub_url = sub_url.format(data_id=sub_list_str)
                            print("URL:", sub_url)
                            user_url = f"{table_name}|*|{sub_url}|*|{config.DESTINATION_PATH}" + \
                                       f"|*|{database_name}|*|{schema_name}|*|{table_name}|*|{num}{n + 1}|*|{current_modified_date}|*|{hour}"
                            id_list.append(user_url)
                            if url_data:
                                url_data += '\n'
                            url_data += user_url
        file_path = os.path.join(path, year, month, day, hour)
        logging.info("Before data store")
        if is_store_data:
            logging.info("In data store block")
            file_path = os.path.join(file_path, sequence_table_name,
                                     f"{sequence_table_name}_{num}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json")
            blob = bucket.blob(file_path)
            if id_list and file_data.get("d") and file_data["d"].get("results"):
                # blob.upload_from_string(json.dumps(file_data))
                count = 3
                is_success = False
                msg = ""
                while count > 0:
                    try:
                        count -= 1
                        blob.upload_from_string(json.dumps(file_data))
                        is_success = True
                        break
                    except Exception as exc:
                        time.sleep(120)
                        msg = exc.__str__()
                        logging.warning(f'Error while uploading data to GCS {file_path} : {exc.__str__()}')
                if not is_success:
                    raise Exception(
                        f'Error while uploading data to GCS {file_path} after 3 retries error: {msg}')
        url = file_data["d"].get("__next")
        logging.info("Next URL path: " + str(url))
        num += 1
        if not url:
            break
    file_path = os.path.join(path, url_file_name)
    no_of_urls = len(url_data.split("\n"))
    context["task_instance"].xcom_push(key="no_of_urls", value=no_of_urls)
    logging.info(f"NUMBER OF URL COUNT: {no_of_urls}")
    blob = bucket.blob(file_path)
    print(f"No of URLS: {data_count}")
    context["task_instance"].xcom_push(key="number_of_urls", value=data_count)
    count = 3
    is_success = False
    msg = ""
    while count > 0:
        try:
            count -= 1
            blob.upload_from_string(url_data)
            # blob.upload_from_string(url_data)
            is_success = True
            break
        except Exception as exc:
            time.sleep(120)
            msg = exc.__str__()
            logging.warning(f'Error while uploading data to GCS {file_path} : {exc.__str__()}')
    if not is_success:
        raise Exception(
            f'Error while uploading data to GCS {file_path} after 3 retries error: {msg}')


def update_snowflake_timestamp(bucket_name, db_file_path, file_name, temp_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    target_bucket = storage_client.bucket(bucket_name)
    logging.info("bucket_name: " + bucket_name)
    logging.info("db_file_path: " + db_file_path)
    logging.info("db_file_name: " + temp_file_name)
    if bucket.blob(os.path.join(db_file_path, temp_file_name)).exists():
        db_file_blob = bucket.blob(os.path.join(db_file_path, temp_file_name))
        bucket.copy_blob(db_file_blob, target_bucket,
                         os.path.join(db_file_path, file_name))


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
        task_id=task_id + '_cloudrun',
        bash_command=f'''gcloud beta run jobs execute {job_name} \
                            --region  {config.location} --project {config.project_name} \
                            --args form_process.py \
                            --update-env-vars SOURCE_FILE_PATH={config.DESTINATION_PATH}/employee_forms_list \
                            --tasks {task_count} \
                            --wait''',
        dag=dag,
    )
    task_obj.execute(dict({}))


def skip_task_if_no_data(number_of_urls):
    group_task_list = []
    print(f"FILE_PATTERN:-{number_of_urls}")
    print(f"FILE_PATTERN TYPE:-{type(number_of_urls)}")
    number_of_urls = int(number_of_urls)
    if number_of_urls > 0:
        group_task_list.append("PE_YE_SGN_overall_data")
    else:
        group_task_list.append("GCS_to_Snowflake_landing")
    return group_task_list


try:
    with DAG(
            JOB_DETAILS.get("JOB_NAME"),
            default_args=default_args,
            description=JOB_DETAILS.get("JOB_DESC"),
            max_active_runs=config.max_active_runs,
            concurrency=config.concurrency,
            tags=json.loads(config.tags) if config.tags else None,
            catchup=config.catchup,
            schedule_interval=config.schedule_interval,
            # schedule_interval=None,
            # schedule_interval='@daily',
    ) as dag:
        dag_tasks = []
        task_list = JOB_DETAILS.get("TASK_LIST")
        task_dict = {}
        for task_detail in task_list:
            task_id = task_detail.get("TASK_NAME")
            task_type = task_detail.get("TASK_TYPE")
            downstream = task_detail.get("DOWNSTREAM")
            upstream = task_detail.get("UPSTREAM")

            if task_type == "audit1":
                file_name = task_detail.get("DATE_MODIFICATION_FILE_NAME")
                temp_file_name = task_detail.get("TEMP_FILE_NAME")
                task_obj = PythonOperator(
                    task_id=task_id,
                    python_callable=get_last_modified_filter,
                    op_kwargs={"bucket_name": config.BUCKET_NAME, "path": config.DESTINATION_PATH,
                               "temp_file_name": temp_file_name, "file_name": file_name},
                    provide_context=True
                )

            elif task_type == "airflow":
                api_details = task_detail.get("API_DETAILS")
                task_obj = PythonOperator(
                    task_id=task_id,
                    python_callable=create_url_file_api_data,
                    op_kwargs={"bucket_name": config.BUCKET_NAME, "path": config.DESTINATION_PATH,
                               "api_details": api_details},
                    provide_context=True
                )
            elif task_type == "dataflow":
                python_file = task_detail.get("PYTHON_FILE")
                dataflow_job_name = task_detail.get("JOB_NAME")
                task_obj = DataflowCreatePythonJobOperator(
                    task_id=task_id,
                    job_name=dataflow_job_name,
                    py_interpreter='python3',
                    py_file=f"/home/airflow/gcs/dags/pipeline_pmgm/{python_file}",
                    location=config.location,
                    wait_until_finished=True)
            elif task_type == 'cloudrun_execute':
                job_name = task_detail.get("JOB_NAME")
                task_count = task_detail.get("TASK_COUNT")
                task_obj = PythonOperator(
                    task_id=task_id,
                    python_callable=execute_cloudrun_job,
                    op_kwargs={"job_name": job_name, "url_count": '{{task_instance.xcom_pull(key="no_of_urls")}}'},
                    provide_context=True
                )

            elif task_type == "snowflake":
                task_obj = SnowflakeOperator(
                    task_id=task_id,
                    sql=get_query_list('{{task_instance.xcom_pull(key="hour")}}'),
                    snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
                    trigger_rule=TriggerRule.NONE_FAILED
                    # do_xcom_push=True,
                )

            elif task_type == "dbt":
                task_obj = DbtCloudRunJobOperator(
                    task_id=task_id,
                    job_id=config.DBT_CLOUD_JOB_ID,
                    wait_for_termination=True,
                    additional_run_config={"threads_override": 8},
                    depends_on_past=False,
                )

            elif task_type == "audit":
                file_name = task_detail.get("DATE_MODIFICATION_FILE_NAME")
                temp_file_name = task_detail.get("TEMP_FILE_NAME")
                task_obj = PythonOperator(
                    task_id=task_id,
                    python_callable=update_snowflake_timestamp,
                    op_kwargs={"bucket_name": config.BUCKET_NAME, "db_file_path": config.DESTINATION_PATH,
                               "temp_file_name": temp_file_name, "file_name": file_name}
                )
            elif task_type == "job_start_time":
                task_obj = PythonOperator(
                    task_id=task_id,
                    python_callable=store_start_time_in_xcom,
                    op_kwargs={},
                    dag=dag
                )
            elif task_type == "job_end_time":
                schema_name = JOB_DETAILS.get("SCHEMA")
                task_obj = PythonOperator(
                    task_id=task_id,
                    python_callable=store_start_time_in_snowflake,
                    op_kwargs={"task_id": task_id, "task_list": task_list,
                               "table_name": "JOB_EXECUTION_LOG", "schema_name": schema_name,
                               "job_name": JOB_DETAILS.get("JOB_NAME")},
                    # do_xcom_push=True,
                    provide_context=True,
                    dag=dag,
                    trigger_rule=TriggerRule.ALL_DONE
                )
            elif task_type == 'no_data_skip':
                task_obj = BranchPythonOperator(
                    task_id=task_id,
                    python_callable=skip_task_if_no_data,
                    op_kwargs={"number_of_urls": '{{task_instance.xcom_pull(key="number_of_urls")}}'},
                    dag=dag)
            task_details = {"task_obj": task_obj,
                            "upstream": upstream,
                            "downstream": downstream}
            task_dict.update({task_id: task_details})
            dag_tasks.append(task_id)

except IndexError as ex:
    logging.debug("Exception", str(ex))


a = None
for t in dag_tasks:
    task_details = task_dict.get(t)
    task = task_details.get("task_obj")
    upstream_task = task_details.get("upstream")
    downstream_task = task_details.get("downstream")
    if upstream_task:
        u_task_obj = task_dict[upstream_task]["task_obj"]
        task.set_upstream(u_task_obj)
    if downstream_task:
        d_task_obj = task_dict[downstream_task]["task_obj"]
        task.set_downstream(d_task_obj)
