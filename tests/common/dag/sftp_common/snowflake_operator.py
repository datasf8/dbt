import json
from google.cloud import storage
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import logging
from datetime import datetime
from sftp_common.dag_config import LMS_SNOWFLAKE_CONN
from airflow.models import TaskInstance, DagModel
from airflow import settings


def create_columns_name(column_list):
    col_dict = {}
    new_col_list = []
    for item in column_list:
        item = item.strip()
        if item in col_dict:
            col_name = item.strip().strip('"')
            col_name = col_name + " " + str(col_dict[item])
            col_dict[item] += 1
            new_col_list.append(col_name)
        else:
            col_dict[item] = 1
            col_name = item.strip().strip('"')
            new_col_list.append(col_name)
    return new_col_list


def call_snowflake_operator(config, bucket_name, parent_folder, files_to_be_copy, file_name, **context):
    """

    :param config:
    :param bucket_name:
    :param parent_folder:
    :param files_to_be_copy:
    :param file_name:
    :param is_csv:
    :return:
    """
    file_name1 = file_name
    is_csv = config.IS_CSV
    if not is_csv:
        file_name1 = file_name + ".csv"
    hour = context["task_instance"].xcom_pull(key="hour")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    logging.info("Bucket data:" + str(bucket))
    # creating landing table on the basis of csv file
    blob = bucket.blob(f"{parent_folder}/{hour}/{file_name1}")
    file_data = blob.download_as_string()
    file_data_rows = str(file_data.decode("UTF-8")).split("\n")
    column_header = file_data_rows[int(config.CSV_HEADER_LOCATION)]
    header_list = create_columns_name(column_header.split(","))
    select_string = ''
    for n, col_name in enumerate(header_list):
        if select_string:
            select_string += ", "
        col_name = col_name.strip().strip('"')
        select_string += f'${n+1} AS "{col_name}"'
    logging.info(f"COLUMN HEADER FOR CSV: {select_string}")
    for i, file in enumerate(files_to_be_copy):
        database = file[1]
        schema = file[2]
        table_name = file[3]
        # column_names = ", ".join(file[4:])
        snowflake_stage_name = config.STAGE_SCHEMA_NAME + "." + config.SNOWFLAKE_STAGE_NAME
        file_location = f"{snowflake_stage_name}/{parent_folder}/{hour}/{file_name1}"
        queries = [
            f"USE DATABASE {database};",
            f"USE SCHEMA {schema};",
            f"""CREATE OR REPLACE TABLE {database}.{schema}.{table_name} AS SELECT 
            {select_string}
            from @{file_location}
            (FILE_FORMAT => "{config.FILE_FORMAT}");"""]

        snow_pipe = SnowflakeOperator(
            task_id="copy_to_snowflake_" + str(i),
            sql=queries,
            snowflake_conn_id=LMS_SNOWFLAKE_CONN,
            # do_xcom_push=True,
        )
        snow_pipe.execute(dict({}))


def snowflake_truncate_table(files_to_be_copy):
    """

    :param files_to_be_copy:
    :return:
    """
    queries = []
    for i, file in enumerate(files_to_be_copy):
        database = file[1]
        schema = file[2]
        table_name = file[3]
        if not queries:
            queries = [
                f"USE DATABASE {database};",
                f"USE SCHEMA {schema};"]
        queries.append(f"TRUNCATE TABLE {database}.{schema}.{table_name};")
    # print("ALL TRUNCATE QUERIES", queries)
    snow_pipe = SnowflakeOperator(
        task_id="truncate_snowflake_table_1",
        sql=queries,
        snowflake_conn_id=LMS_SNOWFLAKE_CONN,
        # do_xcom_push=True,
    )
    snow_pipe.execute(dict({}))


def store_start_time_in_snowflake(database_name, job_name, task_id, task_list, table_name, schema_name, **context):
    dag_id = job_name
    # database_name = dag_config.get("DATABASE")
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
        snowflake_conn_id=LMS_SNOWFLAKE_CONN,
        # do_xcom_push=True,
    )
    snowflake_exe.execute(dict({}))
    if main_state == 'failed':
        raise Exception("Upstream is failed")
