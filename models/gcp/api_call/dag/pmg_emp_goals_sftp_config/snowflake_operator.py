import json
from google.cloud import storage
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import logging
from pmg_emp_goals_sftp_config.dag_config import SNOWFLAKE_STAGE_NAME, LMS_SNOWFLAKE_CONN
from pmg_emp_goals_sftp_config import dag_config


def call_snowflake_operator(bucket_name, parent_folder, files_to_be_copy, file_name,):
    """

    :param bucket_name:
    :param parent_folder:
    :param files_to_be_copy:
    :param file_name:
    :return:
    """
    file_name1 = file_name
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    logging.info("Bucket data:" + str(bucket))
    for i, file in enumerate(files_to_be_copy):
        database = file[1]
        schema = file[2]
        table_name = file[3]
        column_names = ", ".join(file[4:])
        snowflake_stage_name = SNOWFLAKE_STAGE_NAME
        file_location = f"{snowflake_stage_name}/{parent_folder}/{file_name1}"
        queries = [
            f"USE DATABASE {database};",
            f"USE SCHEMA {schema};",
            f"""copy into {database}.{schema}.{table_name} from @{file_location}
            FILE_FORMAT = (format_name="{dag_config.FILE_FORMAT}");"""]


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
