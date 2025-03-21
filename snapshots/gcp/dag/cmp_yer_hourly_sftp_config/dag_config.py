
import os
import json
from os.path import join, dirname
from dotenv import load_dotenv
import configparser
from datetime import datetime

# Project Configurations
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)


class DagConfig:
    retries = int(os.environ.get('retries'))
    retry_delay = int(os.environ.get('retry_delay'))
    execution_timeout = int(os.environ.get('execution_timeout'))
    task_timeout = int(os.environ.get('task_timeout'))
    dagrun_timeout = int(os.environ.get('dagrun_timeout'))
    depends_on_past = os.environ.get('depends_on_past', "False").lower() in ("true", "t", 1)
    start_date = datetime.strptime(os.environ.get('start_date'), "%Y-%m-%d %H:%M:%S")

    max_active_runs = int(os.environ.get('max_active_runs'))
    concurrency = int(os.environ.get('concurrency'))
    catchup = os.environ.get('catchup', "False").lower() in ("true", "t", 1)
    job_name = os.environ.get('job_name')
    description = os.environ.get('description')
    tags = json.loads(os.environ.get('tags')) if os.environ.get('tags') else None
    # Temporary Path
    # dags task to be executed
    IS_UNZIP = os.environ.get("IS_UNZIP", "False").lower() in ("true", "t", 1)
    IS_LOAD_SNOWFLAKE = os.environ.get("IS_LOAD_SNOWFLAKE", "False").lower() in ("true", "t", 1)
    IS_ENCRYPT_FILE = os.environ.get("IS_ENCRYPT_FILE", "False").lower() in ("true", "t", 1)
    IS_DBT_LOAD = os.environ.get("IS_DBT_LOAD", "False").lower() in ("true", "t", 1)
    IS_EXCEL_CONVERT = os.environ.get("IS_EXCEL_CONVERT", "False").lower() in ("true", "t", 1)

    FILE_PATTERN_TYPE = os.environ.get("FILE_PATTERN_TYPE")
    PREVIOUS_DATE = os.environ.get("PREVIOUS_DATE")
    FILE_LIST = []  # json.loads(os.environ.get("FILE_LIST"))

    FILE_DETAILS = os.environ.get("FILE_DETAILS")
    FILE_FORMAT = os.environ.get("FILE_FORMAT")
    run_for_last_day = os.environ.get("run_for_last_day", "False").lower() in ("true", "t", 1)
    STAGE_SCHEMA_NAME = os.environ.get("STAGE_SCHEMA_NAME")
    IS_CSV = os.environ.get("IS_CSV", "False").lower() in ("true", "t", 1)
    ZIP_FILE_PATH = os.environ.get("ZIP_FILE_PATH")
    NO_DATE_REQUIRED = os.environ.get("NO_DATE_REQUIRED", False)

    config_data = configparser.ConfigParser()
    file_path = os.path.dirname(os.path.abspath(__file__))
    config_data.read(os.path.join(file_path, "config.ini"))
    CONFIG_DETAILS = config_data["%ENV%"]

    schedule_interval = CONFIG_DETAILS.get('schedule_interval', None)
    SNOWFLAKE_STAGE_NAME = CONFIG_DETAILS.get("SNOWFLAKE_STAGE_NAME")
    DESTINATION_BUCKET = CONFIG_DETAILS.get("DESTINATION_BUCKET")
    DBT_CLOUD_ACCOUNT_ID = CONFIG_DETAILS.get("DBT_CLOUD_ACCOUNT_ID")
    DBT_CLOUD_JOB_ID = CONFIG_DETAILS.get("DBT_CLOUD_JOB_ID")
    FILE_PATTERN = CONFIG_DETAILS.get("FILE_PATTERN")
    LMS_SCP_CONN = CONFIG_DETAILS.get("LMS_SCP_CONN")
    SOURCE_PATH = CONFIG_DETAILS.get("SOURCE_PATH")
    IS_DELETE_SFTP_FILE = CONFIG_DETAILS.get("IS_DELETE_SFTP_FILE", "False").lower() in ("true", "t", 1)
    POKE_INTERVAL = 600
