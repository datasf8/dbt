
import os
import json
from datetime import datetime
from os.path import join, dirname
from dotenv import load_dotenv
import configparser

# Project Configurations
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

project_name = os.environ.get("project_name")
location = os.environ.get("location")
zone = os.environ.get('zone')
job_name = os.environ.get('job_name')

# DAGs configuration
depends_on_past = os.environ.get('depends_on_past', "False").lower() in ("true", "t", 1)
start_date = datetime.strptime(os.environ.get('start_date'), "%Y-%m-%d %H:%M:%S")
email = json.loads(os.environ.get('email'))
email_on_failure = os.environ.get('email_on_failure', "False").lower() in ("true", "t", 1)
email_on_retry = os.environ.get('email_on_retry', "False").lower() in ("true", "t", 1)
retries = int(os.environ.get('retries'))
retry_delay = int(os.environ.get('retry_delay'))
execution_timeout = int(os.environ.get('execution_timeout'))
dagrun_timeout = int(os.environ.get('dagrun_timeout'))
description = os.environ.get('description')
max_active_runs = int(os.environ.get('max_active_runs'))
concurrency = int(os.environ.get('concurrency'))
catchup = os.environ.get('catchup', "False").lower() in ("true", "t", 1)

# GCS Bucket, location & Folder names
# BUCKET_LOC = os.environ.get("BUCKET_LOC")
DESTINATION_PATH = os.environ.get("DESTINATION_PATH")
ZIP_FILE_PATH = os.environ.get("ZIP_FILE_PATH")
PROCESSED_FILE_PATH = os.environ.get("PROCESSED_FILE_PATH")
# GCS Archieve Bucket
ARCH_BUCKET = os.environ.get("ARCH_BUCKET")

DBT_CLOUD_CONN_ID = os.environ.get("DBT_CLOUD_CONN_ID")
DBT_CLOUD_CONN_SECRET = os.environ.get("DBT_CLOUD_CONN_SECRET")

# Temporary Path
# dags task to be executed
IS_UNZIP = os.environ.get("IS_UNZIP", "False").lower() in ("true", "t", 1)
IS_LOAD_SNOWFLAKE = os.environ.get("IS_LOAD_SNOWFLAKE", "False").lower() in ("true", "t", 1)
IS_ENCRYPT_FILE = os.environ.get("IS_ENCRYPT_FILE", "False").lower() in ("true", "t", 1)
IS_DBT_LOAD = os.environ.get("IS_DBT_LOAD", "False").lower() in ("true", "t", 1)
IS_FULL_LOAD = os.environ.get("IS_FULL_LOAD", "False").lower() in ("true", "t", 1)

FILE_PATTERN_TYPE = os.environ.get("FILE_PATTERN_TYPE")
PREVIOUS_DATE = os.environ.get("PREVIOUS_DATE")
FILE_LIST = []  # json.loads(os.environ.get("FILE_LIST"))
LMS_SCP_SECRET = os.environ.get("LMS_SCP_SECRET")

FILE_DETAILS = os.environ.get("FILE_DETAILS")
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")
ENCRYPTION_SECRET = os.environ.get("ENCRYPTION_SECRET")
LMS_SNOWFLAKE_CONN = os.environ.get("LMS_SNOWFLAKE_CONN")
LMS_SNOWFLAKE_SECRET = os.environ.get("LMS_SNOWFLAKE_SECRET")
FILE_FORMAT = os.environ.get("FILE_FORMAT")


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
