import os
import json
from datetime import datetime
from os.path import join, dirname
from dotenv import load_dotenv
from google.cloud import secretmanager
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
description = os.environ.get('description')
tags = os.environ.get('tags')

max_active_runs = int(os.environ.get('max_active_runs'))
concurrency = int(os.environ.get('concurrency'))
catchup = os.environ.get('catchup', "False").lower() in ("true", "t", 1)

BUCKET_NAME = os.environ.get('BUCKET_NAME')

DESTINATION_PATH = os.environ.get('DESTINATION_PATH')
path = os.environ.get("path")
stage_schema = os.environ.get("STAGE_SCHEMA")
format_schema = os.environ.get("format_schema")
SECRET_NAME = os.environ.get("SECRET_NAME")
TEMP_FILE_NAME = os.environ.get('TEMP_FILE_NAME')
SNOWFLAKE_CONN_ID = os.environ.get('SNOWFLAKE_CONN_ID')
FILE_FORMAT = os.environ.get('FILE_FORMAT')
DBT_CLOUD_CONN_ID = os.environ.get("DBT_CLOUD_CONN_ID")
DBT_CLOUD_CONN_SECRET = os.environ.get("DBT_CLOUD_ACCOUNT_ID")
JOB_NAME = os.environ.get("JOB_NAME")
JOB_DESC = os.environ.get("JOB_DESC")
FILE_NAME = os.environ.get("FILE_NAME")
SCHEMA = os.environ.get("SCHEMA")


def get_secret(project, secret_name, version='latest'):
    client = secretmanager.SecretManagerServiceClient()
    secret_path = client.secret_version_path(project, secret_name, version)
    secret = client.access_secret_version(secret_path)
    secret_data = secret.payload.data.decode('UTF-8')
    return json.loads(secret_data)


def get_authentication(is_local=False):
    secret_details = get_secret(project_name, SECRET_NAME)
    if secret_details.get("is_auth"):
        return {"auth": (secret_details.get("user_name"), secret_details.get("password"))}
    return {"headers": {"Authorization": secret_details.get("authorization_token")}}


config_data = configparser.ConfigParser()
file_path = os.path.dirname(os.path.abspath(__file__))
config_data.read(os.path.join(file_path, "config.ini"))

CONFIG_DETAILS = config_data["%ENV%"]

DATABASE = CONFIG_DETAILS.get("DATABASE")
stage_database = CONFIG_DETAILS.get("STAGE_DATABASE")
SNOWFLAKE_STAGE_NAME = CONFIG_DETAILS.get("SNOWFLAKE_STAGE_NAME")
MAIN_URL = CONFIG_DETAILS.get("MAIN_URL")
schedule_interval = CONFIG_DETAILS.get("schedule_interval", None)
DBT_CLOUD_ACCOUNT_ID = CONFIG_DETAILS.get("DBT_CLOUD_ACCOUNT_ID")
DBT_CLOUD_JOB_ID = CONFIG_DETAILS.get("DBT_CLOUD_JOB_ID")