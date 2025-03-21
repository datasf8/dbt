
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

# DAGs configuration

email = json.loads(os.environ.get('email'))
email_on_failure = os.environ.get('email_on_failure', "False").lower() in ("true", "t", 1)
email_on_retry = os.environ.get('email_on_retry', "False").lower() in ("true", "t", 1)


# GCS Bucket, location & Folder names
# BUCKET_LOC = os.environ.get("BUCKET_LOC")
DESTINATION_PATH = os.environ.get("DESTINATION_PATH")
PROCESSED_FILE_PATH = os.environ.get("PROCESSED_FILE_PATH")
# GCS Archieve Bucket
ARCH_BUCKET = os.environ.get("ARCH_BUCKET")

DBT_CLOUD_CONN_ID = os.environ.get("DBT_CLOUD_CONN_ID")
DBT_CLOUD_CONN_SECRET = os.environ.get("DBT_CLOUD_CONN_SECRET")

LMS_SCP_SECRET = os.environ.get("LMS_SCP_SECRET")

ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")
ENCRYPTION_SECRET = os.environ.get("ENCRYPTION_SECRET")
LMS_SNOWFLAKE_CONN = os.environ.get("LMS_SNOWFLAKE_CONN")
LMS_SNOWFLAKE_SECRET = os.environ.get("LMS_SNOWFLAKE_SECRET")
MODE = os.environ.get("MODE")
audit_schema_name = os.environ.get("audit_schema_name")
DATABASE = os.environ.get("DATABASE")
