import os
import json
from os.path import join, dirname
from dotenv import load_dotenv
import configparser


# Project Configurations
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

# New configuration

DESTINATION_PATH = os.environ.get("DESTINATION_PATH")
JOB_DETAILS = json.loads(os.environ.get("JOB_DETAILS")) if os.environ.get("JOB_DETAILS") else {}


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
