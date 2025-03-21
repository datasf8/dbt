import os
import json
from google.cloud import secretmanager
from os.path import join, dirname
from dotenv import load_dotenv
# import logging


# Project Configurations
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

run_locally = os.environ.get("run_locally", "False").lower() in ("true", "t", 1)
is_local = os.environ.get("is_local", "False").lower() in ("true", "t", 1)
DELIMITER = os.environ.get("DELIMITER")
BUCKET = os.environ.get("BUCKET")
PROJECT = os.environ.get("PROJECT")
SECRET_NAME = os.environ.get("SECRET_NAME")
FOLDER_PATH = os.environ.get("FOLDER_PATH")
SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT")
SUB_NETWORK = os.environ.get("SUB_NETWORK")
PROJECT_PATH = os.environ.get("PROJECT_PATH")
# MAIN_URL = os.environ.get("MAIN_URL")
EMPLOYEE_REVIEW_URLS = os.environ.get("EMPLOYEE_REVIEW_URLS")
# STEP_DATA = json.loads(os.environ.get("STEP_DATA"))
# STEP_TYPES = json.loads(os.environ.get("STEP_TYPES"))
STEP_DATA = {"1": "PE", "3": "YE", '4': "MGR", "5": "SGN", "completed": "completed"}
STEP_TYPES = ["1", "3", '4', "5", "completed"]
MAIN_URL = os.environ.get("MAIN_URL")


def get_secret(project, secret_name, version='latest'):
    client = secretmanager.SecretManagerServiceClient()
    secret_path = client.secret_version_path(project, secret_name, version)
    secret = client.access_secret_version(name=secret_path)
    secret_data = secret.payload.data.decode('UTF-8')
    return json.loads(secret_data)


def get_authentication(is_local=False):
    secret_details = get_secret(PROJECT, SECRET_NAME)
    if secret_details.get("is_auth"):
        return {"auth": (secret_details.get("user_name"), secret_details.get("password"))}
    return {"headers": {"Authorization": secret_details.get("authorization_token")}}


def get_rls_authentication(is_local=False):
    secret_details = get_secret(PROJECT, SECRET_NAME)
    if secret_details.get("is_auth"):
        return {"auth": (secret_details.get("user_name"), secret_details.get("password"))}
    return {"headers": {"Authorization": secret_details.get("authorization_token")}}
