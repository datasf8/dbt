import json
from airflow import settings
from airflow.models.connection import Connection
import logging
from google.cloud import secretmanager
import ast
import sys
import os
from pmg_emp_pe_comments_config.dag_config import project_name

# projects/326025982749/secrets/sftp_gcp_dv/versions/1


def get_secret(project, secret_name, version='latest'):
        client = secretmanager.SecretManagerServiceClient()
        secret_path = client.secret_version_path(project, secret_name, version)
        secret = client.access_secret_version(name=secret_path)
        return secret.payload.data.decode('UTF-8')


def create_new_connection(conn_id, secret_name, version):
    sys.stdout = open(os.devnull, 'w')
    project = project_name
    if not version:
        version = "latest"
    plaintext_secret = get_secret(project, secret_name, version)

    if plaintext_secret:
        conn_json = json.loads(plaintext_secret)
        conn_json["conn_id"] = conn_id
        conn = Connection(
            **conn_json
        )
        session = settings.Session()  # get the session
        session.add(conn)
        session.commit()
        conn = plaintext_secret = conn_json = None
        sys.stdout = sys.__stdout__
    return "Connection created Successfully"


def airflow_connections(conn_id, secret_name, version="latest"):
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if str(conn_name) == str(conn_id):
        logging.warning(f"Connection {conn_id} already exists please delete connection in Airflow to replace settings")
    else:
        create_new_connection(conn_id, secret_name, version)

# a = {
#     "conn_id": "",
#     "conn_type": "snowflake",
#     "description": "Snowflake Connection for DEV server",
#     "host": "https://dj54893.europe-west4.gcp.snowflakecomputing.com/",
#     "login": "ASEEEEEEE",
#     "password": "CAASSSaa",
#     "extra": {"extra__snowflake__account": "dj54893", "extra__snowflake__aws_access_key_id": "",
#               "extra__snowflake__aws_secret_access_key": "", "extra__snowflake__database": "DEMO_DB",
#               "extra__snowflake__region": "europe-west4.gcp", "extra__snowflake__role": "HRDP_DV_DOMAIN_ADMIN",
#               "extra__snowflake__warehouse": "HRDP_DBT_BASE_WH"}
# }


# b = {
#     "conn_id": "",
#     "conn_type": "sftp",
#     "description": "SFTP connection to connect server",
#     "host": "sftp55.sapsf.eu",
#     "login": "lorealsa01",
#     "password": "*******",
#     "port": 22,
#     "extra": {"no_host_key_check": true}
# }

# b = {
#     "conn_id": "",
#     "conn_type": "dbt_cloud",
#     "description": "DBT cloud connection to connect DBT server",
#     "account_id": 1111111,
#     "api_token": "AAAAAAA",
#     "extra": {}
# }