
import ast
from datetime import datetime,timedelta
import logging
from pmg_emp_dev_goals_sftp_config import dag_config
from pmg_emp_dev_goals_sftp_config.airflow_conn import airflow_connections
from pmg_emp_dev_goals_sftp_config.dag_config import LMS_SCP_SECRET, LMS_SCP_CONN, FILE_DETAILS, \
                                                        LMS_SNOWFLAKE_CONN, LMS_SNOWFLAKE_SECRET, \
                                                        DBT_CLOUD_CONN_ID, DBT_CLOUD_CONN_SECRET

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

airflow_connections(LMS_SCP_CONN, LMS_SCP_SECRET)
airflow_connections(LMS_SNOWFLAKE_CONN, LMS_SNOWFLAKE_SECRET)
airflow_connections(DBT_CLOUD_CONN_ID, DBT_CLOUD_CONN_SECRET)

def get_file_patterns():
    env_name = dag_config.FILE_PATTERN
    today_date = (datetime.today()-timedelta(days=1)).strftime("%Y%m%d")
    previous_date = dag_config.PREVIOUS_DATE
    if previous_date:
        previous_date = datetime.strptime(previous_date, "%Y-%m-%d")
        today_date = previous_date.strftime("%Y%m%d")
    file_patterns = []
    file_patterns.append(env_name + today_date)
    # file_list = sftp_config.FILE_LIST
    # if file_list:
    #     file_patterns = file_list
    return file_patterns


def get_folder_list():
    previous_date = dag_config.PREVIOUS_DATE
    today_date = datetime.today()-timedelta(days=1)
    if previous_date:
        previous_date = datetime.strptime(previous_date, "%Y-%m-%d")
        today_date = previous_date
    year = str(today_date.year)
    month = str(today_date.month).zfill(2)
    day = str(today_date.day).zfill(2)
    folder_list = []
    folder_list.append(year + "/" + month + "/" + day)
    return folder_list


def get_unzip_file_details():
    file_details = ast.literal_eval(FILE_DETAILS) if FILE_DETAILS else []
    logger.info("FILES DETAILS:" + str(file_details))
    file_names = []
    if file_details:
        for file_obj in file_details:
            file_names.append(file_obj)
    return file_names

