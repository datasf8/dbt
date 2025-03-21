from airflow import DAG
import pandas as pd
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
import os
from google.cloud import storage
from pmg_emp_pe_comments_config.utils import *
from pmg_emp_pe_comments_config.get_sftp_files import SFTPSensor
from pmg_emp_pe_comments_config.snowflake_operator import call_snowflake_operator, snowflake_truncate_table
from pmg_emp_pe_comments_config.dag_config import LMS_SCP_CONN
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudGetJobRunArtifactOperator,
    DbtCloudRunJobOperator,
)
from pmg_emp_pe_comments_config import dag_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# def xls_to_csv(bucket_name, file_name, input_excel_file_location, output_csv_path):
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     full_file_path=os.path.join(input_excel_file_location, file_name)
#     full_output_csv_path=os.path.join(output_csv_path, file_name + ".csv")
#     blob = bucket.blob(full_file_path)
#     data_bytes = blob.download_as_bytes()
#     excelFile = pd.read_excel(data_bytes)
#     bucket.blob(full_output_csv_path).upload_from_string(excelFile.to_csv(index=None, header=True,  compression='gzip', quotechar='"', doublequote=True, line_terminator='\n'), 'text/csv')
#

default_args = {
    'owner': 'airflow',
    'depends_on_past': dag_config.depends_on_past,
    'start_date': dag_config.start_date,
    'email': dag_config.email,
    'email_on_failure': dag_config.email_on_failure,
    'email_on_retry': dag_config.email_on_retry,
    'retries': dag_config.retries,
    'retry_delay': timedelta(minutes=dag_config.retry_delay),
    'execution_timeout': timedelta(minutes=dag_config.execution_timeout),
    "dbt_cloud_conn_id": dag_config.DBT_CLOUD_CONN_ID,
    "account_id": dag_config.DBT_CLOUD_ACCOUNT_ID
}

truncate_snowflake_table = load_to_dbt_cloud = None
try:
    with DAG(
            dag_config.job_name,
            default_args=default_args,
            description="DAG for copying data from SFTP server for pmgm employee PE comments",
            tags=['pmgm'],
            max_active_runs=dag_config.max_active_runs,
            concurrency=dag_config.concurrency,
            catchup=dag_config.catchup,
            dagrun_timeout=timedelta(minutes=dag_config.dagrun_timeout),
            schedule_interval=dag_config.schedule_interval

    ) as dag:
        get_file_name_from_sftp = SFTPSensor(
            task_id="get_filelist_from_SFTP",
            sftp_conn_id=LMS_SCP_CONN,
            filepath=os.path.join(dag_config.SOURCE_PATH),
            filepattern=get_file_patterns(),
            # depends_on_past=True,
        )
        if dag_config.IS_LOAD_SNOWFLAKE:
            truncate_snowflake_table = PythonOperator(
                task_id="truncate_snowflake_table",
                python_callable=snowflake_truncate_table,
                depends_on_past=dag_config.depends_on_past,
                op_kwargs={
                    "files_to_be_copy": get_unzip_file_details()
                },

            )
        file_task = []
        zip_file_folder_list = get_folder_list()
        for i, file in enumerate(get_file_patterns()):
            # if '{{task_instance.xcom_pull(key="file_name_' + str(i+1) + '")}}':
            internal_task = []
            Sftp = SFTPToGCSOperator(
                task_id="file-copy-sftp-to-gcs_" + str(i+1),
                sftp_conn_id=LMS_SCP_CONN,
                depends_on_past=dag_config.depends_on_past,
                # dag_config.OBJECT_NAME),
                source_path='{{task_instance.xcom_pull(key="full_path_' + str(i+1) + '")}}',
                destination_bucket=os.environ.get("GCS_BUCKET_NAME", dag_config.DESTINATION_BUCKET),
                destination_path=f'{dag_config.DESTINATION_PATH}/{dag_config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}/' \
                                 + '{{task_instance.xcom_pull(key="file_name_' + str(i+1) + '")}}',
                )
            # excel_to_csv = PythonOperator(
            #     task_id="excel_to_csv_to_GCS_" + str(i+1),
            #     python_callable=xls_to_csv,
            #     depends_on_past=dag_config.depends_on_past,
            #     op_kwargs={
            #         "bucket_name": os.environ.get("GCS_BUCKET_NAME", dag_config.DESTINATION_BUCKET),
            #         "file_name": '{{task_instance.xcom_pull(key="file_name_' + str(i + 1) + '")}}',
            #         "input_excel_file_location": f'{dag_config.DESTINATION_PATH}/{dag_config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}',
            #         "output_csv_path": f'{dag_config.DESTINATION_PATH}/{dag_config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}'
            #     }
            # )
            internal_task.append(Sftp)
            # internal_task.append(excel_to_csv)
            if dag_config.IS_LOAD_SNOWFLAKE:
                snowflake_data_load = PythonOperator(
                    task_id="GCS_to_Snowflake_" + str(i + 1),
                    python_callable=call_snowflake_operator,
                    op_kwargs={
                        "bucket_name": os.environ.get("GCS_BUCKET_NAME", dag_config.DESTINATION_BUCKET),
                        "parent_folder": f'{dag_config.DESTINATION_PATH}/{dag_config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}',
                        "file_name": f'{dag_config.DESTINATION_PATH}/{dag_config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}/' \
                                 + '{{task_instance.xcom_pull(key="file_name_' + str(i+1) + '")}}',
                        "files_to_be_copy": get_unzip_file_details(),
                    },
                    depends_on_past=dag_config.depends_on_past,

                )
                internal_task.append(snowflake_data_load)

            if dag_config.IS_ENCRYPT_FILE:
                encrypt_files = EncryptionOperator(
                    task_id="Encrypt_zip_" + str(i+1),
                    bucket_name=os.environ.get("GCS_BUCKET_NAME", dag_config.DESTINATION_BUCKET),
                    file_name='{{task_instance.xcom_pull(key="file_name_' + str(i+1) + '")}}',
                    path_to_zip_file=f'{dag_config.DESTINATION_PATH}/{dag_config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}',
                    path_to_encrypt_file=f'{dag_config.DESTINATION_PATH}/{dag_config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}',
                )
                internal_task.append(encrypt_files)

                # decrypt_files = DecryptionOperator(
                #     task_id="Decrypt_zip_" + str(i+1),
                #     bucket_name=os.environ.get("GCS_BUCKET_NAME", dag_config.DESTINATION_BUCKET),
                #     file_name='{{task_instance.xcom_pull(key="file_name_' + str(i+1) + '")}}',
                #     path_to_zip_file=f'{dag_config.DESTINATION_PATH}/{zip_file_folder_list[i]}',
                #     path_to_decrypt_file=f'{dag_config.DESTINATION_PATH}/{zip_file_folder_list[i]}',
                # )
                # internal_task.append(decrypt_files)

            file_task.append(internal_task)
        if dag_config.IS_DBT_LOAD:
            load_to_dbt_cloud = DbtCloudRunJobOperator(
                task_id="load_to_dbt_cloud",
                job_id=dag_config.DBT_CLOUD_JOB_ID,
                wait_for_termination=True,
                additional_run_config={"threads_override": 8},
                depends_on_past=dag_config.depends_on_past,
            )

except IndexError as ex:
    logging.debug("Exception", str(ex))

a = None
for task in file_task:
    a = get_file_name_from_sftp
    if dag_config.IS_LOAD_SNOWFLAKE:
        a = a >> truncate_snowflake_table
    for t in task:
        a = a >> t
    if dag_config.IS_DBT_LOAD:
        a >> load_to_dbt_cloud
