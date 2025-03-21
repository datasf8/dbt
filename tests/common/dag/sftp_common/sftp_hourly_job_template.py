import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
import os
from google.cloud import storage
from datetime import timedelta,datetime
import logging
from sftp_common.utils import *
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.sftp.hooks.sftp import SFTPHook
# from sftp_common.get_sftp_files import SFTPSensor
from sftp_common.snowflake_operator import call_snowflake_operator, snowflake_truncate_table, store_start_time_in_snowflake
from sftp_common.encryption_decryption_operator import EncryptionOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from sftp_common import dag_config
from airflow.utils.trigger_rule import TriggerRule


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def store_start_time_in_xcom(**context):
    today_date = datetime.now()
    context["task_instance"].xcom_push(key="job_start_time", value=today_date.strftime("%Y-%m-%d %H:%M:%S"))


def xls_to_csv(bucket_name, file_name, input_excel_file_location, output_csv_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    full_file_path = os.path.join(input_excel_file_location, file_name)
    full_output_csv_path = os.path.join(output_csv_path, file_name + ".csv")
    blob = bucket.blob(full_file_path)
    data_bytes = blob.download_as_bytes()
    excel_file = pd.read_excel(data_bytes, 0)
    bucket.blob(full_output_csv_path).upload_from_string(excel_file.to_csv(index=None, header=True,
                                                                           compression='gzip', quotechar='"',
                                                                           doublequote=True, line_terminator='\n'),
                                                         'text/csv')


def delete_sftp_file(config):
    source_path = config.SOURCE_PATH
    file_list = get_file_patterns(config)
    for file in file_list:
        file_name = file
        sftp_conn_id = config.LMS_SCP_CONN
        sftp_hook = SFTPHook(sftp_conn_id)
        sftp_hook.delete_file(os.path.join(source_path, file_name))


def add_current_hour_in_xcom(**context):
    today_date = datetime.today()
    hour = str(today_date.hour).zfill(2)
    context["task_instance"].xcom_push(key="hour", value=hour)


def archive_gcs_file(config, zip_file_folder_list, **context):
    bucket_name = config.DESTINATION_BUCKET
    file_list = get_file_patterns(config)
    hour = context["task_instance"].xcom_pull(key="hour")
    for file in file_list:
        file_name = file
        storage_client = storage.Client()
        today_date = datetime.now().strftime("%Y%m%d%H%M%S")
        bucket = storage_client.bucket(bucket_name)
        file_path = f'{dag_config.DESTINATION_PATH}/{config.ZIP_FILE_PATH}/{zip_file_folder_list[0]}/{hour}/{file_name}'
        file_name_list = file_name.split(".")
        target_file_name = file_name_list[0] + "_" + str(today_date) + "." + file_name_list[1]
        target_path = f'{dag_config.DESTINATION_PATH}/{config.ZIP_FILE_PATH}/{zip_file_folder_list[0]}/{hour}/{target_file_name}'
        logging.info(f"TARGET_PATH: {target_path}")
        logging.info(f"SOURCE_PATH: {file_path}")
        blob = bucket.blob(file_path)
        bucket.copy_blob(blob, bucket, target_path)
        blob.delete()


def store_start_time_in_xcom(**context):
    today_date = datetime.now()
    context["task_instance"].xcom_push(key="job_start_time", value=today_date.strftime("%Y-%m-%d %H:%M:%S"))


def get_sftp_dag_details(dag, config):
    load_to_dbt_cloud = object
    delete_sftp_file_t = object
    add_hour_in_xcom = PythonOperator(
        task_id="add_current_hour_in_xcom",
        python_callable=add_current_hour_in_xcom,
        # op_kwargs={"config": config, "zip_file_folder_list": zip_file_folder_list},
        provide_context=True,
        dag=dag
    )
    try:
        task_list = ["add_current_hour_in_xcom"]
        file_task = []
        zip_file_folder_list = get_folder_list(config, True)
        for i, file in enumerate(get_file_patterns(config)):
            internal_task = []

            get_file_name_from_sftp = SFTPSensor(
                task_id="Check_file_on_sftp_server_" + str(i+1),
                sftp_conn_id=config.LMS_SCP_CONN,
                path=os.path.join(config.SOURCE_PATH, file),
                file_pattern='',
                timeout=timedelta(minutes=config.task_timeout).seconds,
                # timeout=120,
                mode=dag_config.MODE,
                poke_interval=int(config.POKE_INTERVAL) if config.POKE_INTERVAL else None,
                soft_fail=False,
                # depends_on_past=True,
                dag=dag
            )
            internal_task.append(get_file_name_from_sftp)
            task_list.append("Check_file_on_sftp_server_" + str(i + 1))
            if config.IS_LOAD_SNOWFLAKE:
                truncate_snowflake_table = PythonOperator(
                    task_id="truncate_snowflake_table_" + str(i+1),
                    python_callable=snowflake_truncate_table,
                    depends_on_past=False,
                    op_kwargs={
                        "files_to_be_copy": get_unzip_file_details(config.FILE_DETAILS, i),
                    },
                    provide_context=True,
                    dag=dag

                )
                internal_task.append(truncate_snowflake_table)
                task_list.append("truncate_snowflake_table_" + str(i+1))
            # if '{{task_instance.xcom_pull(key="file_name_' + str(i+1) + '")}}':

            sftp_operator_obj = SFTPToGCSOperator(
                task_id="file-copy-sftp-to-gcs_" + str(i+1),
                sftp_conn_id=config.LMS_SCP_CONN,
                depends_on_past=False,
                # dag_config.OBJECT_NAME),
                source_path=os.path.join(config.SOURCE_PATH, file),
                destination_bucket=os.environ.get("GCS_BUCKET_NAME", config.DESTINATION_BUCKET),
                destination_path=f'{dag_config.DESTINATION_PATH}/{config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}/'\
                                 + '{{task_instance.xcom_pull(key="hour")}}' + f'/{file}',
                dag=dag
                )
            internal_task.append(sftp_operator_obj)
            task_list.append("file-copy-sftp-to-gcs_" + str(i+1))
            if config.IS_EXCEL_CONVERT:
                excel_to_csv = PythonOperator(
                    task_id="excel_to_csv_to_GCS_" + str(i+1),
                    python_callable=xls_to_csv,
                    depends_on_past=False,
                    op_kwargs={
                        "bucket_name": os.environ.get("GCS_BUCKET_NAME", config.DESTINATION_BUCKET),
                        "file_name": file,
                        "input_excel_file_location": f'{dag_config.DESTINATION_PATH}/{config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}',
                        "output_csv_path": f'{dag_config.DESTINATION_PATH}/{config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}'
                    },
                    dag=dag
                )
                internal_task.append(excel_to_csv)
                task_list.append("excel_to_csv_to_GCS_" + str(i+1))
            if config.IS_LOAD_SNOWFLAKE:
                snowflake_data_load = PythonOperator(
                    task_id="GCS_to_Snowflake_" + str(i + 1),
                    python_callable=call_snowflake_operator,
                    op_kwargs={
                        "config": config,
                        "bucket_name": os.environ.get("GCS_BUCKET_NAME", config.DESTINATION_BUCKET),
                        "parent_folder": f'{dag_config.DESTINATION_PATH}/{config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}',
                        "file_name": file,
                        "files_to_be_copy": get_unzip_file_details(config.FILE_DETAILS, i)
                    },
                    depends_on_past=False,
                    dag=dag

                )
                internal_task.append(snowflake_data_load)
                task_list.append("GCS_to_Snowflake_" + str(i + 1))

            if config.IS_ENCRYPT_FILE:
                encrypt_files = EncryptionOperator(
                    task_id="Encrypt_zip_" + str(i+1),
                    bucket_name=os.environ.get("GCS_BUCKET_NAME", config.DESTINATION_BUCKET),
                    file_name=file,
                    path_to_zip_file=f'{dag_config.DESTINATION_PATH}/{config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}',
                    path_to_encrypt_file=f'{dag_config.DESTINATION_PATH}/{config.ZIP_FILE_PATH}/{zip_file_folder_list[i]}',
                    dag=dag
                )
                internal_task.append(encrypt_files)
                task_list.append("Encrypt_zip_" + str(i+1))

                # decrypt_files = DecryptionOperator(
                #     task_id="Decrypt_zip_" + str(i+1),
                #     bucket_name=os.environ.get("GCS_BUCKET_NAME", dag_config.DESTINATION_BUCKET),
                #     file_name='{{task_instance.xcom_pull(key="file_name_' + str(i+1) + '")}}',
                #     path_to_zip_file=f'{dag_config.DESTINATION_PATH}/{zip_file_folder_list[i]}',
                #     path_to_decrypt_file=f'{dag_config.DESTINATION_PATH}/{zip_file_folder_list[i]}',
                # )
                # internal_task.append(decrypt_files)

            file_task.append(internal_task)

        archive_file = PythonOperator(
            task_id="archive_gcs_file",
            python_callable=archive_gcs_file,
            op_kwargs={"config": config, "zip_file_folder_list": zip_file_folder_list},
            provide_context=True,
            dag=dag
        )
        task_list.append("archive_gcs_file")
        # internal_task.append(archive_delete_sftp_file)
        if config.IS_DBT_LOAD:
            load_to_dbt_cloud = DbtCloudRunJobOperator(
                task_id="load_to_dbt_cloud",
                job_id=config.DBT_CLOUD_JOB_ID,
                wait_for_termination=True,
                additional_run_config={"threads_override": 8},
                depends_on_past=False,
            )
            task_list.append("load_to_dbt_cloud")
        if config.IS_DELETE_SFTP_FILE:
            delete_sftp_file_t = PythonOperator(
                task_id="delete_sftp_file",
                python_callable=delete_sftp_file,
                op_kwargs={"config": config},
                dag=dag
            )
            task_list.append("delete_sftp_file")
        store_start_time_in = PythonOperator(
            task_id="store_start_time_in_xcom",
            python_callable=store_start_time_in_xcom,
            op_kwargs={},
            dag=dag
        )
        task_list.extend(["store_start_time_in_xcom", "store_job_details_in_snowflake"])
        schema_name = dag_config.audit_schema_name
        store_job_details_in_snowflake = PythonOperator(
            task_id="store_job_details_in_snowflake",
            python_callable=store_start_time_in_snowflake,
            op_kwargs={"database_name": dag_config.DATABASE, "task_id": "store_job_details_in_snowflake",
                       "task_list": task_list, "table_name": "JOB_EXECUTION_LOG", "schema_name": schema_name,
                       "job_name": config.job_name},
            # do_xcom_push=True,
            provide_context=True,
            dag=dag,
            trigger_rule=TriggerRule.ALL_DONE
        )
        b = []
        for task in file_task:
            a = store_start_time_in >> add_hour_in_xcom
            # a = get_file_name_from_sftp
            # if config.IS_LOAD_SNOWFLAKE:
            #     a = a >> truncate_snowflake_table
            for t in task:
                if not a:
                    a = t
                else:
                    a = a >> t
            if config.IS_DBT_LOAD:
                a = a >> load_to_dbt_cloud
            # if config.IS_DELETE_SFTP_FILE:
            a = a >> archive_file
            if config.IS_DELETE_SFTP_FILE:
                a = a >> delete_sftp_file_t
            a >> store_job_details_in_snowflake
            b.append(a)
        # b = add_hour_in_xcom >> b
        return b
    except IndexError as ex:
        logging.debug("Exception", str(ex))
        raise Exception(ex)
