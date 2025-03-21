import logging
import os.path

from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from google.cloud import storage
import io
from typing import Union
from cryptography.fernet import Fernet
from pmg_emp_goals_sftp_config.dag_config import ENCRYPTION_SECRET
from pmg_emp_goals_sftp_config.airflow_conn import get_secret
from pmg_emp_goals_sftp_config.dag_config import project_name


class EncryptionOperator(BaseOperator):
    """
    An operator which takes in a path to a zip file and unzips the contents to a location you define.

    :param path_to_zip_file: Full path to the zip file you want to Unzip
    :type path_to_zip_file: string
    :param path_to_unzip_contents: Full path to where you want to save the contents of the Zip file
        you're Unzipping
    :type path_to_unzip_contents: string
    """

    template_fields = ('bucket_name', 'file_name', 'path_to_zip_file', 'path_to_encrypt_file',
                       )
    template_ext = []

    def __init__(
            self,
            bucket_name,
            file_name,
            path_to_zip_file,
            path_to_encrypt_file,
            **kwargs):
        super().__init__(**kwargs)
        self.file_name = file_name
        self.bucket_name = bucket_name
        self.path_to_zip_file = path_to_zip_file
        self.path_to_encrypt_file = self._set_destination_path(path_to_encrypt_file)

    def key_load(self):
        key = get_secret(project_name, ENCRYPTION_SECRET)
        return key

    def file_encrypt(self, key, blob, bucket):
        f = Fernet(key)
        encrypted = f.encrypt(blob)
        encrypt_file_name = "encrypt_" + self.file_name
        blob = bucket.blob(self.path_to_encrypt_file + "/" + encrypt_file_name)
        blob.upload_from_string(encrypted)

    def delete_original_file(self, blob):
        blob.delete()

    def execute(self, context):
        # No check is done if the zip file is valid so that the operator fails when expected so that
        # airflow can properly mark the task as failed and schedule retries as needed

        # for file_obj in self.list_of_files:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(f'{self.path_to_zip_file}/{self.file_name}')
        zipbytes = blob.download_as_string()
        key = self.key_load()
        self.file_encrypt(key, zipbytes, bucket)
        self.delete_original_file(blob)


    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")

    @staticmethod
    def _set_destination_path(path: Union[str, None]) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""


class DecryptionOperator(BaseOperator):
    """
    An operator which takes in a path to encrypted zip file and Decrypt the contents to a location you define.

    :param path_to_zip_file: Full path to the zip file you want to Decrypt
    :type path_to_zip_file: string
    :param path_to_unzip_contents: Full path to where you want to save the contents of the Zip file
        you're Unzipping
    :type path_to_unzip_contents: string
    """

    template_fields = ('bucket_name', 'file_name', 'path_to_zip_file', 'path_to_decrypt_file',
                       )
    template_ext = []

    def __init__(
            self,
            bucket_name,
            file_name,
            path_to_zip_file,
            path_to_decrypt_file,
            **kwargs):
        super().__init__(**kwargs)
        self.file_name = file_name
        self.bucket_name = bucket_name
        self.path_to_zip_file = path_to_zip_file
        self.path_to_decrypt_file = self._set_destination_path(path_to_decrypt_file)

    def key_load(self):
        key = get_secret(project_name, ENCRYPTION_SECRET)
        return key

    def file_decrypt(self, key, blob, bucket):
        f = Fernet(key)
        decrypted = f.decrypt(blob)
        # decrypt_file_name = self.file_name.split("_", 1)[1]
        blob = bucket.blob(self.path_to_decrypt_file + "/" + self.file_name)
        blob.upload_from_string(decrypted)

    def delete_original_file(self, blob):
        blob.delete()

    def execute(self, context):
        # No check is done if the zip file is valid so that the operator fails when expected so that
        # airflow can properly mark the task as failed and schedule retries as needed

        # for file_obj in self.list_of_files:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(f'{self.path_to_zip_file}/encrypt_{self.file_name}')
        zipbytes = blob.download_as_string()
        key = self.key_load()
        self.file_decrypt(key, zipbytes, bucket)
        self.delete_original_file(blob)


    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")

    @staticmethod
    def _set_destination_path(path: Union[str, None]) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""


# Defining the plugin class
class ZipOperatorPlugin(AirflowPlugin):
    name = "zip_operator_plugin"
    operators = [EncryptionOperator, DecryptionOperator]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
