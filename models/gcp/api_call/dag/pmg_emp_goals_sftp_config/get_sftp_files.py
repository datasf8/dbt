import os
from paramiko.sftp import SFTP_NO_SUCH_FILE
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sensors.base import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
import logging


class SFTPSensor(BaseSensorOperator):

    def __init__(self, filepath, filepattern, sftp_conn_id='sftp_default', *args, **kwargs):
        super(SFTPSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.filepattern = filepattern
        self.hook = SFTPHook(sftp_conn_id)

    def poke(self, context):
        self.log.info(
            "SFTP File Path on server:- %s",
            self.filepath,

        )
        full_path = self.filepath
        file_names = []
        try:
            self.log.info(
                "File patterns: %s",
                self.filepattern,
            )
            directory = self.hook.describe_directory(full_path)
            self.log.info(
                "File Directory data: %s",
                str(directory),
            )
            self.filepattern = [self.filepattern] if isinstance(self.filepattern, str) else self.filepattern
            file_count = 0
            for filepattern in self.filepattern:
                for file in directory:
                    self.log.info(
                        "File Name: %s and File pattern: %s",
                        file,
                        filepattern,
                    )
                    if file.startswith(filepattern):
                        file_name = os.path.join(full_path, file)
                        file_names.append(file_name)
                        file_count += 1
                        context["task_instance"].xcom_push(key="full_path_{}".format(file_count), value=file_name)
                        context["task_instance"].xcom_push(key="file_name_{}".format(file_count), value=file)
                        break
                    # if not re.match(file_pattern, files):
                    #     self.log.info(files)
                    #     self.log.info(file_pattern)
                    # else:
                    #     context["task_instance"].xcom_push("file_name", files)
                    #     return True

        except IOError as e:
            if e.errno != SFTP_NO_SUCH_FILE:
                raise Exception("ERROR File(s) not found on SFTP server")
        self.hook.close_conn()
        if not file_names:
            raise Exception("File(s) not found on SFTP server")
        return file_names


class SFTPSensorPlugin(AirflowPlugin):
    name = "sftp_sensor"
    sensors = [SFTPSensor]
