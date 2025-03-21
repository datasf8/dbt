import time
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import requests
from google.cloud import storage
import os
import json
from google.cloud import secretmanager
from os.path import join, dirname
from dotenv import load_dotenv

# sys.path.append('/home/airflow/gcs/dags/pipeline_pmgm/')
# from pipeline_pmgm_config import dag_config as config


class config:
    # import logging
    # Project Configurations
    dotenv_path = join(dirname(__file__), 'pipeline_pmgm_config/.env')
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
    secret_details = get_secret(config.PROJECT, config.SECRET_NAME)
    if secret_details.get("is_auth"):
        return {"auth": (secret_details.get("user_name"), secret_details.get("password"))}
    return {"headers": {"Authorization": secret_details.get("authorization_token")}}


class MainApiCall(beam.DoFn):

    def __init__(self, headers, bucket, main_url, folder_path, step_data, step_types):
        self.headers = headers
        self.bucket = bucket
        self.main_url = main_url
        self.path = None
        self.folder_path = folder_path
        self.step_data = step_data
        self.step_types = step_types

    def process(self, input_uri):
        # print(f"input_uri : {input_uri}")
        # print(f"input_uri type : {type(input_uri)}")
        logging.info(f"input_uri : {input_uri}")
        logging.info(f"input_uri type : {type(input_uri)}")
        folder = input_uri[0]
        uri = input_uri[1]
        employee_id = input_uri[6]
        current_time = input_uri[7]
        hour = input_uri[8]
        self.path = input_uri[2]
        # print(f"folder : {folder}")
        # print(f"folder type : {type(folder)}")
        logging.info(f"folder : {folder}")
        logging.info(f"folder type : {type(folder)}")
        logging.info(f"uri2 : {uri}")
        logging.info(f"uri2 type : {type(uri)}")
        today_date = datetime.today()
        year = str(today_date.year)
        month = str(today_date.month).zfill(2)
        day = str(today_date.day).zfill(2)
        db_file_path = self.path
        modified_date_data = {}
        db_file_name = "employee_forms_modified_date.dat"
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket)
        if bucket.blob(os.path.join(db_file_path, db_file_name)).exists():
            db_file_blob = bucket.blob(os.path.join(db_file_path, db_file_name))
            with db_file_blob.open("r") as myfile:
                for line in myfile:
                    line = line.strip("\n")
                    l1 = [line.split(",")]
                    modified_date_data.update(dict(l1))
        modified_date_data = dict(modified_date_data) if modified_date_data else {}
        if not modified_date_data:
            modified_date_data = {"EmployeeLastRun": "1900-04-01T00:00:00"}
        date_filter = f"lastModifiedDateTime le datetimeoffset'{current_time}'"
        if modified_date_data:
            date_filter += f" and lastModifiedDateTime gt datetimeoffset'{modified_date_data.get('EmployeeLastRun')}'"
        # uri = uri + f"&$filter={date_filter}"
        modified_date_data["FormDataIdLastRun"] = current_time
        self.path = str(os.path.join(self.path, year, month, day, hour))
        employee_urls, employee_url_lst = self.api_rcall(uri, employee_id, current_time, hour)
        file_path = os.path.join(self.path, "sdds_employee_forms",
                                 f"sdds_employee_review{employee_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}")
        blob = bucket.blob(file_path)
        count = 3
        is_success = False
        msg = ""
        while count > 0:
            try:
                count -= 1
                blob.upload_from_string(employee_urls)
                is_success = True
                break
            except Exception as exc:
                time.sleep(120)
                msg = exc.__str__()
                logging.warning(f'Error while uploading data to GCS {file_path} : {exc.__str__()}')
        if not is_success:
            raise Exception(
                f'Error while uploading data to GCS {file_path} after 3 retries error: {msg}')
        logging.info(employee_url_lst)
        return employee_url_lst

    def api_rcall(self, url, employee_id, current_time, hour):
        user_file_name = f"{employee_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
        count = 3
        is_success = False
        while count > 0:
            count -= 1
            try:
                f = requests.get(url, timeout=30, **self.headers)
                status_code = f.status_code
                if str(status_code) == '404':
                    break
                f.raise_for_status()
                file_data = f.json()
                is_success = True
                break
            except Exception as exc:
                time.sleep(120)
                logging.warning(f'Error while Retrieving data from Source API {url} : {exc.__str__()}')
        if not is_success:
            raise Exception(f'Error while Retrieving data from Source API {url} after 3 retries')
        employee_url_lst = []
        employee_url_data = ""
        form_content_data = {}

        if not file_data.get("d"):
            logging.warning(f"content id {employee_id} and data is: {file_data}")
            return employee_url_data, employee_url_lst
        for content_dict in file_data["d"]["results"]:
            form_content_step = content_dict.get("formContentAssociatedStepId")
            # logging.info(f"formContentAssociatedStepId: {form_content_step}")
            if form_content_step in self.step_types:
                if form_content_step in form_content_data:
                    last_modified_date = form_content_data[form_content_step].get("auditTrailLastModified")
                    audit_trial_id = form_content_data[form_content_step].get("auditTrailId")
                    if last_modified_date == content_dict.get("auditTrailLastModified")  \
                            and audit_trial_id < content_dict.get("auditTrailId"):
                        form_content_data[form_content_step] = content_dict
                    elif last_modified_date < content_dict.get("auditTrailLastModified"):
                        form_content_data[form_content_step] = content_dict
                else:
                    form_content_data[form_content_step] = content_dict

        content_data_list = []
        logging.info(f"FORM_CONTENT_DATA: {form_content_data}")
        for form_content_step_id in form_content_data:
            path = self.folder_path
            logging.info(f"Folder Path from ENV: {path}")
            # path = "successfactors/PMG"
            # step_data = {"2": "PE", "5": "YE", "S1": "SGN"}
            step_data = self.step_data
            logging.info(f"formContentAssociatedStepId(1): {form_content_step_id}")
            folder_name = step_data.get(str(form_content_step_id))
            logging.info(f"Folder Name type: {folder_name}")
            # path = os.path.join(path, folder_name)
            content_dict = form_content_data.get(form_content_step_id)
            content_data_list.append(content_dict)
            form_content_id = content_dict.get("formContentId")
            user_url = ""
            if folder_name != "completed":
                # if folder_name == "PE":
                if folder_name in ("PE", "YE", "MGR"):
                    logging.info(f"PE Folder Name: {folder_name}")
                    logging.info(f"formContentAssociatedStepId(1): {form_content_step_id}")
                    user_url = f"FORMOBJCOMPSUMMARYSECTION|*|{self.main_url}/odata/v2/FormObjCompSummarySection(formContentId={form_content_id}L,formDataId={employee_id}L)/?$format=json&paging=cursor&$expand=overallCompRating,overallObjRating"
                    user_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMOBJCOMPSUMMARYSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(user_url)
                    if employee_url_data:
                        employee_url_data += '\n'
                    employee_url_data += user_url
                    overall_rating_url = f"FORMSUMMARYSECTION|*|{self.main_url}/odata/v2/FormSummarySection(formContentId={form_content_id}L,formDataId={employee_id}L)/overallFormRating?$format=json&paging=cursor"
                    overall_rating_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMSUMMARYSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(overall_rating_url)
                    if employee_url_data:
                        employee_url_data += '\n'
                    employee_url_data += user_url
                if folder_name in ("PE", "YE"):
                    user_url = f"FORMCUSTOMSECTION|*|{self.main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=14)/?$format=json&paging=cursor&$expand=othersRatingComment, customElement"
                    user_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMCUSTOMSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(user_url)
                    if employee_url_data:
                        employee_url_data += '\n'
                    employee_url_data += user_url
                if folder_name in ("YE", "MGR"):
                    user_url = f"FORMCUSTOMSECTION|*|{self.main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=12)/?$format=json&paging=cursor&$expand=othersRatingComment, customElement"
                    user_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMCUSTOMSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(user_url)
                    if employee_url_data:
                        employee_url_data += '\n'
                    employee_url_data += user_url
                if folder_name == "MGR":
                    user_url = f"FORMCUSTOMSECTION|*|{self.main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=13)/?$format=json&paging=cursor&$expand=othersRatingComment, customElement"
                    user_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMCUSTOMSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(user_url)
                    if employee_url_data:
                        employee_url_data += '\n'
                    employee_url_data += user_url
                if folder_name == "SGN":
                    user_url = f"FORMCUSTOMSECTION|*|{self.main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=19)/?$format=json&paging=cursor&$expand=customElement,othersRatingComment"
                    user_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMCUSTOMSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(user_url)
                if employee_url_data:
                    employee_url_data += "\n"
                employee_url_data += user_url
        file_path = os.path.join(self.path, "FORMAUDITTRAILS", user_file_name)
        if content_data_list:
            file_data = {"d": {"results": content_data_list}}
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.bucket)
            blob = bucket.blob(file_path)
            count = 3
            is_success = False
            msg = ""
            while count > 0:
                try:
                    count -= 1
                    blob.upload_from_string(json.dumps(file_data))
                    # blob.upload_from_string(url_data)
                    is_success = True
                    break
                except Exception as exc:
                    time.sleep(120)
                    msg = exc.__str__()
                    logging.warning(f'Error while uploading data to GCS {file_path} : {exc.__str__()}')
            if not is_success:
                raise Exception(
                    f'Error while uploading data to GCS {file_path} after 3 retries error: {msg}')
        return employee_url_data, employee_url_lst


class CallAPI(beam.DoFn):

    def __init__(self, headers):
        self.headers = headers

    def process(self, input_uri):
        try:
            # print(f"input_uri : {input_uri}")
            # print(f"input_uri type : {type(input_uri)}")
            logging.info(f"input_uri : {input_uri}")
            logging.info(f"input_uri type : {type(input_uri)}")

            folder = input_uri[0]
            uri = input_uri[1]
            path = input_uri[2]
            content_id = input_uri[6]
            step_folder_name = input_uri[8]
            hour = input_uri[9]
            # # print(f"folder : {folder}")
            # # print(f"folder type : {type(folder)}")
            logging.info(f"folder : {folder}")
            logging.info(f"folder type : {type(folder)}")

            # # print(f"uri2 : {uri}")
            # # print(f"uri2 type : {type(uri)}")
            logging.info(f"uri2 : {uri}")
            logging.info(f"uri2 type : {type(uri)}")
            count = 3
            is_success = False
            while count > 0:
                count -= 1
                try:
                    res = requests.get(uri, timeout=30, **self.headers)
                    status_code = res.status_code
                    if str(status_code) in ('404', '500'):
                        if str(status_code) == '500' and 'with section index: 14' not in res.text:
                            pass
                        else:
                            break
                    res.raise_for_status()
                    # print(f"Successfully Retrieved Data from Source API {folder}")
                    logging.info(f"Successfully Retrieved Data from Source API {folder}")
                    res = res.content
                    decode = res.decode("utf-8")
                    json_data = json.loads(decode)
                    logging.info(f"API call is successful {uri}")
                    data_list = []
                    data_list.append(json_data)
                    data_json = {folder: [data_list, path, content_id, step_folder_name, hour]}
                    data_list2 = []
                    data_list2.append(data_json)
                    return data_list2
                except Exception as exc:
                    time.sleep(120)
                    logging.warning(f'Error while Retrieving data from Source API {uri} : {exc.__str__()}')
            if not is_success:
                raise Exception(f'Error while Retrieving data from Source API {uri} after 3 retries')
        except Exception as message:
            logging.error(f'Error while Retrieving data from Source API {uri} : {message}')
            pass


class SaveToGCS(beam.DoFn):

    def __init__(self, bucket_name):
        self._bucket = bucket_name

    def process(self, element):
        try:

            # print(type(element))
            uri = element.keys()
            # print("URI", uri)
            for i in element:
                uri = i
                json_data = element[i]
            path = json_data[1]
            content_id = json_data[2]
            step_folder_name = json_data[3]
            hour = json_data[4]

            today_date = datetime.today()
            year = str(today_date.year)
            month = str(today_date.month).zfill(2)
            day = str(today_date.day).zfill(2)
            path = os.path.join(path, year, month, day, hour)

            json_data = json_data[0][0]
            # if not json_data or not json_data.get("d") or not json_data["d"].get("results"):
            #     return ()
            destination_blob_name = f"{path}/{uri}/{content_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3]}.json"
            # print("Bucket folder path" + destination_blob_name)
            storage_client = storage.Client()
            bucket = storage_client.bucket(self._bucket)
            blob = bucket.blob(destination_blob_name)
            # blob.upload_from_string(data=json.dumps(json_data))
            count = 3
            is_success = False
            msg = ""
            while count > 0:
                try:
                    count -= 1
                    blob.upload_from_string(data=json.dumps(json_data))
                    is_success = True
                    break
                except Exception as exc:
                    time.sleep(120)
                    msg = exc.__str__()
                    logging.warning(f'Error while uploading data to GCS {destination_blob_name} : {exc.__str__()}')
            if not is_success:
                raise Exception(
                    f'Error while uploading data to GCS {destination_blob_name} after 3 retries error: {msg}')
            # print("File uploaded to {}.".format(destination_blob_name))
            logging.info("File uploaded to {}.".format(destination_blob_name))
            logging.info(f"{uri}: Successfully stored file on GCS")
            return ()
        except Exception as message:
            logging.error(f"{uri}: Error while Storing file on GCS: {message}")
            pass


# Creating Apache Beam Pipeline
def dataflow(run_local):
    # defining Other pipeline details
    # PROJECT = 'itg-hranalytics-gbl-ww-dv'
    # BUCKET = 'hrdp_data_dv_rnd'

    # Authorization details for Answers API
    headers = get_authentication()
    # Configuration pipeline options
    pipeline_options = {
            'project': config.PROJECT,
            'staging_location': f'gs://{config.BUCKET}/{config.PROJECT_PATH}/staging',
            'runner': 'DataflowRunner',
            'job_name': "sdds-pmg-emp-forms-api-call",
            # 'template_location': f'gs://{config.BUCKET}/{config.PROJECT_PATH}/templates/pmg_emp_forms_api_template',
            'disk_size_gb': 100,
            'temp_location': f'gs://{config.BUCKET}/{config.PROJECT_PATH}/staging',
            'setup_file': '/home/airflow/gcs/dags/pipeline/setup.py',
            'save_main_session': True,
            'region': 'europe-west1',
            'subnetwork': f'https://www.googleapis.com/compute/v1/projects/{config.PROJECT}/regions/europe-west1/subnetworks/{config.SUB_NETWORK}',
            'no_use_public_ips': True,
            'service_account_email': config.SERVICE_ACCOUNT,
            'direct_num_workers': 4,
            'max_num_workers': 1000,
            'direct_running_mode': 'multi_threading'
        }
    employee_profile_url_path = config.EMPLOYEE_REVIEW_URLS
    file_delimiter = config.DELIMITER
    if run_local:
        pipeline_options['runner'] = 'DirectRunner'
    options = PipelineOptions.from_dictionary(pipeline_options)
    with beam.Pipeline(options=options) as p:
        sourceData = (
                p
                | 'Read File Collection' >> beam.io.ReadFromText(employee_profile_url_path)
                | 'String To Dict' >> beam.Map(lambda w: w.split(file_delimiter))
                | 'Recursive Api Call' >> beam.ParDo(MainApiCall(headers, bucket=config.BUCKET,
                                                                 main_url=config.MAIN_URL,
                                                                 folder_path=config.FOLDER_PATH,
                                                                 step_data=config.STEP_DATA,
                                                                 step_types=config.STEP_TYPES))
                | 'List To Dict' >> beam.Map(lambda w: w.split(file_delimiter))
                | 'Call API ' >> beam.ParDo(CallAPI(headers))
                | 'Write Data to GCS Bucket' >> beam.ParDo(SaveToGCS(bucket_name=config.BUCKET))

        )


# Calling main for Direct Runner or Data Flow Runner
if __name__ == '__main__':
    start_time = time.time()
    run_locally = config.run_locally
    dataflow(run_locally)
    # print("--- %s seconds ---" % (time.time() - start_time))
