import time
from datetime import datetime
import json
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import requests
from google.cloud import storage
import os
import sys
sys.path.append('/home/airflow/gcs/dags/pipeline/')
from pipeline_config import dag_config as config


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
        # print(f"uri2 : {uri}")
        # print(f"uri2 type : {type(uri)}")
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
        self.path = os.path.join(self.path, year, month, day, hour)
        employee_urls, employee_url_lst = self.api_rcall(uri, employee_id, current_time, hour)
        file_path = os.path.join(self.path, "employee_forms", f"employee_review{employee_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}")
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
        file_data = {}
        while count > 0:
            count -= 1
            try:
                f = requests.get(url, timeout=30, **self.headers)
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
            logging.info(content_dict)
            form_content_step = content_dict.get("formContentAssociatedStepId")
            # logging.info(f"formContentAssociatedStepId: {form_content_step}")
            if form_content_step in self.step_types:
                if form_content_step in form_content_data:
                    last_modified_date = form_content_data[form_content_step].get("auditTrailLastModified")
                    if last_modified_date < content_dict.get("auditTrailLastModified"):
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
                if folder_name in ("PE", "YE"):
                    user_url = f"PEOPLE_BUSINESS_GOALS|*|{self.main_url}/odata/v2/FormObjCompSummarySection(formContentId={form_content_id}L,formDataId={employee_id}L)/?$format=json&paging=cursor&$expand=overallCompRating,overallObjRating"
                    user_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|PEOPLE_BUSINESS_GOALS|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(user_url)
                    if employee_url_data:
                        employee_url_data += '\n'
                    employee_url_data += user_url
                    overall_rating_url = f"OVERALL_RATING|*|{self.main_url}/odata/v2/FormSummarySection(formContentId={form_content_id}L,formDataId={employee_id}L)/overallFormRating?$format=json&paging=cursor"
                    overall_rating_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|OVERALL_RATING|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(overall_rating_url)
                    employee_url_data += user_url
                if folder_name == "YE":
                    user_url = f"COMMENTS|*|{self.main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=9)/?$format=json&paging=cursor&$expand=othersRatingComment, customElement"
                    user_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|COMMENTS|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(user_url)
                    if employee_url_data:
                        employee_url_data += '\n'
                    employee_url_data += user_url
                if folder_name == "SGN":
                    user_url = f"SIGNATURE_COMMENTS|*|{self.main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=10)/?$format=json&paging=cursor&$expand=customElement,othersRatingComment"
                    user_url += f"|*|{path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|SIGNATURE_COMMENTS|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                    employee_url_lst.append(user_url)
                if employee_url_data:
                    employee_url_data += "\n"
                employee_url_data += user_url
        file_path = os.path.join(self.path, "FORM_CONTENT", user_file_name)
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
        uri = ""
        try:
            # print(f"input_uri : {input_uri}")
            # print(f"input_uri type : {type(input_uri)}")
            logging.info(f"input_uri : {input_uri}")
            logging.info(f"input_uri type : {type(input_uri)}")

            folder = input_uri[0]
            uri = input_uri[1]
            path = input_uri[2]
            content_id = input_uri[6]
            current_date = input_uri[7]
            hour = input_uri[8]
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
            # res = object
            while count > 0:
                count -= 1
                try:
                    res = requests.get(uri, **self.headers)
                    res.raise_for_status()
                    # print(f"Successfully Retrieved Data from Source API {folder}")
                    logging.info(f"Successfully Retrieved Data from Source API {folder}")
                    res = res.content
                    decode = res.decode("utf-8")
                    json_data = json.loads(decode)
                    logging.info(f"API call is successful {uri}")
                    data_list = []
                    data_list.append(json_data)
                    data_json = {folder: [data_list, path, content_id, hour]}
                    data_list2 = []
                    data_list2.append(data_json)
                    is_success = True
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

    def __init__(self, bucket_name, job_type):
        self._bucket = bucket_name
        self.job_type = job_type

    def process(self, element):
        uri = ""
        try:

            # print(type(element))
            uri = element.keys()
            # print("URI", uri)
            json_data = []
            for i in element:
                uri = i
                json_data = element[i]
            path = json_data[1]
            content_id = json_data[2]
            hour = json_data[3]
            today_date = datetime.today()
            year = str(today_date.year)
            month = str(today_date.month).zfill(2)
            day = str(today_date.day).zfill(2)
            path = os.path.join(path, year, month, day, hour)

            json_data = json_data[0][0]
            user_data_str = ""
            destination_blob_name = f"{path}/{uri}/{content_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3]}.json"
            # print("Bucket folder path" + destination_blob_name)
            storage_client = storage.Client()
            bucket = storage_client.bucket(self._bucket)

            blob = bucket.blob(destination_blob_name)
            if self.job_type == "profile":
                for user_data in json_data["d"]["results"]:
                    if user_data_str:
                        user_data_str += "\n"
                    user_data_str += json.dumps({"d": user_data})
            elif self.job_type == "rls":
                user_data_str = "\n".join(json.dumps(i) for i in json_data["d"])
            else:
                user_data_str = json.dumps(json_data)
            count = 3
            is_success = False
            msg = ""
            while count > 0:
                try:
                    count -= 1
                    blob.upload_from_string(data=user_data_str)
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
def dataflow(argv=None):
    # defining Other pipeline details
    # PROJECT = 'itg-hranalytics-gbl-ww-dv'
    # BUCKET = 'hrdp_data_dv'

    # Authorization details for Answers API
    headers = config.get_authentication()
    # Configuration pipeline options
    pipeline_options = {
        'project': config.PROJECT,
        'staging_location': f'gs://{config.BUCKET}/{config.PROJECT_PATH}/staging',
        'runner': 'DataflowRunner',
        'job_name': "pmg-emp-profile-api-call",
        # 'template_location': f'gs://{config.BUCKET}/{config.PROJECT_PATH}/templates/pmg_emp_api_call_template',
        'disk_size_gb': 100,
        'temp_location': f'gs://{config.BUCKET}/{config.PROJECT_PATH}/staging',
        'setup_file': '/home/airflow/gcs/dags/pipeline/setup.py',
        'num_workers': 10,
        'save_main_session': True,
        'region': 'europe-west1',
        'subnetwork': f'https://www.googleapis.com/compute/v1/projects/{config.PROJECT}/regions/europe-west1/subnetworks/{config.SUB_NETWORK}',
        'no_use_public_ips': True,
        'service_account_email': config.SERVICE_ACCOUNT,
        'direct_num_workers': 10,
        'direct_running_mode': 'multi_threading'
    }
    # employee_profile_url_path = config.EMPLOYEE_PROFILE_URLS
    employee_profile_url_path = ""
    file_delimiter = config.DELIMITER
    # if run_local:
    #     pipeline_options['runner'] = 'DirectRunner'
    options = PipelineOptions.from_dictionary(pipeline_options)
    print(f"kwargs: {argv}")
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default=employee_profile_url_path,
                        help='Input file to process urls.')
    parser.add_argument('--job_type',
                        dest='job_type',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default="",
                        help='Input file to process urls.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    print(f"KNOWN ARGUMENTS: {known_args}")
    print(f"PIPELINE ARGUMENTS: {pipeline_args}")
    job_type = {known_args.job_type}
    url_path = f"gs://{config.BUCKET}/{known_args.input}"
    with beam.Pipeline(options=options) as p:
        if job_type != "header_form":
            sourceData = (
                    p
                    | 'Read File Collection' >> beam.io.ReadFromText(url_path)
                    | 'String To Dict' >> beam.Map(lambda w: w.split(file_delimiter))
                    # | 'Recursive Api Call' >> beam.ParDo(RecursiveApiCall(headers, bucket=config.BUCKET))
                    # | 'List To Dict' >> beam.Map(lambda w: w.split(file_delimiter))
                    | 'Call API ' >> beam.ParDo(CallAPI(headers))
                    | 'Write Data to GCS Bucket' >> beam.ParDo(SaveToGCS(config.BUCKET, job_type))

            )
        else:
            sourceData = (
                    p
                    | 'Read File Collection' >> beam.io.ReadFromText(url_path)
                    | 'String To Dict' >> beam.Map(lambda w: w.split(file_delimiter))
                    | 'Recursive Api Call' >> beam.ParDo(MainApiCall(headers, bucket=config.BUCKET,
                                                                     main_url=config.MAIN_URL,
                                                                     folder_path=config.FOLDER_PATH,
                                                                     step_data=config.STEP_DATA,
                                                                     step_types=config.STEP_TYPES))
                    | 'List To Dict' >> beam.Map(lambda w: w.split(file_delimiter))
                    | 'Call API ' >> beam.ParDo(CallAPI(headers))
                    | 'Write Data to GCS Bucket' >> beam.ParDo(SaveToGCS(config.BUCKET, job_type))

            )


# Calling main for Direct Runner or Data Flow Runner
if __name__ == '__main__':
    start_time = time.time()
    dataflow()
    # print("--- %s seconds ---" % (time.time() - start_time))
