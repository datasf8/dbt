#!/usr/bin/env python3

import math
import os
import time
from datetime import datetime
# import click
import google.auth
from google.cloud import storage
from google.cloud import secretmanager
import logging
import requests
import json


storage_client = storage.Client()
_, PROJECT_ID = google.auth.default()
TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))
input_object = os.environ.get("SOURCE_FILE_PATH")
input_bucket = os.environ.get("BUCKET_NAME")
# secret_name = os.environ.get("SECRET_NAME")
main_url = "https://api55.sapsf.eu"
folder_path = input_object
step_data = {"1": "PE", "3": "YE", '4': "MGR", "5": "SGN", "completed": "completed"}
step_types = ["1", "3", '4', "5", "completed"]

# @click.command()
# @click.argument("input_object")
# @click.option("--input_bucket", default=f"input-{PROJECT_ID}")


def process():
    method_start = time.time()

    # Output useful errorrmation about the processing starting.
    logging.info(
        f"Task {TASK_INDEX}: Processing part {TASK_INDEX} of {TASK_COUNT} "
        f"for gs://{input_bucket}/{input_object}"
    )

    # Download the Cloud Storage object
    bucket = storage_client.bucket(input_bucket)
    blob = bucket.blob(input_object)

    # Split blog into a list of strings.
    contents = blob.download_as_string().decode("utf-8")
    data = contents.split("\n")
    secret_details = json.loads(os.environ.get("SECRET_NAME"))
    headers = {"auth": (secret_details.get("user_name"), secret_details.get("password"))}
    logging.info(f"length of URL file is : {len(data)}")
    if len(data) <= 0 or (len(data) == 1 and data[0] == ''):
        return
    # Determine the chunk size, and identity this task's chunk to process.
    chunk_size = math.ceil(len(data) / TASK_COUNT)
    chunk_start = chunk_size * TASK_INDEX
    chunk_end = chunk_start + chunk_size

    # Process each line in the chunk.
    count = 0
    loop_start = time.time()
    for line in data[chunk_start:chunk_end]:
        # Perform your operation here. This is just a placeholder.
        api_calls(line.split("|*|"), headers)
        time.sleep(0.1)
        count += 1

    # Output useful errorrmation about the processing completed.
    time_taken = round(time.time() - method_start, 3)
    time_setup = round(loop_start - method_start, 3)
    logging.info(
        f"Task {TASK_INDEX}: Processed {count} lines "
        f"(ln {chunk_start}-{min(chunk_end-1, len(data))} of {len(data)}) "
        f"in {time_taken}s ({time_setup}s preparing)"
    )


def api_calls(input_uri, headers):
    logging.info(f"input_uri : {input_uri}")
    logging.info(f"input_uri type : {type(input_uri)}")
    folder = input_uri[0]
    uri = input_uri[1]
    employee_id = input_uri[6]
    current_time = datetime.strptime(input_uri[7], '%Y-%m-%dT%H:%M:%S')
    hour = input_uri[8]
    path = input_uri[2]
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
    db_file_path = path
    modified_date_data = {}
    db_file_name = "employee_forms_modified_date.dat"
    bucket = storage_client.bucket(input_bucket)
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
    api_rcall(uri, employee_id, current_time, hour, headers, str(db_file_path))
    # logging.error(employee_url_lst)


def api_rcall(url, employee_id, current_time, hour, headers, file_path):
    user_file_name = f"{employee_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
    count = 3
    file_data = {}
    is_success = False
    while count > 0:
        count -= 1
        try:
            f = requests.get(url, timeout=30, **headers)
            status_code = f.status_code
            if str(status_code) == '404':
                break
            f.raise_for_status()
            file_data = f.json()
            is_success = True
            break
        except Exception as exc:
            time.sleep(30)
            logging.warning(f'Error while Retrieving data from Source API {url} : {exc.__str__()}')
    if not is_success:
        raise Exception(f'Error while Retrieving data from Source API {url} after 3 retries')
    employee_url_lst = []
    employee_url_data = ""
    form_content_data = {}
    # path = file_path
    if not file_data.get("d"):
        logging.info(f"content id {employee_id} and data is: {file_data}")
        return employee_url_data, employee_url_lst
    for content_dict in file_data["d"]["results"]:
        form_content_step = content_dict.get("formContentAssociatedStepId")
        if form_content_step in step_types:
            if form_content_step in form_content_data:
                last_modified_date = form_content_data[form_content_step].get("auditTrailLastModified")
                audit_trial_id = form_content_data[form_content_step].get("auditTrailId")
                if last_modified_date <= content_dict.get("auditTrailLastModified") \
                        and audit_trial_id < content_dict.get("auditTrailId"):
                    form_content_data[form_content_step] = content_dict
                elif last_modified_date < content_dict.get("auditTrailLastModified"):
                    form_content_data[form_content_step] = content_dict
            else:
                form_content_data[form_content_step] = content_dict

    content_data_list = []
    for form_content_step_id in form_content_data:
        logging.info(f"Folder Path from ENV: {file_path}")
        logging.info(f"formContentAssociatedStepId(1): {form_content_step_id}")
        folder_name = step_data.get(str(form_content_step_id))
        logging.info(f"Folder Name type: {folder_name}")
        content_dict = form_content_data.get(form_content_step_id)
        content_data_list.append(content_dict)
        form_content_id = content_dict.get("formContentId")
        user_url = ""
        if folder_name != "completed":
            if folder_name in ("PE", "YE", "MGR"):
                logging.info(f"PE Folder Name: {folder_name}")
                logging.info(f"formContentAssociatedStepId(1): {form_content_step_id}")
                user_url = f"FORMOBJCOMPSUMMARYSECTION|*|{main_url}/odata/v2/FormObjCompSummarySection(formContentId={form_content_id}L,formDataId={employee_id}L)/?$format=json&$expand=overallCompRating,overallObjRating"
                user_url += f"|*|{file_path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMOBJCOMPSUMMARYSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                employee_url_lst.append(user_url)
                if employee_url_data:
                    employee_url_data += '\n'
                employee_url_data += user_url
                overall_rating_url = f"FORMSUMMARYSECTION|*|{main_url}/odata/v2/FormSummarySection(formContentId={form_content_id}L,formDataId={employee_id}L)/overallFormRating?$format=json"
                overall_rating_url += f"|*|{file_path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMSUMMARYSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                employee_url_lst.append(overall_rating_url)
                if employee_url_data:
                    employee_url_data += '\n'
                employee_url_data += user_url
            if folder_name in ("PE", "YE"):
                user_url = f"FORMCUSTOMSECTION|*|{main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=14)/?$format=json&$expand=othersRatingComment, customElement"
                user_url += f"|*|{file_path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMCUSTOMSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                employee_url_lst.append(user_url)
                if employee_url_data:
                    employee_url_data += '\n'
                employee_url_data += user_url
            if folder_name in ("YE", "MGR"):
                user_url = f"FORMCUSTOMSECTION|*|{main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=12)/?$format=json&$expand=othersRatingComment, customElement"
                user_url += f"|*|{file_path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMCUSTOMSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                employee_url_lst.append(user_url)
                if employee_url_data:
                    employee_url_data += '\n'
                employee_url_data += user_url
            if folder_name == "MGR":
                user_url = f"FORMCUSTOMSECTION|*|{main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=13)/?$format=json&$expand=othersRatingComment, customElement"
                user_url += f"|*|{file_path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMCUSTOMSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                employee_url_lst.append(user_url)
                if employee_url_data:
                    employee_url_data += '\n'
                employee_url_data += user_url
            if folder_name == "SGN":
                user_url = f"FORMCUSTOMSECTION|*|{main_url}/odata/v2/FormCustomSection(formContentId={form_content_id}L,formDataId={employee_id}L,sectionIndex=19)/?$format=json&$expand=customElement,othersRatingComment"
                user_url += f"|*|{file_path}|*|HRDP_COMMON_OBJ_DB|*|COMMON_OBJ_SCH|*|FORMCUSTOMSECTION|*|{form_content_id}|*|{current_time}|*|{folder_name}|*|{hour}"
                employee_url_lst.append(user_url)
            if employee_url_data:
                employee_url_data += "\n"
            employee_url_data += user_url
    today_date = datetime.today()
    year = str(current_time.year)
    month = str(current_time.month).zfill(2)
    day = str(current_time.day).zfill(2)
    path = str(os.path.join(file_path, year, month, day, hour))
    file_path = os.path.join(path, "FORMAUDITTRAILS", user_file_name)
    if content_data_list:
        file_data = {"d": {"results": content_data_list}}
        storage_client = storage.Client()

        bucket = storage_client.bucket(input_bucket)
        blob = bucket.blob(file_path)
        logging.info(f"GCP file path:{file_path}")
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
                time.sleep(30)
                msg = exc.__str__()
                logging.warning(f'Error while uploading data to GCS {file_path} : {exc.__str__()}')
        if not is_success:
            logging.error(f'Error while uploading data to GCS {file_path} : {msg}')
            raise Exception(
                f'Error while uploading data to GCS {file_path} after 3 retries error: {msg}')
    for url_data in employee_url_lst:
        final_url(url_data, headers, current_time)


def final_url(input_uri, headers, current_time):
    try:
        logging.info(f"input_uri : {input_uri}")
        logging.info(f"input_uri type : {type(input_uri)}")
        input_uri = input_uri.split("|*|")
        folder = input_uri[0]
        uri = input_uri[1]
        path = input_uri[2]
        content_id = input_uri[6]
        step_folder_name = input_uri[8]
        hour = input_uri[9]
        logging.info(f"folder 1: {folder}")
        logging.info(f"folder type 1: {type(folder)}")
        logging.info(f"uri2 1: {uri}")
        logging.info(f"uri2 type 1: {type(uri)}")
        json_data = {}
        count = 3
        is_success = False
        msg = ""
        while count > 0:
            count -= 1
            try:
                res = requests.get(uri, timeout=30, **headers)
                status_code = res.status_code
                if str(status_code) in ('404', '500'):
                    if str(status_code) == '500' and 'with section index: 14' not in res.text:
                        pass
                    else:
                        break
                res.raise_for_status()
                logging.info(f"Successfully Retrieved Data from Source API {folder} 1")
                res = res.content
                decode = res.decode("utf-8")
                json_data = json.loads(decode)
                is_success = True
                logging.info(f"API call is successful {uri} 1")
            except Exception as exc:
                time.sleep(30)
                msg = exc.__str__()
                logging.warning(f'Error while Retrieving data from Source API {uri} : {msg} 1')
        if not is_success:
            raise Exception(f'Error while Retrieving data from Source API {uri} after 3 retries {msg} 1')
        # today_date = datetime.today()
        year = str(current_time.year)
        month = str(current_time.month).zfill(2)
        day = str(current_time.day).zfill(2)
        path = os.path.join(path, year, month, day, hour)
        destination_blob_name = f"{path}/{folder}/{content_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3]}.json"
        bucket = storage_client.bucket(input_bucket)
        blob = bucket.blob(destination_blob_name)
        logging.info(f"GCP file path:{destination_blob_name}")
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
                time.sleep(30)
                msg = exc.__str__()
                logging.warning(f'Error while uploading data to GCS {destination_blob_name} : {exc.__str__()}')
        if not is_success:
            raise Exception(
                f'Error while uploading data to GCS {destination_blob_name} after 3 retries error: {msg}')
        logging.info("File uploaded to {}.".format(destination_blob_name))
        logging.info(f"{uri}: Successfully stored file on GCS")
    except Exception as message:
        logging.warning(f'Error while Retrieving data from Source API {uri} : {message}')
        pass


if __name__ == "__main__":
    process()
