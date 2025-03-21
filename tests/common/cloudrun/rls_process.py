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

logging.info(f"TASK_INDEX: {TASK_INDEX}\n TASK_COUNT: {TASK_COUNT}\ninput_object: {input_object}\n")
# @click.command()
# @click.argument("input_object")
# @click.option("--input_bucket", default=f"input-{PROJECT_ID}")


def process():
    method_start = time.time()

    # Output useful information about the processing starting.
    logging.error(
        f"Task {TASK_INDEX}: Processing part {TASK_INDEX} of {TASK_COUNT} "
        f"for gs://{input_bucket}/{input_object}"
    )

    # Download the Cloud Storage object
    bucket = storage_client.bucket(input_bucket)
    blob = bucket.blob(input_object)

    # Split blog into a list of strings.
    contents = blob.download_as_string().decode("utf-8")
    data = contents.split("\n")

    # Determine the chunk size, and identity this task's chunk to process.
    chunk_size = math.ceil(len(data) / TASK_COUNT)
    chunk_start = chunk_size * TASK_INDEX
    chunk_end = chunk_start + chunk_size

    # Process each line in the chunk.
    count = 0
    loop_start = time.time()
    for line in data[chunk_start:chunk_end]:
        # Perform your operation here. This is just a placeholder.
        api_calls(line.split("|*|"))
        time.sleep(0.1)
        count += 1

    # Output useful information about the processing completed.
    time_taken = round(time.time() - method_start, 3)
    time_setup = round(loop_start - method_start, 3)
    logging.error(
        f"Task {TASK_INDEX}: Processed {count} lines "
        f"(ln {chunk_start}-{min(chunk_end-1, len(data))} of {len(data)}) "
        f"in {time_taken}s ({time_setup}s preparing)"
    )


def api_calls(input_uri):
    logging.error(f"input_uri : {input_uri}")
    logging.error(f"input_uri type : {type(input_uri)}")

    folder = input_uri[0]
    uri = input_uri[1]
    path = input_uri[2]
    content_id = input_uri[6]
    current_date = input_uri[7]
    hour = input_uri[8]
    logging.info(f"folder : {folder}")
    logging.info(f"folder type : {type(folder)}")
    logging.info(f"uri2 : {uri}")
    logging.info(f"uri2 type : {type(uri)}")
    json_data = {}
    count = 3
    is_success = False
    secret_details = json.loads(os.environ.get("SECRET_NAME"))
    headers = {"auth": (secret_details.get("user_name"), secret_details.get("password"))}
    while count > 0:
        count -= 1
        try:
            res = requests.get(uri, **headers)
            status_code = res.status_code
            if str(status_code) == '400':
                break
            res.raise_for_status()
            logging.info(f"Successfully Retrieved Data from Source API {folder}")
            res = res.content
            decode = res.decode("utf-8")
            json_data = json.loads(decode)
            logging.info(f"API call is successful {uri}")
            is_success = True
        except Exception as exc:
            time.sleep(30)
            logging.warning(f'Error while Retrieving data from Source API {uri} : {exc.__str__()}')
    if not is_success:
        raise Exception(f'Error while Retrieving data from Source API {uri} after 3 retries')
    try:
        logging.error("USER_GROUP_ID {}.".format(content_id))
        today_date = datetime.strptime(current_date, '%Y-%m-%dT%H:%M:%S')
        year = str(today_date.year)
        month = str(today_date.month).zfill(2)
        day = str(today_date.day).zfill(2)
        path = os.path.join(path, year, month, day, hour)
        destination_blob_name = f"{path}/{folder}/{content_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3]}.json"
        bucket = storage_client.bucket(input_bucket)
        blob = bucket.blob(destination_blob_name)
        user_data_str = "\n".join(json.dumps(i) for i in json_data["d"])
        count = 3
        is_success = False
        msg = ""
        while count > 0:
            try:
                count -= 1
                blob.upload_from_string(user_data_str)
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
        logging.error(f"{uri}: Error while Storing file on GCS: {message}")


if __name__ == "__main__":
    process()
