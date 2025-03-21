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
secret_name = os.environ.get("SECRET_NAME")

logging.info(f"TASK_INDEX: {TASK_INDEX}\n TASK_COUNT: {TASK_COUNT}\ninput_object: {input_object}\n")
logging.info(f"secret_name: {secret_name}\n input_bucket: {input_bucket}\n")
# @click.command()
# @click.argument("input_object")
# @click.option("--input_bucket", default=f"input-{PROJECT_ID}")


def process():
    method_start = time.time()

    # Output useful information about the processing starting.
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
    logging.info(
        f"Task {TASK_INDEX}: Processed {count} lines "
        f"(ln {chunk_start}-{min(chunk_end-1, len(data))} of {len(data)}) "
        f"in {time_taken}s ({time_setup}s preparing)"
    )


def api_calls(input_uri):
    try:
        logging.info(f"input_uri : {input_uri}")
        logging.info(f"input_uri type : {type(input_uri)}")
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

        try:
            secret_details = json.loads(os.environ.get("SECRET_NAME"))
            headers = {"auth": (secret_details.get("user_name"), secret_details.get("password"))}
        except Exception as message:
            logging.error(f'Error while Retrieving secret {secret_name} : {message}')
            return f'Error while Retrieving secret {secret_name} : {message}'
        json_data = {}
        count = 3
        is_success = False
        while count > 0:
            count -= 1
            try:
                logging.info(f"URL: {uri}")
                res = requests.get(uri, timeout=30, **headers)
                res.raise_for_status()
                logging.info(f"Successfully Retrieved Data from Source API {folder}")
                res = res.content
                decode = res.decode("utf-8")
                json_data = json.loads(decode)
                logging.info(f"API call is successful {uri}")
                is_success = True
                break
            except Exception as exc:
                time.sleep(10)
                logging.warning(f'Error while Retrieving data from Source API {uri} : {exc.__str__()}')
        if not is_success:
            raise Exception(f'Error while Retrieving data from Source API {uri} after 3 retries')
        try:
            today_date = datetime.strptime(current_date, '%Y-%m-%dT%H:%M:%S')
            year = str(today_date.year)
            month = str(today_date.month).zfill(2)
            day = str(today_date.day).zfill(2)
            path = os.path.join(path, year, month, day, hour)
            destination_blob_name = f"{path}/{folder}/{content_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3]}.json"
            if json_data["d"]["results"]:
                # storage_client = storage.Client()
                bucket = storage_client.bucket(input_bucket)
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_string(data=json.dumps(json_data))
                logging.info("File uploaded to {}.".format(destination_blob_name))
                logging.info(f"Successfully stored file on GCS")
            else:
                logging.info(f"Data not present")
        except Exception as message:
            logging.error(f"Error while Storing file on GCS: {message}")
            return f'Error while Retrieving data from Source API {uri} : {message}'

    except Exception as message:
        logging.error(f'Error while Retrieving data from Source API {uri} : {message}')
        pass
    return f'Successfully retrieved data from API {uri}'


if __name__ == "__main__":
    process()
