import ast
import json
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_file_patterns(config):
    no_date_required = config.NO_DATE_REQUIRED if config.NO_DATE_REQUIRED else False
    file_patterns = json.loads(config.FILE_PATTERN) if config.FILE_PATTERN.startswith("[") else config.FILE_PATTERN
    run_for_last_day = config.run_for_last_day
    print(f"RUN_FOR_LAST_DAY: {run_for_last_day}")
    if not file_patterns:
        raise Exception("File pattern not configured in config file")
    env_name_list = file_patterns if isinstance(file_patterns, list) else [file_patterns]
    today_date = datetime.today().strftime("%Y%m%d")
    print(f"TODAYS DATE BEFORE: {today_date}")
    if run_for_last_day:
        today_date = (datetime.today()-timedelta(days=1)).strftime("%Y%m%d")
        print(f"TODAYS DATE IF: {today_date}")

    previous_date = config.PREVIOUS_DATE
    if previous_date:
        previous_date = datetime.strptime(previous_date, "%Y-%m-%d")
        today_date = previous_date.strftime("%Y%m%d")
    file_pattern_list = []
    print(f"TODAYS DATE: {today_date}")
    for env_name in env_name_list:
        if no_date_required:
            file_pattern_list.append(env_name)
        else:
            file_name = env_name + today_date + ".csv"
            file_pattern_list.append(file_name)
    # file_list = sftp_config.FILE_LIST
    # if file_list:
    #     file_patterns = file_list
    print(f"FINAL_FILE_PATTERN: {file_pattern_list}")
    return file_pattern_list


def get_folder_list(config, is_hour=False):
    file_patterns = json.loads(config.FILE_PATTERN)  if config.FILE_PATTERN.startswith("[") else  config.FILE_PATTERN
    if not file_patterns:
        raise Exception("File pattern not configured in config file")
    env_name_list = file_patterns if isinstance(file_patterns, list) else [file_patterns]
    previous_date = config.PREVIOUS_DATE
    today_date = datetime.today()
    run_for_last_day = config.run_for_last_day
    print(f"RUN_FOR_LAST_DAY: {run_for_last_day}")
    if run_for_last_day:
        today_date = datetime.today()-timedelta(days=1)
    if previous_date:
        previous_date = datetime.strptime(previous_date, "%Y-%m-%d")
        today_date = previous_date
    year = str(today_date.year)
    month = str(today_date.month).zfill(2)
    day = str(today_date.day).zfill(2)
    # hour = str(today_date.hour).zfill(2)
    folder_list = []
    for env_name in env_name_list:
        print(env_name)
        # if is_hour:
        #     folder_list.append(year + "/" + month + "/" + day + "/" + hour)
        # else:
        folder_list.append(year + "/" + month + "/" + day)
    return folder_list


def get_unzip_file_details(file_details, file_no=None):
    file_details = ast.literal_eval(file_details) if file_details else []
    logger.info("FILES DETAILS:" + str(file_details))
    if file_details and file_no is not None:
        file_details = [file_details[file_no]]
    file_names = []
    if file_details:
        for file_obj in file_details:
            file_names.append(file_obj)
    return file_names
