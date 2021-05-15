import pathlib
import os
from dotenv import load_dotenv
import csv
import json
from datetime import date, datetime
from dateutil import parser
import pandas as pd
import math

load_dotenv(override=True)
CASE_ID_FIELD = os.getenv('CASE_ID_FIELD')
OUTPUT_DIR = os.getenv('OUTPUT_DIR')
SUBFILE_OUTPUT_DIR = os.getenv('SUBFILE_OUTPUT_DIR')
EVENT_DATE = os.getenv('EVENT_DATE')
SORTING_CONFIG = os.getenv('SORTING_CONFIG')

# for sorting optimization
TIME_CHUNKS = []

def write_csv(event_name, row):
    if CASE_ID_FIELD not in row:
        print(
            "[ERROR]: Event <" + event_name + "> does not have the following Case ID field registered: " + CASE_ID_FIELD)
        return
    if EVENT_DATE not in row:
        print(
            "[ERROR]: Event <" + event_name + "> does not have the following Event Time field registered: " + EVENT_DATE)
        return

    # file_path = str(pathlib.Path().absolute()) + "/" + SUBFILE_OUTPUT_DIR + str(row[CASE_ID_FIELD]) + ".csv"
    file_name = generate_file_name(row[EVENT_DATE])
    file_path = str(pathlib.Path().absolute()) + "/" + SUBFILE_OUTPUT_DIR + file_name + ".csv"
    with open(file_path, "a+") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow([row[CASE_ID_FIELD], event_name, row[EVENT_DATE], json.dumps(row, default=json_serial)])
    csv_file.close()


def generate_file_name(event_time):
    global TIME_CHUNKS
    file_name = ""
    if isinstance(event_time, (datetime, date)):
        # print(event_time.year, event_time.month, event_time.day, event_time.hour)
        file_name = str(event_time.date())
        if SORTING_CONFIG == 'HOUR':
            file_name += "-" + str(event_time.hour)
        elif SORTING_CONFIG == 'HALFHOUR':
            SEC = event_time.minute // 30
            file_name += "-" + str(event_time.hour) + "-" + str(SEC)
        elif SORTING_CONFIG == 'MINUTE':
            file_name += "-" + str(event_time.hour) + "-" + str(event_time.minute)
    elif isinstance(event_time, str):
        parsed_date = parser.parse(event_time)
        # print(parsed_date.year, parsed_date.month, parsed_date.day, parsed_date.hour)
        file_name = str(parsed_date.date())
        if SORTING_CONFIG == 'HOUR':
            file_name += "-" + str(parsed_date.hour)
        elif SORTING_CONFIG == 'HALFHOUR':
            SEC = parsed_date.minute // 30
            file_name += "-" + str(parsed_date.hour) + "-" + str(SEC)
        elif SORTING_CONFIG == 'MINUTE':
            file_name += "-" + str(parsed_date.hour) + "-" + str(parsed_date.minute)
    return file_name


def sort_file(csv_file_path, write_to):
    df = pd.read_csv(csv_file_path, names=["CASE_ID", "EVENT_NAME", "EVENT_DATE", "DATA"])
    sorted_df = df.sort_values(by=["EVENT_DATE"], ascending=True)
    #sorted_df.to_csv(csv_file_path, index=False, header=False)
    sorted_df.to_csv(write_to, index=False, header=False, mode='a+')


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def sorting_csv_files():
    process_nested_table_cache()
    write_dir = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + "output.csv"
    dir_path = str(pathlib.Path().absolute()) + "/" + SUBFILE_OUTPUT_DIR
    files = sorted(os.listdir(dir_path))
    for file_name in files:
        if '.csv' in file_name:
            sort_file(dir_path + file_name, write_dir)


def directory_prepare(only_clean_cache=False):
    if not only_clean_cache:
        dir_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR
        for file_name in os.listdir(dir_path):
            if '.csv' in file_name:
                os.remove(dir_path + file_name)
        dir_path = str(pathlib.Path().absolute()) + "/" + SUBFILE_OUTPUT_DIR
        for file_name in os.listdir(dir_path):
            if '.csv' in file_name:
                os.remove(dir_path + file_name)

    tmp_dir_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + "tmp/"
    for file_name in os.listdir(tmp_dir_path):
        if '.csv' in file_name:
            os.remove(tmp_dir_path + file_name)

    cache_dir_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + "cache/"
    for file_name in os.listdir(cache_dir_path):
        os.remove(cache_dir_path + file_name)


def nested_table_file_header_write(event_name, row, tmp_file_name):
    event_name = event_name.split('.')[0]
    if CASE_ID_FIELD not in row:
        print(
            "[ERROR]: Event <" + event_name + "> does not have the following Case ID field registered: " + CASE_ID_FIELD)
        return
    if EVENT_DATE not in row:
        print(
            "[ERROR]: Event <" + event_name + "> does not have the following Event Time field registered: " + EVENT_DATE)
        return

    # Touch file just in case
    '''
    file_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + str(row[CASE_ID_FIELD]) + ".csv"
    with open(file_path, "a+") as csv_file:
        a = None
    csv_file.close()
    '''

    file_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + "tmp/" + str(
        row[CASE_ID_FIELD]) + "|" + tmp_file_name + ".csv"
    with open(file_path, "a+") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        row['NESTED_ROWS'] = []
        csv_writer.writerow([row[CASE_ID_FIELD], event_name, row[EVENT_DATE], json.dumps(row, default=json_serial)])
    csv_file.close()


def nested_table_file_body_write(case_id, tmp_file_name, row):
    # print(case_id, tmp_file_name, row)
    file_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + "tmp/" + str(
        case_id) + "|" + tmp_file_name + ".csv"
    read_info = []
    try:
        with open(file_path, "r") as csv_file:
            read_info = next(csv.reader(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL))
        csv_file.close()
        json_body = json.loads(read_info[3])
        json_body['NESTED_ROWS'].append(row)
        read_info[3] = json.dumps(json_body, default=json_serial)
        with open(file_path, "w") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(read_info)
        csv_file.close()
    except Exception as inst:
        red_info = []


def handle_overflow_cache(key, value):
    cache_dir_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + "cache/" + str(key)
    with open(cache_dir_path, "a+") as f:
        f.write(str(value))
    f.close()


def read_overflow_cache(key):
    cache_file_dir_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + "cache/" + str(key)
    try:
        with open(cache_file_dir_path) as f:
            value = f.readline()
            f.close()
            return value
    except IOError as e:
        return None


def process_nested_table_cache():
    output_file_path = str(pathlib.Path().absolute()) + "/" + SUBFILE_OUTPUT_DIR
    tmp_file_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + "tmp/"
    for file_name in os.listdir(tmp_file_path):
        if '.csv' in file_name:
            case_id = file_name.split("|")[0]
            read_info = []
            with open(tmp_file_path + file_name, "r") as csv_file:
                read_info = next(csv.reader(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL))
            csv_file.close()

            # output_file = output_file_path + case_id + ".csv"
            file_name = generate_file_name(read_info[2])
            output_file = output_file_path + file_name + ".csv"
            with open(output_file, "a+") as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csv_writer.writerow(read_info)
            csv_file.close()


def count_dir_lines():
    counter = 0
    file_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR
    for file_name in os.listdir(file_path):
        if ".csv" in file_name:
            file = open(file_path + file_name, "r")
            reader = csv.reader(file)
            counter += len(list(reader))
    print("Events Numbers: ", counter)


def save_experiment_to_csv(file_name, data_list):
    load_dotenv(override=True)
    exp_dir = os.getenv('EXP_OUTPUT_DIR')
    file_path = str(pathlib.Path().absolute()) + "/" + exp_dir + file_name + ".csv"
    with open(file_path, "a+") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(data_list)
    csv_file.close()
