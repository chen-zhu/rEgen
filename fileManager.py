import pathlib
import os
from dotenv import load_dotenv
import csv
import json
from datetime import date, datetime
import pandas as pd
import numpy

load_dotenv(override=True)
CASE_ID_FIELD = os.getenv('CASE_ID_FIELD')
OUTPUT_DIR = os.getenv('OUTPUT_DIR')
SUBFILE_OUTPUT_DIR = os.getenv('SUBFILE_OUTPUT_DIR')
EVENT_DATE = os.getenv('EVENT_DATE')

# for sorting optimization
MIN_TIME = 0
MAX_TIME = 0
EVEN_NUM = 0
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
    file_path = str(pathlib.Path().absolute()) + "/" + SUBFILE_OUTPUT_DIR + "output.csv"
    set_global(row[EVENT_DATE])
    with open(file_path, "a+") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow([row[CASE_ID_FIELD], event_name, row[EVENT_DATE], json.dumps(row, default=json_serial)])
    csv_file.close()


def set_global(row_time):
    # bad design here. Should've made it OOD
    global MIN_TIME, MAX_TIME, EVEN_NUM
    EVEN_NUM += 1
    unix_time = 0
    if isinstance(row_time, (datetime, date)):
        unix_time = row_time.timestamp()
    elif isinstance(row_time, str):
        unix_time = datetime.strptime(row_time, "%Y-%m-%d %H:%M:%S.%f").timestamp()
    if MIN_TIME == 0 or MAX_TIME == 0:
        MIN_TIME = unix_time
        MAX_TIME = unix_time
    elif MIN_TIME > unix_time:
        MIN_TIME = unix_time
    elif MAX_TIME < unix_time:
        MAX_TIME = unix_time
    # print(MIN_TIME, MAX_TIME, EVEN_NUM)

def sort_file(csv_file_path):
    df = pd.read_csv(csv_file_path, names=["CASE_ID", "EVENT_NAME", "EVENT_DATE", "DATA"])
    sorted_df = df.sort_values(by=["EVENT_DATE"], ascending=True)
    sorted_df.to_csv(csv_file_path, index=False, header=False)


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def sorting_csv_files():
    process_nested_table_cache()
    time_chunk()
    dir_path = str(pathlib.Path().absolute()) + "/" + SUBFILE_OUTPUT_DIR
    for file_name in os.listdir(dir_path):
        if '.csv' in file_name:
            sort_file(dir_path + file_name)


def time_chunk():
    global MIN_TIME, MAX_TIME, EVEN_NUM, TIME_CHUNKS
    chunk = (EVEN_NUM // 100) + 1
    if chunk > 1:
        TIME_CHUNKS = numpy.linspace(MIN_TIME, MAX_TIME + 1, chunk)
        print(TIME_CHUNKS, MIN_TIME, MAX_TIME, chunk)


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
            output_file = output_file_path + "output.csv"
            with open(output_file, "a+") as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csv_writer.writerow(read_info)
                set_global(read_info[2])
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
