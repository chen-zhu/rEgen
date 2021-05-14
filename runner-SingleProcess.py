'''
1. Grammar and Parser
2. Run DB Connector to gather information
3. Run Validator (syntax check + column check)
4. Run interpreter
'''
import os
from parser import *
from validator import *
from database import *
from interpreter import *
from pprint import pprint
import pathlib
from dotenv import load_dotenv
from fileManager import sorting_csv_files, directory_prepare
import time

load_dotenv(override=True)
RULES_DIR = os.getenv('RULES_DIR')

if __name__ == "__main__":
    directory_prepare()
    data_path = str(pathlib.Path().absolute()) + "/" + RULES_DIR
    start_time = time.time()
    total_event = 0
    for file_name in os.listdir(data_path):
        if file_name in [".DS_Store", "__pycache__"]:
        #if file_name not in ["UserSubmitsTask"]:
            continue
        p = parser()
        parsed_result = p.parsingFile(data_path + file_name)
        p.structurePrettyPrint(parsed_result)

        db = database()
        v = validator(p, db)
        v.validate_rule_parameters()
        v.validate_source_database()

        i = interpreter(p, db)
        i.field_mappings()

        i.events_generator()
        total_event += i.event_count

    sorting_csv_files()
    print("\n--- %s seconds ---" % (time.time() - start_time), total_event)
    directory_prepare(True)


