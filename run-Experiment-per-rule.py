import os
from parser import *
from validator import *
from database import *
from interpreter import *
import pathlib
import dotenv
from fileManager import sorting_csv_files, directory_prepare, count_dir_lines, save_experiment_to_csv
import time
import random

#per db + per rule
if __name__ == "__main__":
    db_list = ["12000_tasks", "16000_tasks", "20000_tasks", "24000_tasks", "28000_tasks", "36000_tasks"]

    for db_name in db_list:
        dotenv_file = dotenv.find_dotenv()
        dotenv.set_key(dotenv_file, "SOURCE_DATABASE", db_name)

        dotenv_file = dotenv.find_dotenv()
        dotenv.load_dotenv(dotenv_file, override=True)
        RULES_DIR = os.getenv('RULES_DIR')
        SOURCE_DATABASE = os.getenv('SOURCE_DATABASE')
        data_path = str(pathlib.Path().absolute()) + "/" + RULES_DIR
        print("[current source DB]: " + db_name + ", -- SOURCE_DATABASE: ", os.getenv('SOURCE_DATABASE'))

        all_files = os.listdir(data_path)
        if ".DS_Store" in all_files:
            all_files.remove(".DS_Store")
        if "__pycache__" in all_files:
            all_files.remove("__pycache__")

        processing_files = all_files #random.sample(all_files, 6)

        for file_name in processing_files:
            execution_time = []
            total_event = 0
            for i in range(6):
                directory_prepare()
                # clean up target

                start_time = time.time()
                total_event = 0

                p = parser()
                parsed_result = p.parsingFile(data_path + file_name)
                p.structurePrettyPrint(parsed_result)

                db = database(db_name)
                v = validator(p, db)
                v.validate_rule_parameters()
                v.validate_source_database()

                i = interpreter(p, db)
                i.field_mappings()

                i.events_generator()
                total_event += i.event_count

                sorting_csv_files()
                exe_time = time.time() - start_time
                execution_time.append(exe_time)
                count_dir_lines()
                directory_prepare(True)
                time.sleep(1)
            save_experiment_to_csv("per_db_per_rule", [db_name, file_name, str(total_event)])
            save_experiment_to_csv("per_db_per_rule", execution_time)
            print("Execution Time List: " + file_name + " Total Events: " + str(total_event) + " :\n", execution_time)
