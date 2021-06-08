'''
1. check if duplicate attributs from single table~
2. check if table exist in db
3. check if column exist in db
'''
import collections
import os
from dotenv import load_dotenv


class validator:

    def __init__(self, parser_obj, database_obj):
        self.parser = parser_obj
        self.db = database_obj
        load_dotenv(override=True)

    def validate_rule_parameters(self):
        print("Validating rule body params...")
        body = []
        body_simple_columns_all = []
        for table_name in self.parser.body_simple_columns:
            body_simple_columns_all += self.parser.body_simple_columns[table_name]
            if len(self.parser.body_simple_columns) > 1:
                col_set = set(self.parser.body_simple_columns[table_name])
                if len(col_set) == len(self.parser.body_simple_columns[table_name]):
                    body.append(col_set)
                else:
                    print("[ERROR]: Rule BODY TABLE, " + table_name + ", contains duplicate column(s)!")
                    return
        if len(self.parser.body_simple_columns) > 1:
            duplicate = body[0].intersection(*body[1:])
            if len(duplicate):
                print("[ERROR]: Rule Body contains duplicate columns: ", duplicate)
        #print('body_simple_columns_all', body_simple_columns_all)

        body_condition_list = []
        for table_name in self.parser.body_condition_columns:
            for cond in self.parser.body_condition_columns[table_name]:
                body_condition_list.append(cond[0])
        #print('body_condition_list', body_condition_list)

        print("Validating rule header params...")
        CASE_ID_FIELD = os.getenv('CASE_ID_FIELD')
        EVENT_DATE = os.getenv('EVENT_DATE')
        for event_name, cols in self.parser.header_all_columns.items():
            duplicate = [item for item, count in collections.Counter(cols).items() if count > 1]
            if len(duplicate) > 0:
                print("[ERROR]: Event " + event_name + " contains duplicate columns: ", duplicate)
                return
            if CASE_ID_FIELD not in cols or EVENT_DATE not in cols:
                print("[ERROR]: Event " + event_name + " contains no CASE_ID_FIELD  || EVENT_DATE. Please make sure "
                                                       "that .env file has been configured properly. Keep in mind "
                                                       "that each event *MUST* contains these two columns.")
                return

        for event_name in self.parser.header_simple_columns:
            for col in self.parser.header_simple_columns[event_name]:
                if col not in body_simple_columns_all and col not in body_condition_list:
                    print("[ERROR]: attribute, <" + col + ">, within the event <" + event_name + "> is unable to locate from rule body. ")
                    return


    def validate_source_database(self):
        print("Validating body against the source table...")
        for table_name in self.parser.body_simple_columns:
            self.db.table_columns_info(table_name)
            for col in self.parser.body_simple_columns[table_name]:
                if col not in self.db.table_config_cache[table_name]:
                    print("[ERROR]: the column <" + col + "> does not exist in the source database table <" + table_name + ">")
