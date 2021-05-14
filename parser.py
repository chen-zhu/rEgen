from grammar import *


class parser:

    def __init__(self):
        self.mapping_name = ""
        # use tons of memory to boost efficiency here ;P
        self.raw_mapping_body = []
        self.raw_mapping_header = []
        self.reverse_column_map = {}  # add this line to improve efficiency!
        self.body_all_columns = {}
        # body dir: contains table index and columns
        self.body_simple_columns = {}
        # body condition dir: contains table index and conditional columns info
        self.body_condition_columns = {}
        # body variable mapping
        self.body_variable_map = {}

        self.header_all_columns = {}
        # header dir: contains table index and columns
        self.header_simple_columns = {}
        # header condition dir: contains table index and conditional columns info
        self.header_condition_columns = {}

    def parsingFile(self, file_path):
        # print("\nParsing <File Path:" + file_path + ">")
        grammar_structure = grammar().Syntax()
        try:
            parsed_result = grammar_structure.parseFile(file_path)
        except Exception as ex:
            print(file_path + " parsing failed: ")
            print(ex)
            return False
        # print(parsed_result)
        return parsed_result

    def tokenize(self, parsed_result):
        if not parsed_result:
            return
        self.mapping_name = parsed_result[0]
        self.raw_mapping_body = parsed_result[1]
        self.raw_mapping_header = parsed_result[2]
        for body in self.raw_mapping_body:
            table_name = body[0]
            self.body_all_columns[table_name] = []
            self.body_simple_columns[table_name] = []
            self.body_condition_columns[table_name] = []
            for col in body[1]:
                if isinstance(col, (float, int, str)):
                    self.body_simple_columns[table_name].append(col)
                    self.body_all_columns[table_name].append(col)
                    self.reverse_column_map[col] = table_name
                else:
                    self.body_condition_columns[table_name].append(col)
                    self.body_all_columns[table_name].append(col[0])
                    self.reverse_column_map[col[0]] = table_name
                    # also put mapped variable into body_variable_map
                    if len(col) == 3 and col[1] == ":":
                        col_name = col[0]
                        third_val = col[2]
                        # only pick out variable assignment. Ignore string/number filter!
                        if not self.is_string_or_number_or_NULL_or_bool(third_val):
                            if third_val in self.body_variable_map:
                                self.body_variable_map[third_val].append(table_name + "." + col_name)
                            else:
                                self.body_variable_map[third_val] = [col_name, table_name + "." + col_name]
        for header in self.raw_mapping_header:
            event_name = header[0]
            self.header_all_columns[event_name] = []
            self.header_simple_columns[event_name] = []
            self.header_condition_columns[event_name] = []
            for col in header[1]:
                if isinstance(col, (float, int, str)):
                    self.header_simple_columns[event_name].append(col)
                    self.header_all_columns[event_name].append(col)
                else:
                    self.header_condition_columns[event_name].append(col)
                    self.header_all_columns[event_name].append(col[0])

    def is_string_or_number_or_NULL_or_bool(self, value):
        if "\"" in value:
            return True
        if "'" in value:
            return True
        if value.replace('.', '', 1).isdigit():
            return True
        if value.lower() in ["null", "notnull"]:
            return True
        if value.lower() in ["true", "false"]:
            return True
        return False

    def structurePrettyPrint(self, parsed_result):
        if not parsed_result:
            return
        self.mapping_name = parsed_result[0]
        self.raw_mapping_body = parsed_result[1]
        self.raw_mapping_header = parsed_result[2]
        print("\n\n---------------" + self.mapping_name + "---------------")
        self.tokenize(parsed_result)
        debug = False
        if debug:
            print("Body Column: ", self.body_simple_columns)
            print("Body Condition: ", self.body_condition_columns)
            print("Body Variable Map", self.body_variable_map)
            print("Body All: ", self.body_all_columns)
            # print("Reverse_map: ", self.reverse_column_map)
            print("Header Column: ", self.header_simple_columns)
            print("Header Condition: ", self.header_condition_columns)
            print("Header All: ", self.header_all_columns)
        print("")
