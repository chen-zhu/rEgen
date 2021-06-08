import mysql.connector
import os
from dotenv import load_dotenv
from pypika import Query, Column, Table, Field

load_dotenv(override=True)
SOURCE_HOST = os.getenv('SOURCE_HOST')
SOURCE_USER = os.getenv('SOURCE_USER')
SOURCE_PASSWORD = os.getenv('SOURCE_PASSWORD')
SOURCE_DATABASE = os.getenv('SOURCE_DATABASE')


class database:

    def __init__(self, source_db=None):
        source_db = source_db if source_db is not None else SOURCE_DATABASE
        self.source_db = mysql.connector.connect(
            host=SOURCE_HOST,
            user=SOURCE_USER,
            password=SOURCE_PASSWORD,
            database=source_db,
            raw=False
        )
        self.source_cursor = self.source_db.cursor()
        self.table_config_cache = {}

    def table_columns_info(self, table_name):
        # only run if there is no such table cached!
        if table_name not in self.table_config_cache:
            query = "SHOW FIELDS FROM " + table_name
            self.source_cursor.execute(query)
            table_config = self.source_cursor.fetchall()
            self.table_config_cache[table_name] = {}
            for row_info in table_config:
                self.table_config_cache[table_name][row_info[0]] = row_info[1].decode()
        # print("table_columns_info", self.table_config_cache)

    def execute_query(self, query, run_on_source=True, need_commit=False, return_last_insert_id=False, dictionary_cursor=False):
        using_db = self.source_db
        running_cursor = using_db.cursor(dictionary=dictionary_cursor)
        running_cursor.execute(query)
        rows = running_cursor.fetchall()

        if need_commit:
            using_db.commit()
        if return_last_insert_id:
            return running_cursor.lastrowid
        return rows
