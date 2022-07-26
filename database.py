import os.path
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
from pandas import DataFrame


# to do look whats terality https://ibexorigin.medium.com/good-bye-pandas-meet-terality-its-evil-twin-with-identical-syntax-455b42f33a6d


@dataclass
class DatabaseLog:
    log: DataFrame = field(init=False)

    def __post_init__(self):
        log_data = {"action": ["init"], "info": ["init log entry"], "time": [datetime.now()]}
        self.log = DataFrame(data=log_data)

    def add_log_entry(self, function_ref, info=""):
        function_name = function_ref.__name__
        log_length = len(self.log)
        log_data = {"action": function_name, "info": info, "time": datetime.now()}
        self.log.loc[log_length] = log_data


@dataclass
class Database:
    name: str
    persistent_path: str
    # values are pandas dataframes
    tables: dict = field(init=False)
    log: DatabaseLog = field(init=False)
    datetime_created: datetime = field(init=False)
    datetime_last_change: datetime = field(init=False)




    def action(self, name, paras):
        pass

    def add_table(self, name, columns):
        columns_dict = {}
        for column in columns:
            columns_dict[column] = []
        table_pd = DataFrame(data=columns_dict)
        self.tables[name] = table_pd

    def add_row(self, table_name, data):
        df = self.tables[table_name]
        df_length = len(df)
        index_of_new_row = df_length
        if isinstance(data, pd.DataFrame):
            self.tables[table_name] = self.tables[table_name].append(data)
        elif isinstance(data, list):
            # new_row = pd.Series(data=data, index=self.tables[table_name].columns)
            # self.tables[table_name] = self.tables[table_name].append(new_row, ignore_index=True)
            df.loc[df_length] = data
        elif isinstance(data, dict):
            # new_row = pd.Series(data, index=self.tables[table_name].columns)
            # self.tables[table_name] = self.tables[table_name].append(new_row, ignore_index=True)
            df.loc[df_length] = data
        else:
            print("Not implemented, operation failed")
        self.log.add_log_entry(self.add_row, "table: " + table_name + ", row_index: " + str(index_of_new_row))

    def __post_init__(self):
        self.tables = {}
        self.datetime_created = datetime.now()
        self.datetime_last_change = self.datetime_created
        self.log = DatabaseLog()

    def save(self):
        database_dir_path = self.persistent_path + "/" + self.name
        table_dir = database_dir_path + "/tables"
        log_dir = database_dir_path + "/log"
        if not os.path.exists(self.persistent_path):
            os.mkdir(self.persistent_path)
        if not os.path.exists(database_dir_path):
            os.mkdir(database_dir_path)
        if not os.path.exists(table_dir):
            os.mkdir(table_dir)
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        for key in self.tables.keys():
            table = self.tables[key]
            path_of_table = table_dir + "/" + key + ".csv"
            table.to_csv(path_of_table)
        self.log.log.to_csv(log_dir + "/log.csv")



