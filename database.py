import copy
import os.path
from dataclasses import dataclass, field
from datetime import datetime
import json

import pandas
import pandas as pd
import numpy as np
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
class Table:
    name: str
    df: DataFrame
    primary_keys: tuple
    is_unique: bool = False
    dtypes: dict = None
    primary_key: str = field(init=False)

    def __post_init__(self):
        self.primary_key = ""
        for i in range(0, len(self.primary_keys)):
            self.primary_key += str(self.primary_keys[i])
            if i < len(self.primary_keys)-1:
                self.primary_key += ":"
        # if self.dtypes is None:
        #     self.dtypes = list()
        #     for t in df.dtypes:
        #         self.dtypes.append(str(t))

    def check_uniqueness(self, data : dict):
        new_dict = dict()
        partial_table = None
        for key in self.primary_keys:
            if partial_table is None:
                #idx = np.where((self.df[key] == data[key]))
                idx = self.df[key] == data[key]
                partial_table = self.df.loc[idx]
            else:
                partial_table = partial_table.loc[partial_table[key] == data[key]]
        if partial_table is not None and len(partial_table) > 0:
            return False
        else:
            return True
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

    def add_table(self, name, columns, primary_keys: tuple, is_unique=False, dtypes=None):
        columns_dict = {}
        for column in columns:
            columns_dict[column] = []
        if dtypes is None:
            table_pd = DataFrame(data=columns_dict)
            self.tables[name] = Table(name, table_pd, primary_keys=primary_keys, is_unique=is_unique)
        else:
            dtypes_list = list()
            for k, v in dtypes.items():
                dtypes_list.append((k, v))
            dtypes_list = np.dtype(dtypes_list)
            empty_data = np.empty(0, dtype=dtypes_list)
            table_pd = DataFrame(empty_data)
            self.tables[name] = Table(name, table_pd, primary_keys=primary_keys, is_unique=is_unique, dtypes=dtypes)

    def add_row(self, table_name, data):

        table = self.tables[table_name]
        df = table.df
        df_length = len(df)
        index_of_new_row = df_length

        if isinstance(data, pd.DataFrame):
            self.tables[table_name] = self.tables[table_name].append(data)
        elif isinstance(data, list):
            # new_row = pd.Series(data=data, index=self.tables[table_name].columns)
            # self.tables[table_name] = self.tables[table_name].append(new_row, ignore_index=True)
            df.loc[df_length] = data
        elif isinstance(data, dict):
            #idx = np.where((df['Salary_in_1000'] >= 100) & (df['Age'] < 60) & (df['FT_Team'].str.startswith('S')))
            # new_row = pd.Series(data, index=self.tables[table_name].columns)
            # self.tables[table_name] = self.tables[table_name].append(new_row, ignore_index=True)
            df.loc[df_length] = data
        else:
            print("Not implemented, operation failed")
        self.log.add_log_entry(self.add_row, "table: " + table_name + ", row_index: " + str(index_of_new_row))

    def update_row(self, table_name : str, data: dict, index=None, condition=None):
        table = self.tables[table_name]
        df_table = table.df
        if index is not None:
            for key, value in data.items():
                df_table.at[index, key] = value
                self.log.add_log_entry(self.update_row, "table: " + table_name + ", row_index: " + str(index))
        elif  condition is not None:
            #https://stackoverflow.com/questions/36909977/update-row-values-where-certain-condition-is-met-in-pandas
            print("TO DO")

        else:
            partial_table = None
            for key in table.primary_keys:
                if partial_table is None:
                    # idx = np.where((self.df[key] == data[key]))
                    idx = df_table[key] == data[key]
                    partial_table = df_table.loc[idx]
                else:
                    idx = partial_table[key] == data[key]
                    partial_table = partial_table.loc[idx]
            print(partial_table)
            print(partial_table.index)
            df_table.loc[int(partial_table.index[0])] = list(data.values())


    def __post_init__(self):
        self.tables = {}
        self.datetime_created = datetime.now()
        self.datetime_last_change = self.datetime_created
        self.log = DatabaseLog()

    def save_as_csv(self, alt_path=None):
        table_info = dict()
        database_dir_path = self.persistent_path + "/" + self.name
        path_to_save = self.persistent_path if alt_path is None else alt_path
        table_dir = database_dir_path + "/tables"
        log_dir = database_dir_path + "/log"
        if not os.path.exists(path_to_save):
            os.mkdir(path_to_save)
        if not os.path.exists(database_dir_path):
            os.mkdir(database_dir_path)
        if not os.path.exists(table_dir):
            os.mkdir(table_dir)
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        for key in self.tables.keys():
            table = self.tables[key]

            path_of_table = table_dir + "/" + key + ".csv"
            table.df.to_csv(path_of_table)

            info_dict = dict()
            dtypes_dict = dict()
            if table.dtypes is None:
                for k, v in table.df.dtypes.items():
                    dtypes_dict[k] = str(v)
            else:
                for k, v in table.dtypes.items():
                    dtypes_dict[k] = str(v)
            info_dict["dtypes"] = dtypes_dict
            info_dict["primary_keys"] = table.primary_keys
            info_dict["is_unique"] = table.is_unique
            with open(table_dir + "/" + key + ".json", 'w') as f:
                json.dump(info_dict, f)
        self.log.log.to_csv(log_dir + "/log.csv")

    @staticmethod
    def load_from_csv(path, database_name):
        from datetime import datetime
        db = Database(database_name, path)
        database_dir_path = db.persistent_path + "/" + db.name
        table_dir = database_dir_path + "/tables"
        log_dir = database_dir_path + "/log"
        tables = os.listdir(table_dir)
        for table in tables:
            if ".csv" in table:
                table_name = table.split(".csv")[0]

                with open(table_dir + "/" + table_name + ".json") as f:
                    my_dict = json.load(f)
                    dtypes = my_dict["dtypes"]
                    dtypes_2 = copy.deepcopy(dtypes)
                    parse_dates = list()
                    for k, v in dtypes.items():
                        if "datetime" in v:
                            parse_dates.append(k)
                            del dtypes_2[k]

                    table_pd = pandas.read_csv(table_dir + "/" + table, index_col=[0], dtype=dtypes_2, parse_dates=parse_dates)
                    db.tables[table_name] = Table(table_name, table_pd, my_dict["primary_keys"], is_unique=my_dict["is_unique"], dtypes=dtypes)
        log = pandas.read_csv(log_dir + "/log.csv",index_col=[0], parse_dates=["time"])

        db.log.log = log
        db.datetime_created = db.log.log.iloc[0]['time'].to_pydatetime()
        db.log.add_log_entry(Database.load_from_csv, "loaded from " + database_dir_path)
        return db
