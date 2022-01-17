import io
import os
import math
import inspect
import pandas as pd
from pprint import pprint
import shutil
from types import SimpleNamespace
from typing import Union, Dict, List, Tuple
from Data.Connect import pgdbbase
from Meta import pgclassdefault
from Meta import pggenericfunc
from Data.Utils import pgdirectory
from Data.Utils import pgfile
from mysql import connector
from mysql.connector import errorcode
from collections import OrderedDict
from Data.Utils import StrFunc
from Regex import pgregex
from Data.Utils import pgjson

__version__ = "1.8"

MYSQL_MAX_ROW_SIZE = 8126


def length(string: str) -> int:
    string = str(string)
    str_len = len(string)
    approx_cal = {
        '20': 40,
        '4000': 4000,
        'None': 20
    }
    index = None
    if string == "None":
        index = "None"
    elif len(string) <= 20:
        index = "20"
    elif len(string) >= 2000:
        index = "4000"
    return approx_cal[index] if index is not None else round(int(math.ceil(str_len / math.pow(10, len(str(str_len)) - 1))) * math.pow(10, len(str(str_len)) - 1) * 1.5)


def data_transform(data: dict) -> dict:
    return {k: str(v).replace("[]", "NONE") for k, v in data.items()}


def object_type(data_object) ->str:
    if isinstance(data_object, dict):
        return "dict"
    elif isinstance(data_object, list):
        return "list"
    else:
        return "other"


def args_check(args: SimpleNamespace) -> bool:
    if args:
        for val in args.__dict__.values():
            if val is None:
                return False
    return True


class PGMysqlLite(pgdbbase.PGDBBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str = "mysql", logging_enable: str = False):

        super(PGMysqlLite, self).__init__(project_name=project_name,
                                          object_short_name="PG_MSL",
                                          config_file_pathname=__file__.split('.')[0] + ".ini",
                                          logging_enable=logging_enable,
                                          config_file_type="ini")

        ### Common Variables
        self._database_type = "mysql"
        self._name = None
        self._db_parameter = {}

        #specific
        self._column_prefix = "pg"
        try:
            self._db = self._config.parameters["config_file"]['default']['database']
            _conn_info = {'user': self._config.parameters["config_file"]['default']['user'],
                          'password': self._config.parameters["config_file"]['default']['password'],
                          'host': self._config.parameters["config_file"]['default']['host'],
                          'database': self._config.parameters["config_file"]['default']['database'],
                          'port': self._config.parameters["config_file"]['default']['port']}
            self._cnx = connector.connect(**_conn_info)
        except connector.Error as err:
            if err.errno == connector.errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == connector.errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print("Please make sure database parameters in ini are set appropriately")
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)


class PGMysqlLiteExt(PGMysqlLite):
    def __init__(self, project_name: str = "mysqlExt", logging_enable: str = False):
        super(PGMysqlLiteExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _execute(self, sql: str):
        pass

    def set_table_null_default(self, table_name: str, database_name: str = None, default_val: str = "NONE1"):
        """
        This function will checks whether table exists
        return True if exists, othewise False
        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}
        """
        try:
            _database_name = database_name if database_name else self._config.parameters["config_file"]['default']['database']
            update_sql = f"update {_database_name}.{table_name} set "
            for item in self.simple_query(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{_database_name}' AND table_name = '{table_name}'"):
                update_sql += f"{item[0]} = COALESCE({item[0]}, '{default_val}'), "
            print(update_sql)

            return self.simple_query(update_sql[:-2].strip(), "update")
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def column_find_replace(self, table_name: str, x: str, y: str, database_name: str = None, ):
        """
        This function will checks whether table exists
        return True if exists, othewise False
        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}
        """
        try:
            _database_name = database_name if database_name else self._config.parameters["config_file"]['default'][
                'database']

            update_sql = f"update {_database_name}.{table_name} set "
            for item in self.simple_query(
                    f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{_database_name}' AND table_name = '{table_name}'"):
                update_sql += f"{item[0]} = CASE WHEN {item[0]} = '{x}' THEN {y} ELSE {item[0]} END, "
            print(update_sql)
            return self.simple_query(update_sql[:-2].strip(), "update")
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def column_upper_lower(self, table_name: str, is_upper: bool = False, database_name: str = None):
        """
        This function will checks whether table exists
        return True if exists, othewise False
        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}
        """
        try:
            _database_name = database_name if database_name else self._config.parameters["config_file"]['default'][
                'database']

            update_sql = f"update {_database_name}.{table_name} set "
            for item in self.simple_query(
                    f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{_database_name}' AND table_name = '{table_name}'"):
                update_sql += f"{item[0]} = upper({item[0]}), " if is_upper else f"{item[0]} = lower({item[0]}), "
            print(update_sql)
            return self.simple_query(update_sql[:-2].strip(), "update")
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def table_exist(self, table_name, database_name: str = None):
        """
        This function will checks whether table exists
        return True if exists, othewise False
        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}
        """
        try:
            if database_name is None: database_name = self._db

            db_query = f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{database_name}' AND table_name = '{table_name}'"
            return False if not self.simple_query(db_query) else True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def populate_data(self, table_name, data_in, mode: str = "simple", database_name=None):
        """
        Simple mode:  Will apply schema evolution
        Bulk mode: Will skip schema evolution
        """
        try:
            if not self.table_exist(table_name):
                self.create_table(table_name=table_name, data_in=data_in)
            if mode == "simple":
                self.adjust_table(table_name=table_name, data_in=data_in)
            if self.insert_table(table_name=table_name, mode=mode, data=data_in):
                return True
            else:
                return False
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def alter_table(self, *, table_name: str, column_info: dict, mode: str, database_name=None):
        if database_name is None: database_name = self._db
        try:
            alter_table_query = f"alter table {database_name}.{table_name} "

            for column_name, column_size in column_info.items():
                alter_table_query += f"{mode} {column_name} varchar({column_size}), "

            alter_table_query = alter_table_query[:-2]
            self.simple_query(alter_table_query, mode="alter")
            print(f"table {table_name} is adjusted")
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

    def adjust_table(self, *, table_name: str, data_in, database_name=None):
        """
        This function decides the adjustment needed if input data doesn't fit current table_schema
        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}
        """
        try:
            if database_name is None: database_name = self._db

            table_query =   f"SELECT column_name, CHARACTER_MAXIMUM_LENGTH FROM " \
                            f"information_schema.columns WHERE table_schema = '{database_name}' " \
                            f"AND table_name = '{table_name}'"
            column_info = dict(self.simple_query(table_query))

            if column_info:
                data_in = {f"{self._column_prefix}_{k}": v for k, v in data_in.items()}
                shared_items = {k: length(data_in[k]) for k in column_info if
                                k in data_in and int(column_info[k]) < len(str(data_in[k]))}
                missing_items = {k: length(data_in[k]) for k in data_in if k not in column_info}
                if shared_items:
                    self.alter_table(table_name=table_name, column_info=shared_items, mode="modify")
                if missing_items:
                    self.alter_table(table_name=table_name, column_info=missing_items, mode="add")
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

    def clean_data(self, record: dict):
        return {pgregex.parse_string("clean_column_name", [_ind])[0]: _val for _ind, _val in record.items()}

    def create_table(self, *, table_name, data_in):
        """
        This function will create table based on column name provided in the data
        It will create varchar datatype for all columns and
        determine the column length based on inferring the data itself
        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}
        """
        try:
            #print("aaaaa")
            #print(data_in)

            if isinstance(data_in, list):
                data_in = data_in[0]
            #[self.clean_data(x) for x in pg_data]
            #data_in
            stock_info = OrderedDict(sorted(data_in.items()))
            #print(stock_info)

            #exit(0)

            total_row_size: int = 0
            create_table_query = f"create table {table_name} ("
            for key, val in stock_info.items():
                proposed_col_size = length(str(val))
                total_row_size += proposed_col_size
                if key == list(stock_info.keys())[-1]:
                    create_table_query += f"{self._column_prefix}_{key} varchar({proposed_col_size})) "
                else:
                    create_table_query += f"{self._column_prefix}_{key} varchar({proposed_col_size}),"
            #print(create_table_query)

            #exit(0)
            if total_row_size >= MYSQL_MAX_ROW_SIZE:
                raise (
                    f"Table {table_name} rowsize {total_row_size} is larger than mysql rowsize limitation of {MYSQL_MAX_ROW_SIZE}")
            else:
                print(f"the max row size of table {table_name} is calculated as {total_row_size}")

            #print(create_table_query)

            #exit(0)
            cursor = self._cnx.cursor()

            print(f"Creating table {table_name}...")
            cursor.execute(create_table_query)
        except connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        else:
            print(f"Table {table_name} created")

            cursor.close()

    def insert_table(self, table_name=None, mode: str = "simple", data=None, database_name=None):
        """
        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}
        record format for bulk mode is expected:  [('columnName1': 'data1', 'columnName2': 'data2'),
                                                  ('columnName1': 'data1', 'columnName2': 'data2')...}
        """

        if database_name is None:
            database_name = self._db

        if isinstance(data, dict):
            data = [data]

        try:
            new_data = [StrFunc.wordtransform({f"{self._column_prefix}_{k}": v for k, v in _data.items()}) for _data in
                        data]

            insert_query_part1 = f"INSERT INTO {table_name} ( "
            for key, val in new_data[0].items():
                if key == list(new_data[0].keys())[-1]:
                    insert_query_part1 += f"{key} )"
                else:
                    insert_query_part1 += f"{key}, "
            insert_query_part2 = " VALUES ( " + (len(data[0]) - 1) * "%s, " + "%s )"
            # print(new_data)
            # print(mode)
            _pg_records = [tuple(x.values()) for x in new_data]

            cursor = self._cnx.cursor()
            if mode == "bulk":
                cursor.executemany(f"{insert_query_part1} {insert_query_part2}", _pg_records)
                print(f"inserted {len(new_data)} records to table {table_name}")
            else:
                for _pg_record in _pg_records:
                    cursor.execute(f"{insert_query_part1} {insert_query_part2}", _pg_record)
                print(f"inserted 1 record to table {table_name}")
            cursor.close()
            self._cnx.commit()
        except connector.Error as err:
            print(f"Error:  {err}")  # errno, sqlstate, msg values
            return False
        return True

    def simple_query(self, query_in: str, mode: str ="select"):
        """
        Simply query takes a SQL and return back results in a list of tuples
        """
        try:
            cursor = self._cnx.cursor()
            cursor.execute(query_in)
            if mode == "select":
                data_out = cursor.fetchall()
            self._cnx.commit()
            cursor.close()
            if mode == "select":
                return data_out
        except connector.Error as err:
            print(f"Error:  {err}")  # errno, sqlstate, msg values

    def execute(self, *, table_name, val=None):
        query = ("SELECT column_name FROM information_schema.columns WHERE table_schema = %s "
                 "AND table_name = %s")
        gen_sql = [f"insert into {table_name}"]
        cursor = self._cnx.cursor()
        cursor.execute(query, ("stock_db", table_name))
        # query = ("SELECT first_name, last_name, hire_date FROM employees "
        #         "WHERE hire_date BETWEEN %s AND %s")
        for (column_name) in cursor:
            print(f"{column_name[0]}")
            gen_sql.append(column_name[0])
        cursor.close()

    def pg_load_bulk(self, pg_table_name: str, pg_data: Union[list, dict], ignore_error: bool = False, exception_dirpath: str = None) -> bool:
        try:
            if not self.populate_data(table_name=pg_table_name, mode="bulk", data_in=pg_data):
                if exception_dirpath:
                    with open(exception_dirpath, 'a') as exception_file:
                        exception_file.write(f"{pg_data}\n")
                if ignore_error:
                    print("ignore error is on, process next batch")
                else:
                    return False
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

        return True

    def pg_load_simple(self, pg_table_name: str, pg_data: Union[list, dict], ignore_error: bool = False, exception_dirpath: str = None) -> bool:
        try:
            for _data_item in pg_data:
                if not self.populate_data(table_name=pg_table_name, mode="simple", data_in=_data_item):
                    if exception_dirpath:
                        with open(exception_dirpath, 'a') as exception_file:
                            exception_file.write(f"{_data_item}\n")
                    if ignore_error:
                        print("ignore error is on, process next record")
                    else:
                        return False
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

        return True
    """
    def pg_save(self, pg_table_name: str, pg_data: Union[list, dict], exception_dirpath: str = None, pg_load_mode: str = "auto") -> bool:
        # print(db_session)
        # print(f" Start insert data for ...")
        # mysql.simple_query(f"update stock_queue set status = 'WIP' where stock_symbol = '{stock_symbol}'")

        try:
            if pg_load_mode == "auto":
                pg_load_mode = "bulk" if len(pg_data) > 40 else "simple"

            if isinstance(pg_data, dict):
                pg_data = list(pg_data)
                
            if pg_load_mode == "bulk":
                if not self.populate_data(table_name=pg_table_name, mode=pg_load_mode,
                                          data_in=pg_data) and exception_dirpath:
                    with open(exception_dirpath, 'a') as exception_file:
                        exception_file.write(f"{pg_data}\n")
            else:
                for _data_item in pg_data:
                    if not self.populate_data(table_name=pg_table_name, mode=pg_load_mode, data_in=_data_item) and exception_dirpath:
                        with open(exception_dirpath, 'a') as exception_file:
                            exception_file.write(f"{pg_data}\n")

            print(f"Data is successfully loaded to {pg_table_name}\n\n")
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    """

    def pg_save(self, pg_table_name: str, pg_data: Union[list, dict], pg_ignore_error: bool = False, exception_dirpath: str = None,
                pg_load_mode: str = "auto") -> bool:
        # print(db_session)
        # print(f" Start insert data for ...")
        # mysql.simple_query(f"update stock_queue set status = 'WIP' where stock_symbol = '{stock_symbol}'")

        try:
            if isinstance(pg_data, list):
                pg_data = [self.clean_data(x) for x in pg_data]
            else:
                pg_data = [self.clean_data(x) for x in [pg_data]]
            if pg_load_mode == "auto":
                pg_load_mode = "bulk" if len(pg_data) > 40 else "simple"

            if isinstance(pg_data, dict):
                pg_data = list(pg_data)

            if pg_load_mode == "bulk":
                if not self.pg_load_bulk(pg_table_name, pg_data, False, exception_dirpath):
                    print(f"Bulk load mode failed, switch mode to simple for {pg_table_name}\n\n")
                    if not self.pg_load_simple(pg_table_name, pg_data, pg_ignore_error, exception_dirpath):
                        print(f"Simple load mode failed for {pg_table_name}\n\n")
                        return False
            elif pg_load_mode == "simple":
                if not self.pg_load_simple(pg_table_name, pg_data, pg_ignore_error, exception_dirpath):
                    print(f"Simple load mode failed for {pg_table_name}\n\n")
                    return False

            print(f"Data is successfully loaded to {pg_table_name}\n\n")
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def pg_fs_2_db(self, pg_table_name: str, pg_data_dirpath: str, pg_exception_dirpath: str):
        """
        Expects a directory path where a list of json files

        json files are format   [
                                {column1_name: column1_value, column2_name: column2_value...},
                                {column1_name: column1_value, column2_name: column2_value...},
                                {column1_name: column1_value, column2_name: column2_value...},
                                ...
                                ]
        """
        def _processed(dirpath, processed_file_list: dict = {}):
            for file_name in pgfile.get_all_file_in_dir(dirpath):
                n = processed_file_list.get(file_name, None)
                if not n and file_name != "_processed._internal":
                    #_not_processed.append(file_name)
                    yield file_name
        try:
            _existing_files = pgjson.pg_deserialize_from_disk(f"{pg_data_dirpath}/_processed._internal")
            for file_name in _processed(pg_data_dirpath, _existing_files):
                print(file_name)
                if self.pg_save(pg_table_name, pgjson.pg_deserialize_from_disk(f"{pg_data_dirpath}/{file_name}"), True, pg_exception_dirpath, "bulk"):
                #if self.save_to_db("pg_panini_analysis", pgjson.pg_deserialize_from_disk(f"{dirpath}/{file_name}")):
                    _existing_files[file_name] = "processed"
                else:
                    print("processed failed")

            pgjson.pg_serialize_to_disk(_existing_files, f"{pg_data_dirpath}/_processed._internal")

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None


if __name__ == '__main__':
    #a = PGMysqlLite()
    b = PGMysqlLiteExt()
    exit(0)

    #b.set_table_null_default("re_house_school_detail", "pg_real_estate", "NoN")

    #b.column_find_replace("re_house_school_detail", "NoN", "NULL")
    #b.column_find_replace("sold_property_aggregrated_06_11", "NoN", "NULL")
    #b.column_upper_lower("sold_property_aggregrated_06_11")
    b.set_table_null_default("sold_property_aggregrated_06_11", "pg_real_estate", "0.5")




