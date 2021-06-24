from __future__ import print_function
from Data.Utils import StrFunc
from sqlalchemy import exc
import mysql.connector
from mysql.connector import errorcode
import Data.Utils.pgyaml as pgyaml
import yfinance as yf
from collections import OrderedDict
import math
import sqlalchemy
from sqlalchemy.dialects.mysql import pymysql
from sqlalchemy.orm import sessionmaker, scoped_session
from Data.Security import encryption
from Data.Config import pgconfig
from Data.Connect import base
from types import SimpleNamespace

import pandas as pd

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


class PGMysql:

    def __init__(self, args=None, filename=None):
        try:
            if filename:
                self._cnx = mysql.connector.connect(**pgyaml.yaml_load(yaml_filename=filename))
            elif isinstance(args, SimpleNamespace) and args_check(args):
                self._cnx = mysql.connector.connect(args.__dict__)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        #else:
        #    self._cnx.close()
        self._column_prefix = "pg"
        self._db = "stock_db"

    def _execute(self, sql: str):
        pass

    def table_exist(self, table_name, database_name=None):
        """
        This function will checks whether table exists

        return True if exists, othewise False

        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}

        """
        if database_name is None:
            database_name = self._db

        db_query = f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{database_name}' AND table_name = '{table_name}'"
        if not self.simple_query(db_query):
            return False
        return True

    def populate_data(self, table_name, data_in, mode="simple", database_name=None):
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
            print(f"Error:  {err}")
            return False

    def alter_table(self, *, table_name: str, column_info: dict, mode: str, database_name=None):
        if database_name is None:
            database_name = self._db
        try:
            alter_table_query = f"alter table {database_name}.{table_name} "

            for column_name, column_size in column_info.items():
                alter_table_query += f"{mode} {column_name} varchar({column_size}), "
            alter_table_query = alter_table_query[:-2]
            self.simple_query(alter_table_query, mode="alter")
            print(f"table {table_name} is adjusted")
        except Exception as err:
            print(f"Error:  {err}")

    def adjust_table(self, *, table_name: str, data_in, database_name=None):
        """
        This function decides the adjustment needed if input data doesn't fit current table_schema

        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}

        """
        if database_name is None:
            database_name = self._db

        table_query = f"SELECT column_name, CHARACTER_MAXIMUM_LENGTH FROM " \
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

    def create_table(self, *, table_name, data_in):

        """
        This function will create table based on column name provided in the data
        It will create varchar datatype for all columns and
        determine the column length based on inferring the data itself

        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}

        """
        stock_info = OrderedDict(sorted(data_in.items()))
        total_row_size: int = 0
        create_table_query = f"create table {table_name} ("
        for key, val in stock_info.items():
            proposed_col_size = length(str(val))
            total_row_size += proposed_col_size
            if key == list(stock_info.keys())[-1]:
                create_table_query += f"{self._column_prefix}_{key} varchar({proposed_col_size})) "
            else:
                create_table_query += f"{self._column_prefix}_{key} varchar({proposed_col_size}),"

        if total_row_size >= MYSQL_MAX_ROW_SIZE:
            raise (f"Table {table_name} rowsize {total_row_size} is larger than mysql rowsize limitation of {MYSQL_MAX_ROW_SIZE}")
        else:
            print(f"the max row size of table {table_name} is calculated as {total_row_size}")
        cursor = self._cnx.cursor()
        try:
            print(f"Creating table {table_name}...")
            cursor.execute(create_table_query)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print(f"Table {table_name} created")
        cursor.close()

    def insert_table(self, table_name=None, mode="simple", data=None, database_name=None):
        """
        record format for data is expected:  {'columnName1': 'data1', 'columnName2': 'data2'...}
        record format for bulk mode is expected:  [('columnName1': 'data1', 'columnName2': 'data2'),
                                                  ('columnName1': 'data1', 'columnName2': 'data2')...}
        """

        if database_name is None:
            database_name = self._db

        if mode == "simple":
            new_data = {f"{self._column_prefix}_{k}": v for k, v in data.items()}

        insert_query_part1 = f"INSERT INTO {table_name} ( "
        if mode == "bulk":
            table_query = f"SELECT column_name FROM " \
                          f"information_schema.columns WHERE table_schema = '{database_name}' " \
                          f"AND table_name = '{table_name}'"

            column_list = self.simple_query(table_query)
            for item in column_list:
                column_name = item[0]
                if column_name == column_list[-1][0]:
                    insert_query_part1 += f"{column_name} )"
                else:
                    insert_query_part1 += f"{column_name}, "
            insert_query_part2 = " VALUES ( " + (len(data[0]) - 1) * "%s, " + "%s )"

        else:
            insert_query_part2 = f" VALUES ( "
            for key, val in new_data.items():
                if key == list(new_data.keys())[-1]:
                    insert_query_part1 += f"{key} )"
                    insert_query_part2 += f"%({key})s )"
                else:
                    insert_query_part1 += f"{key}, "
                    insert_query_part2 += f"%({key})s, "

        try:
            cursor = self._cnx.cursor()
            if mode == "bulk":
                print(f"{insert_query_part1} {insert_query_part2}")
                cursor.executemany(f"{insert_query_part1} {insert_query_part2}", data)
                num_of_records = len(data)
                print(f"inserted {num_of_records} records to table {table_name}")
            else:
                cursor.execute(f"{insert_query_part1} {insert_query_part2}", StrFunc.wordtransform(new_data))
                print(f"inserted 1 record to table {table_name}")
            cursor.close()
            self._cnx.commit()
        except mysql.connector.Error as err:
            print(f"Error:  {err}")  # errno, sqlstate, msg values
            return False

        return True

    def simple_query(self, query_in: str, mode="select"):
        """
        Simply query takes a SQL and return back results in a list of tuples
        """

        try:
            cursor = self._cnx.cursor()
            cursor.execute(query_in)
            if mode == "select":
                data_out = cursor.fetchall()
            cursor.close()
            self._cnx.commit()
            if mode == "select":
                return data_out
        except mysql.connector.Error as err:
            print(f"Error:  {err}")  # errno, sqlstate, msg values

    def execute(self, *, table_name, val=None):

        query = ("SELECT column_name FROM information_schema.columns WHERE table_schema = %s "
                 "AND table_name = %s")

        gen_sql = [f"insert into {table_name}"]
        cursor = self._cnx.cursor()
        cursor.execute(query, ("stock_db", table_name))

        #query = ("SELECT first_name, last_name, hire_date FROM employees "
        #         "WHERE hire_date BETWEEN %s AND %s")

        for (column_name) in cursor:
            print(f"{column_name[0]}")
            gen_sql.append(column_name[0])

        cursor.close()


class ConnectMysql(base.Base):
    def __init__(self, args=None):
        if not args:
            self._conf = pgconfig.Config()
            self._host = getattr(self._conf, 'MYSQL_HOST')
            self._username = getattr(self._conf, 'MYSQL_USERNAME')
            #if self.entity == 'meta':
            #    sp = encrypt.SecurityPass(db_system='meta', postfix=self.post_fix)
            #else:
            #    sp = encrypt.SecurityPass(db_system='rds', postfix=self.post_fix)
            #self._post_userpass = sp.gen_decrypt(entity=self.entity)
            self._userpass = getattr(self._conf, 'MYSQL_PASS')
            print(self._userpass)
            self._port = getattr(self._conf, 'MYSQL_PORT')
            self._db = getattr(self._conf, 'MYSQL_DB')
            #self._post_url = getattr(conf, 'SCOOT_RDS_POST_URL')
        else:
            self._host = args.host
            self._username = args.username
            self._userpass = args.userpass
            self._port = args.port
            self._db = args.db
            #self._post_url = args.post_url

        try:
            self._url = f"mysql+mysqlconnector://{self._username}:{self._userpass}@{self._host}:{self._port}/{self._db}"
            self._engine_args = dict()
            self._engine_args['pool_size'] = 5
            self._engine_args['pool_recycle'] = 3600
            self._engine_args['encoding'] = 'utf-8'
            ##** self._engine_args
            print(f"mysql+pymsql://{self._username}:{self._userpass}@{self._host}:{self._port}/{self._db}")

            self._engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{self._username}:"
                                                    f"{self._userpass}@{self._host}:"
                                                    f"{self._port}/{self._db}", **self._engine_args)

            self._Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self._engine,
                                                        expire_on_commit=False))

            self._session = self._Session()

        except Exception as err:
            print('Exception: Error connecting database.')
            raise err
        else:
            print(f"Connected to {self._db} on {self._host} with user: {self._username}")

    @property
    def url(self):
        return self._url

    @property
    def session(self):
        return self._session

    @property
    def engine(self):
        return self._engine

    def execute(self, query):
        try:
            self._session.execute(query)
        except Exception as e:
            raise (f"Encounter error in running SQL {query} error: {e}")

    def commit(self):
        try:
            self._session.commit()

        except exc.IntegrityError:
            self._session.rollback()
            return False

        #except exc.DatabaseError as err:
            #print(f"Something wrong with committing data {err}")

        return True

    def close(self):
        self._session.close()
        print(f"Closed connection to {self._db} on {self._host} with user: {self._username}")

    def get_table_list(self):
        return self._session.execute(f"select table_name from information_schema.tables where "
                                     f"table_type = 'BASE TABLE' and table_schema = {self._db}")


    def get_db_metadata(self, table_name):
        return self._session.execute(f"select table_name, column_name, data_type, character_maximum_length, "
                                     f"numeric_precision, numeric_scale FROM INFORMATION_SCHEMA.COLUMNS WHERE "
                                     f"table_name = '{table_name}' order by 1;").fetchall()

    def __repr__(self):
        return f"{self.__class__.__name__}({self._username}@{self._host})"

def setup_mysql(self):
    host = getattr(self, 'MYSQL_HOST')
    username = getattr(self, 'MYSQL_USERNAME')
    userpass = getattr(self, 'MYSQL_PASS')
    port = getattr(self, 'MYSQL_PORT')
    db = getattr(self, 'MYSQL_DB')

    Rds = namedtuple('Rds', ['host',
                             'username',
                             'userpass',
                             'port',
                             'post_db'])

    return Rds(host=host,
               username=username,
               post_userpass=userpass,
               post_port=rds_post_port,
                   post_db=rds_post_db)

if __name__ == '__main__':

    stock = yf.Ticker("ADVWW")
    stock.info

    exit(0)
    mysql = PGMysql("/Users/jianhuang/opt/anaconda3/envs/stock_data/mysql.yml")


    #mysql.create_table(tablename="stock_detail3",data=stock.info)
    #mysql.insert_table(table_name="stock_detail3", data=stock.info)

    #mysql.execute(table_name="stock_detail")
    mysql.table_exist("stock_detail222")
    #print(mysql.simple_query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'stock_db' AND table_name = 'stock222'"))

    column_info = {
        'pg_52WeekChange': 100,
        'pg_address1': 100
    }

    #mysql.alter_table(table_name="stock_detail2", column_info=column_info)
    #mysql.adjust_table(table_name="stock_detail2", data_in=stock.info)
    #print(type(stock.info))
    """
    data = [
        ('Jane', '100'),
        ('Joe', '100'),
        ('John', '100'),
    ]

    mysql.insert_table("test", "bulk", data)

    exit(0)

    """

    stock = yf.Ticker("ADVWW")
    #stock = yf.Ticker("AMZN")
    #print(len(stock.info.keys()))

    mysql.populate_data(table_name="stock_detail", mode="simple", data_in=stock.info)
    #stock = yf.Ticker("AMZN")
    #mysql.populate_data(table_name="stock_detail", mode="simple", data_in=stock.info)