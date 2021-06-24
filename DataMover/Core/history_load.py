# !/usr/bin/env python3

from subprocess import Popen, PIPE, call, STDOUT
from Data.Utils.db import validate_table_exist
from Data.Utils.db import get_column_name_by_order
from Data.Utils.pgfile import get_random_filename
from Data.Connect import postgresql
import datetime
import gzip
import boto3
import psycopg2
import multiprocessing
import time
import random
import os
from multiprocessing import Queue
from Data.Config import pgconfig
from types import SimpleNamespace
from Data.Logging import pglogging as log
import logging
import sys
from Data.Utils import pgdirectory as dirFunc
from Data.Utils import query as qy
from Data.Utils import check, query
from types import SimpleNamespace
from Data.Utils import db
import subprocess
from Data.Utils import pgprocess

from DataValidator.Core import validate
from Data.Utils import pgfile
from time import sleep
from DataMover.Mode import fullmode

loggingLevel = logging.INFO
logging.basicConfig(level=loggingLevel)

q = Queue()


class Postgres2Redshift_hist:

    def __init__(self, args=None):

        self.conf = pgconfig.Config()

        if args:
            self.args = args
            self.args.parameters = self.conf.parameters
            self.args.name = 'History_load'

        else:
            raise ("Missing table and configuration info, please pass them into the object.")

        #print(self.conf.parameters)
        #print(dir(self.conf))

        try:
            if not self.args.logger:
                self.args.logger = log.Logging(self.conf, logging_level=loggingLevel,
                                               subject='{0} logger'.format(self.args.name)).getLogger(self.args.name)

        except Exception as err:
            logging.critical('unable to instantiate Daemon logger' + str(err))
            sys.exit(300)

        self._extract_filename = None
        self._complete_path = None
        self._white_table_list = None
        self._url = None
        self._db_client_home = None
        self._db_client_dbshell = (getattr(self.conf, 'SCOOT_CLIENT_DBSHELL')).strip()
        self._extract_filename = None
        self._s3_bucket_location = getattr(self.conf, 'SCOOT_S3_BUCKET_LOC')
        #print(self._s3_bucket_location)
        self.tablecount = {}
        self.application_name = "data_mover"
        self.args.logger.info("Environment variables loadded correctly")
        self._postgresql_host = None
        self._postgresql_username = None
        self._postgresql_password = None
        self._post_port = None
        self._postgresql_database = None
        self._postgresql_url = None
        self._redshift_host = None
        self._redshift_user = None
        self._redshift_pass = None
        self._redshift_db = None
        self._redshift_port = None
        self._redshift_url = None

    @property
    def complete_path(self):
        return self._complete_path

    @property
    def s3_bucket_location(self):
        return self._s3_bucket_location

    @property
    def extract_filename(self):
        return self._extract_filename

    @property
    def db_client_dbshell(self):
        return self._db_client_dbshell

    def setup_postgresql(self):

        self._postgresql_host = getattr(self.conf, 'SCOOT_RDS_POST_HOST')
        self._postgresql_username = getattr(self.conf, 'SCOOT_RDS_POST_USERNAME')
        self._postgresql_password = getattr(self.conf, 'SCOOT_RDS_POST_PASS')
        self._post_port = getattr(self.conf, 'SCOOT_RDS_POST_PORT')
        self._postgresql_database = getattr(self.conf, 'SCOOT_RDS_POST_DB')
        self._postgresql_url = f"postgresql://{self._postgresql_username}:" \
                               f"{self._postgresql_password}@{self._postgresql_host}/{self._postgresql_database}"
        #print(self._postgresql_url)
        #self._db_client_home = db_client_home
        self.args.logger.info("Sucessfully initiated db connection to RDS...")

    def setup_redshift(self):

        self._redshift_host = getattr(self.conf, 'SCOOT_REDSHIFT_HOST')
        self._redshift_user = getattr(self.conf, 'SCOOT_REDSHIFT_USERNAME')
        self._redshift_pass = getattr(self.conf, 'SCOOT_REDSHIFT_PASS')
        self._redshift_db = getattr(self.conf, 'SCOOT_REDSHIFT_DB')
        self._redshift_port = getattr(self.conf, 'SCOOT_REDSHIFT_PORT')
        self._redshift_url = f"postgresql://{self._redshift_user}:{self._redshift_pass}@{self._redshift_host}:{self._redshift_port}/{self._redshift_db}"
        self.args.logger.info("Sucessfully initiated db connection to Data Warehouse...")

    def query_builder(self, *, table_name: str, valid_flag=False):
        #print(self.args.arguments.timestamp_col)
        #print(self.args.arguments.key_col)

        try:
            if "," in self.args.arguments.timestamp_col:
                raise ("Only one key column supported")
            else:
                where_clause = f"where {self.args.arguments.timestamp_col} between {self.args.system_parameter.lowwatermark}"\
                              f" and {self.args.system_parameter.highwatermark}"

                if valid_flag:
                        return f"select count(1) from {table_name} {where_clause}"

                else:
                    return f"select * from {table_name} {where_clause}"

        except Exception as err:
            raise (f"Could not extract column list {err}")

    @db.connect('rds')
    def start_end2end_count_validation(self, db_instance=None):
        # print("Start end to end Data validation...")
        validation_query = self.query_builder(valid_flag=False)

        try:
            # print(validation_query)
            query_result = db_instance.session.execute(validation_query)
            item = query_result.fetchall()
            # print(item[0][0], item[0][1], item[0][2])
            # print(f"The number of records in source table is {item[0][2]}")

        except Exception as e:
            raise (f"Something wrong with executing count validation query! {e}")

        return item[0][0]

    def start_end2end_count_validation_v2(self):
        try:
            # print(args)
            validation_query = self.query_builder(table_name=self.args.arguments.source_object, valid_flag=True)

            result = pgprocess.run_query(db_client_dbshell=self._db_client_dbshell, db_url=self._postgresql_url, query=validation_query)
            #print(type(result))
            #print(result)

        except Exception as e:
            print(e)
            return False, 0

        if result:
            return True, result[0]
        else:
            return False, 0

    def extract(self):

        extract_query = self.query_builder(table_name=self.args.arguments.source_object, valid_flag=False)

        column_list = get_column_name_by_order(self.args.arguments.source_object)
        column_list.sort(key=lambda x: x[1])
        self.args.column_list = column_list

        db_client_dbshell = self._db_client_dbshell.strip()

        header_str = ''
        # if opt.ora_add_header:
        #	header_str=' CSV HEADER'

        limit = ''
        #if opt.pgres_lame_duck > 0:
        #    limit = 'LIMIT %d' % opt.pgres_lame_duck

        quote = ''
        #if opt.pgres_lame_duck > 0:
        #    quote = 'QUOTE  \'%s\'' % opt.pgres_quote

        #query = f"COPY (({tablename}) {limit}) TO STDOUT WITH DELIMITER ',' CSV {quote}"
        # select count(1) from batteries where created_at >= '2019-02-25 19:56:10.085183' or updated_at >= '2019-02-25 19:56:10.085183'

        query = f"COPY ({extract_query} {limit} ) TO stdout DELIMITER ',' CSV {quote}"


        #query = f"COPY {tablename} {limit} TO stdout DELIMITER ',' CSV {quote}"
        self.args.logger.info(f"Executing Data Extraction logic....")

        try:
            loadConf = [db_client_dbshell, self._postgresql_url, "-c", query]

            save_directory = self.conf.parameters.get('SCOOT_DATA_HOME', '') + "/hist_data/" + \
                             self.application_name + '/' + self.args.arguments.source_object

            dirFunc.createdirectory(save_directory)

            file_name_save = "hist_save" + get_random_filename(self.args.arguments.source_object) + ".csv"
            complete_path = save_directory + "/" + file_name_save

            with open(complete_path, 'w') as f:
                p2 = Popen(loadConf, stdout=f)
                p2.wait()

            if p2.returncode != 0:
                return False, False

            self._extract_filename = file_name_save
            self._complete_path = complete_path

            self.args.logger.info(f"Data Extraction is successfully completed and data is saved at {file_name_save}.")

        except Exception as err:
            self.args.logger.info(f"Data Extraction process failed: {err}")
            return False, False

        return True, True

    def upload_s3(self):

        self.args.logger.info("start to load to s3...")

        s3_bucket_access_key = getattr(self.conf, 'SCOOT_S3_BUCKET_ACCESS_KEY')
        s3_bucket_secret_key = getattr(self.conf, 'SCOOT_S3_BUCKET_SECRET_KEY')

        try:
            session = boto3.Session(
                aws_access_key_id=s3_bucket_access_key,
                aws_secret_access_key=s3_bucket_secret_key,
            )

            s3_resource = session.resource('s3')

            if self._s3_bucket_location[-1] == '/':
                s3_bucket_location = self._s3_bucket_location[:-1]
            else:
                s3_bucket_location = self._s3_bucket_location

            s3_resource.meta.client.upload_file(self._complete_path, s3_bucket_location, self._extract_filename)

        except Exception as err:
            raise(f"Something wrong while uploading file to S3: {err}")

        self.args.logger.info(f"Data has been loaded to S3 bucket {self._s3_bucket_location}"
                              f"{self._extract_filename} sucessfully!")

    #@db.connect('redshift')

    def setup_staging_env(self):
        self.args.logger.info("Start to setup staging environment...")
        if not pgprocess.is_table_exist(db_type='redshift',
                                        table_name=self.args.arguments.stage_object,
                                        db_client_dbshell=self._db_client_dbshell,
                                        db_url=self._redshift_url,
                                        schema_name='public'):
            pgprocess.create_tbl_by_cp_final_tbl(db_client_dbshell=self._db_client_dbshell,
                                                 db_url=self._redshift_url, schema_name='public',
                                                 stage_tbl_name=self.args.arguments.stage_object,
                                                 final_tbl_name=self.args.arguments.target_object)

        self.args.logger.info("Completed in setting up staging environment!")

    @query.delete_tbl('redshift')
    #@check.check_test_table
    def copy2redshift(self, *, s3_bucket_location: str, tablename: str, opt, isgzip=False, db_instance=None, apply_mode=None) ->None:

        fn = f"s3://{s3_bucket_location}"
        quote = ''

        opt.red_quote = 0
        opt.red_ignoreheader = 0
        opt.red_timeformat = 0

        #prefix_table_name = tablename.split('_')[0]

        #if prefix_table_name != 'test':
        #    print(f"We are in testing mode, please include only testing table")
        #    exit(0)

        opt.red_to_table = tablename

        opt.red_col_delim = ','

        if opt.red_quote:
            quote = 'quote \'%s\'' % opt.red_quote
        ignoreheader = ''
        if opt.red_ignoreheader:
            ignoreheader = 'IGNOREHEADER %s' % opt.red_ignoreheader
        timeformat = ''
        if opt.red_timeformat:
            # timeformat=" dateformat 'auto' "
            timeformat = " TIMEFORMAT '%s'" % opt.red_timeformat.strip().strip("'")

        redshift_access_key = getattr(self.conf, 'SCOOT_REDSHIFT_ACCESS_KEY')
        redshift_secret_key = getattr(self.conf, 'SCOOT_REDSHIFT_SECRET_KEY')

        if isgzip:
            set_gzip = "GZIP"
        else:
            set_gzip = ""

        #CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'

        #column_list = get_column_name_by_order(source_tablename)
        #column_list.sort(key=lambda x: x[1])
        #column_list_str = "(" + ','.join([item[0] for item in column_list]) + ")"

        #arg_column_list_str = "(" + ','.join([item[0]  for item in self.args.column_list]) + ")"
        arg_column_list_str = "(" + ','.join(["\"" + item[0] + "\"" for item in self.args.column_list]) + ")"
        #print(arg_column_list_str)


        #print(column_list_str)
        #self.args.logger.info(arg_column_list_str)

        sql = """
        COPY %s %s FROM '%s' 
        	iam_role 'arn:aws:iam::085201521026:role/RedShiftS3'
        	DELIMITER '%s' 
        	FORMAT CSV %s 
        	%s
        	%s 
        	%s COMPUPDATE OFF STATUPDATE OFF; 
        	COMMIT;
        """ % (opt.red_to_table, arg_column_list_str, fn, opt.red_col_delim, set_gzip, quote, timeformat, ignoreheader)

        self.args.logger.debug(sql)

        try:
            #db_client_dbshell = f"{self._db_client_home.strip()}bin/psql"
            db_client_dbshell = self._db_client_dbshell.strip()
            loadConf = [db_client_dbshell, self._redshift_url, "-c", sql]
            self.args.logger.debug(loadConf)

            p2 = Popen(loadConf)
            p2.wait()

        except Exception as err:
            self.args.logger.critical(f"Error in connecting Redshift {err}")
            raise ("Error in loadding data to Redshift...")
        else:
            self.args.logger.info(f"Data has been loaded to {opt.red_to_table} successfully!")

    def apply_change(self):

        #delete from target using stage where target.pk = stage.pk
        #insert into target select * from stage;

        arg_column_insert_str = "(" + ','.join(["\"" + item[0] + "\"" for item in self.args.column_list]) + ")"
        arg_column_select_str = ','.join(["\"" + item[0] + "\"" for item in self.args.column_list])

        apply_query = """ begin transaction; insert into %s %s select %s from %s; end transaction;
                COMMIT;
                """ % (self.args.arguments.target_object,
                       arg_column_insert_str,
                       arg_column_select_str,
                       self.args.arguments.stage_object)

        self.args.logger.debug(apply_query)

        try:
            #db_client_dbshell = f"{self._db_client_home.strip()}bin/psql"
            db_client_dbshell = self._db_client_dbshell.strip()
            loadConf = [db_client_dbshell, self._redshift_url, "-c", apply_query]
            self.args.logger.debug(loadConf)

            p2 = Popen(loadConf)
            p2.wait()
            if p2.returncode != 0:
                return False

        except Exception as err:
            self.args.logger.critical(f"Error in apply change query on Redshift {err}")
            raise ("Error in apply change query on Redshift...")
        else:
            self.args.logger.info(f"Changes have been applied to {self.args.arguments.target_object} successfully!")
            return True


    def end_end2end_count_validation_v2(self):

        try:
            # print(args)
            validation_query = self.query_builder(table_name=self.args.arguments.stage_object, valid_flag=True)

            result = pgprocess.run_query(db_client_dbshell=self._db_client_dbshell, db_url=self._redshift_url,
                                         query=validation_query)
            #print(type(result))
            #print(result)

        except Exception as e:
            print(e)
            return False, 0

        if result:
            return True, result[0]
        else:
            return False, 0

    def check_stage_table(self):
        return pgprocess.is_table_exist(db_type='redshift', table_name=self.args.arguments.stage_object,
                                        db_client_dbshell=self._db_client_dbshell, db_url=self._redshift_url,
                                        schema_name='public')


if __name__ == '__main__':

    args = SimpleNamespace(logger=None)
    args.arguments = SimpleNamespace(source_object='ride_updates', source_system='rds',
                                     stage_object='stg_ride_updates_100', target_object='test_ride_updates',
                                     target_system='redshift', timestamp_col='id', key_col='id')

    args.system_parameter = SimpleNamespace(datafile_loc='', highwatermark=200000, lowwatermark=1)

    conn = Postgres2Redshift_hist(args)
    conn.setup_postgresql()
    conn.setup_redshift()

    is_failed, start_validation_cnt = conn.start_end2end_count_validation_v2()
    #print(conn.check_stage_table())

    if not is_failed:
        print("Start validation step failed, return back to parent function")

    conn.extract()

    conn.upload_s3()

    args1 = SimpleNamespace(source_system='rds',
                            source_object='ride_updates',
                            target_system='redshift',
                            target_object='test_ride_updates')

    conn.copy2redshift(s3_bucket_location=conn.s3_bucket_location + conn.extract_filename,
                       tablename=args.arguments.stage_object,
                       opt=args1)

    conn.apply_change()
    is_failed, end_validation_cnt = conn.end_end2end_count_validation_v2()
    print(start_validation_cnt, end_validation_cnt)
