# !/usr/bin/env python3

from subprocess import Popen, PIPE, call, STDOUT
from Data.Utils.db import validate_table_exist
from Data.Utils import db
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
from DataValidator.Core import validate
from Data.Utils import pgfile
from Data.Utils import pgprocess as pr
from time import sleep
from DataMover.Mode import fullmode

loggingLevel = logging.INFO
logging.basicConfig(level=loggingLevel)

q = Queue()


class Postgres2Redshift:

    def __init__(self, args=None):

        self.conf = pgconfig.Config()
        if args:
            self.args = args
            self.args.parameters = self.conf.parameters
            self.args.name = 'Data_Mover'

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
        self._db_client_dbshell = getattr(self.conf, 'SCOOT_CLIENT_DBSHELL')
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
        self._target_lookup = None
        self.hwm_target = None

    @property
    def complete_path(self):
        return self._complete_path

    @property
    def s3_bucket_location(self):
        return self._s3_bucket_location

    @property
    def extract_filename(self):
        return self._extract_filename

    def setup_postgresql(self):

        self._postgresql_host = getattr(self.conf, 'SCOOT_RDS_POST_HOST')
        self._postgresql_username = getattr(self.conf, 'SCOOT_RDS_POST_USERNAME')
        self._postgresql_password = getattr(self.conf, 'SCOOT_RDS_POST_PASS')
        self._post_port = getattr(self.conf, 'SCOOT_RDS_POST_PORT')
        self._postgresql_database = getattr(self.conf, 'SCOOT_RDS_POST_DB')
        self._postgresql_url = f"postgresql://{self._postgresql_username}:" \
                               f"{self._postgresql_password}@{self._postgresql_host}/{self._postgresql_database}"
        self._target_lookup = {'source': self._postgresql_url}
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
        self._target_lookup = {'stage': self._redshift_url,
                               'target': self._redshift_url}
        self.args.logger.info("Sucessfully initiated db connection to Data Warehouse...")


    """
    Key needs to be an integer
    """

    def extract_trunk_by_key(self, tablename, env, key, pgres_query_file=None):
        time.sleep(random.randint(1, 3))
        q.put(os.getpid())
        sess = postgresql.ConnectPostgresql(self._postgresql_host,
                                            self._post_port,
                                            self._postgresql_username,
                                            self._postgresql_password,
                                            self._postgresql_database)
        result = sess.execute(f"select max({key}), min({key}) from {tablename}")
        for item in result.fetchall():
            print(item)

    def multi_processing(self, func, num_process):

        processes = []
        for i in range(num_process):
            t = multiprocessing.Process(target=func, args=(i,))
            processes.append(t)
            t.start()

        for one_process in processes:
            one_process.join()

        mylist = []
        while not q.empty():
            mylist.append(q.get())

    """
    def start_end2end_count_validation(self):
        return validate.start_end2end_count_validation_v2(args=self.args,
                                                          db_client_dbshell=self._db_client_dbshell,
                                                          db_url=self._postgresql_url)

    def end_end2end_count_validation(self):
        return validate.end_end2end_count_validation_v2(args=self.args,
                                                        db_client_dbshell=self._db_client_dbshell,
                                                        db_url=self._redshift_url)
    """

    def end2end_count_validation(self, target_system):

        db_url = self._target_lookup[target_system]
        return validate.end2end_count_validation_v2(args=self.args,
                                                    db_client_dbshell=self._db_client_dbshell,
                                                    db_url=db_url,
                                                    target=target_system)

    def extract(self, *, optimize_level=0, col_blacklist=None, pgres_query_file=None):
        print(f"The black list columns are: {col_blacklist}\n")

        column_list_orig = db.get_column_name_by_order(self.args.arguments.source_object)
        column_list = self.exclude_columns(column_list=column_list_orig, black_list_col=col_blacklist)
        column_list.sort(key=lambda x: x[1])
        self.args.column_list = column_list

        #print(self.args)
        if self.args.arguments.cdc_type == 'primary_key':
            extract_query = qy.QueryFactory(self.args.arguments).qb_cdc_extraction_pk(column_list=self.args.column_list)
        else:
            extract_query = qy.QueryFactory(self.args.arguments).qb_cdc_extraction_ts(column_list=self.args.column_list)


        #extract_query = qy.query_builder(self.args.arguments, False)
        #extract_query = qy.query_builder_v3(self.args.arguments)



        #column_list_str = "(" + ','.join([item[0] for item in column_list]) + ")"

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

            save_directory = self.conf.parameters.get('SCOOT_DATA_HOME', '') + "/data/" + \
                             self.application_name + '/' + self.args.arguments.source_object

            dirFunc.createdirectory(save_directory)

            file_name_save = "save" + get_random_filename(self.args.arguments.source_object) + ".csv"
            complete_path = save_directory + "/" + file_name_save

            with open(complete_path, 'w') as f:
                p2 = Popen(loadConf, stdout=f)
                p2.wait()

            if p2.returncode != 0:
                return False, False

            self._extract_filename = file_name_save
            self._complete_path = complete_path

            self.args.logger.info(f"Data Extraction is successfully completed and data is saved at {file_name_save}.")

            #print(f"Is the file exists {file.isfileexist(self.args.system_parameter.datafile_loc)}")

            if optimize_level == 1 and self.args.system_parameter.datafile_loc and \
                    pgfile.isfileexist(self.args.system_parameter.datafile_loc):
                #print(f"Here!!!!!  Optimize level is 1 and datafile_loc is {self.args.system_parameter.datafile_loc}")
                if not validate.csv_to_csv_diff(self.args.system_parameter.datafile_loc, complete_path):
                    return True, False

        except Exception as err:
            self.args.logger.info(f"Data Extraction process failed: {err}")

        return True, True

    def extract_streaming(self, tablename, env, pgres_query_file=None):

        #db_client_dbshell = f"{self._db_client_home.strip()}bin/psql"
        db_client_dbshell = self._db_client_dbshell.strip()
        #print(db_client_dbshell)

        header_str = ''
        # if opt.ora_add_header:
        #	header_str=' CSV HEADER'

        limit = ''
        # if opt.pgres_lame_duck > 0:
        #    limit = 'LIMIT %d' % opt.pgres_lame_duck

        quote = ''
        # if opt.pgres_lame_duck > 0:
        #    quote = 'QUOTE  \'%s\'' % opt.pgres_quote

        # query = f"COPY (({tablename}) {limit}) TO STDOUT WITH DELIMITER ',' CSV {quote}"

        query = f"COPY {tablename} {limit} TO stdout DELIMITER ',' CSV {quote}"

        print(query)
        loadConf = [db_client_dbshell, self._postgresql_url, "-c", query]
        #print(loadConf)


        p1 = Popen(loadConf, stdout=PIPE, stderr=PIPE)

        #f = open("my_table111.csv.gz", "w")
        # p1 = call(loadConf, stdout=f)    works


        self._extract_filename = get_random_filename(tablename) + ".csv.gz"
        compressor = gzip.GzipFile(self._extract_filename, mode='w')
        i = total_size = 0

        while True:  # until EOF
            i += 1
            start_time = datetime.datetime.now()
            #print(start_time)
            #chunk = p1.read(opt.s3_write_chunk_size)
            chunk = p1.stdout.read(8192)
            if not chunk:  # EOF?
                compressor.close()
                #uploadPart()
                #mpu.complete_upload()

                break
            compressor.write(chunk)
            total_size += len(chunk)

            # print(compressor.tell())
            # print(len(chunk),opt.s3_write_chunk_size)
        print(f"{len(chunk)} chunk {i} [{(datetime.datetime.now() - start_time)} sec]")

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

    @query.truncate_tbl('redshift')
    #@check.check_test_table
    def copy2redshift(self, s3_bucket_location: str, tablename: str, opt, isgzip=False, db_instance=None, apply_mode=None) ->None:

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

        #column_list = db.get_column_name_by_order(source_tablename)
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
            #reader = p2.stdout
            #p2.stderr

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

        if self.args.arguments.etl_mode == 'full':
            apply_query = """ begin transaction;  delete from %s; insert into %s %s select %s from %s; end transaction;
                COMMIT;
                """ % (self.args.arguments.target_object,
                       self.args.arguments.target_object,
                       arg_column_insert_str,
                       arg_column_select_str,
                       self.args.arguments.stage_object)
            #print(apply_query)
        else:
        #apply_query = """ begin transaction;  delete from %s using %s where %s.%s = %s.%s; insert into %s select * from %s; end transaction;
            apply_query = """ begin transaction;  delete from %s using %s where %s.%s = %s.%s; insert into %s %s select %s from %s; end transaction;
                COMMIT;
                """ % (self.args.arguments.target_object,
                       self.args.arguments.stage_object,
                       self.args.arguments.target_object,
                       self.args.arguments.key_col,
                       self.args.arguments.stage_object,
                       self.args.arguments.key_col,
                       self.args.arguments.target_object,
                       arg_column_insert_str,
                       arg_column_select_str,
                       self.args.arguments.stage_object)

        self.args.logger.info(apply_query)

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

    def get_hwm_id(self, target):

        if target == "source":
            self.hwm_target = str(self.args.arguments.source_object)
        elif target == "stage":
            self.hwm_target = str(self.args.arguments.stage_object)
        else:
            self.hwm_target = str(self.args.arguments.target_object)

        get_query = f"select max({self.args.arguments.timestamp_col}) from {self.hwm_target}"
        print(get_query)

        result_list = pr.run_query(db_client_dbshell=self._db_client_dbshell, db_url=self._redshift_url, query=get_query)
        return result_list[0]

    @staticmethod
    def exclude_columns(*, column_list: list, black_list_col: list) -> list:
        if not black_list_col:
            return column_list

        for index, item in enumerate(column_list):
            if item[0] in black_list_col:
                del column_list[index]

        return column_list



    @staticmethod
    def cdc_count_comparison(*, start_cnt=0, end_cnt=0, exact_match=True):
        if exact_match:
            return int(start_cnt) == int(end_cnt)
        else:
            return int(start_cnt) <= int(end_cnt)

    @staticmethod
    def cdc_time_comparison(*, max_cdc_col1=datetime.datetime.max, max_cdc_col2=datetime.datetime.max):
        return min(max_cdc_col1, max_cdc_col2)

    @staticmethod
    def cdc_time_comparison(*, max_cdc_col1=datetime.datetime.max, max_cdc_col2=datetime.datetime.max):
        return min(max_cdc_col1, max_cdc_col2)


if __name__ == '__main__':

    args1 = SimpleNamespace(highwatermark='2019-02-25 19:56:10.085183',
                            timestamp_col=['created_at', 'updated_at'],
                            tablename='batteries')

    args2 = SimpleNamespace(highwatermark=None,
                            timestamp_col=['created_at', 'updated_at'],
                            tablename='batteries')

    args = SimpleNamespace(arguments=None)
    args.arguments = SimpleNamespace(highwatermark='3019',
                                     source_object='batteries',
                                     source_system='rds',
                                     stage_object='stg_batteries',
                                     target_object='test_batteries',
                                     target_system='redshift',
                                     timestamp_col='id',
                                     cdc_type='primary_key',
                                     key_col='id',
                                     table_size='large',
                                     etl_mode='incr')

    args.system_parameter = SimpleNamespace(datafile_loc='')
    args.logger = None

    conn = Postgres2Redshift(args)
    conn.setup_postgresql()

    time_cdc1 = datetime.datetime.now()
    print(f"time_cdc1: {time_cdc1}")
    time.sleep(5)
    time_cdc2 = datetime.datetime.now()
    print(f"time_cdc2: {time_cdc2}")
    time_cdc3 = None

    print(conn.cdc_time_comparison(max_cdc_col1=time_cdc1, max_cdc_col2=time_cdc2))
    print(conn.cdc_time_comparison(max_cdc_col2=time_cdc2))

    exit(0)

    conn.extract(args1.tablename, args1)

    conn.upload_s3()

    conn.setup_redshift()

    args=SimpleNamespace(source_system='rds',
                         source_object='batteries',
                         target_system='redshift',
                         target_object='test_batteries')

    conn.copy2redshift(conn.s3_bucket_location + conn.extract_filename, args.source_object, args.target_object, args)
