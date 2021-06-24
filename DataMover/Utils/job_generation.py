from Data.Utils import db
from DataValidator.Model import Table_MetaData
from types import SimpleNamespace
from Data.Utils import pgparse
import logging
import argparse
import sys
from sqlalchemy import and_
from Daemon.Model import Job
from Data.Utils import pgprocess

_version_ = 0.1


def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='Daemon Job generation script')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-t', '--table', action='store', dest='table_name', required=True, help='Target table to be loaded')
        argparser.add_argument('-b', '--batch_size', action='store', dest='batch_size', required=True, help='The batch size, to be used for partitioning')
        argparser.add_argument('-s', '--table_size', action='store', choices=['normal', 'large'],
                               dest='table_size', required=True, help='The table size, to determine the most optimized path')
        argparser.add_argument('-y', '--job_type', action='store', choices=['normal', 'history'],
                               required=True, dest='job_type', help='Type of the jobs')
        argparser.add_argument('-z', '--target_schema', action='store', choices=['rds_na', 'rds_eu', 'rds_sa', 'dev'],
                               required=True, dest='target_schema', help='Target schema')
        argparser.add_argument('-e', '--env', action='store', dest='environment', required=True, help='Environment to be deployed (Dev or QA)')
        logging.debug('parsing argparser arguments')
        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    # print(args.daemon_type)
    return args


class JobGeneration:

    def __init__(self):
        self._table_name = None
        self._table_name_list = []
        self._column_list = {}
        self.key_col = None
        self.cdc_column = ''
        self.table_count = 0
        self._tbl_creation = []
        self.stg_tbl_creation = []
        self._post_red_column_type_mapping = {'varchar': 'varchar',
                                              'integer': 'bigint',
                                              'timestamp without time zone': 'timestamp',
                                              'jsonb': 'varchar(max)',
                                              'text': 'varchar(4000)',
                                              'boolean': 'boolean',
                                              'ARRAY': 'varchar',
                                              'date': 'timestamp',
                                              'bigint': 'bigint',
                                              'character varying': 'varchar(4000)',
                                              'numeric': 'numeric(38, 6)',
                                              'polygon': 'varchar(max)', #incorrect mapping?
                                              'numrange': 'varchar(4000)',
                                              'double precision': 'double precision'}

        self._job_name = None
        self._job_argument = None
        self._system_parameter = None
        self._found = None
        self._package_mapping = {'history': 'history_load',
                                 'normal': 'data_mover'}

        self._package_name = None
        self._min_key_col = 0
        self._max_key_col = 0
        self._counter = 0
        self.cdc_type = 'primary_key'
        self.cdc_is_pk_tbl = {'scooter_versions': 'id',
                              'ride_updates': 'id',
                              'events': 'id',
                              'locations_regions': 'location_id',
                              'scoot_log_messages': 'id',
                              'promo_type_lookup': 'id'}

        self._large_table = ['scooter_versions', 'ride_updates', 'events', 'scoot_log_messages']


    @classmethod
    def set_args(cls, args):
        cls._job_type = args.job_type
        cls._batch_size = args.batch_size
        cls._table_size = args.table_size
        cls._target_schema = args.target_schema
        cls._table_name = args.table_name
        cls._stage_table = f"stg_{cls._table_name}"

        if cls._target_schema == 'rds_na':
            cls._stage_table = f"stage_{cls._table_name}"
        elif cls._target_schema == 'rds_sa':
            cls._stage_table = f"stagesa_{cls._table_name}"
        elif cls._target_schema == 'rds_eu':
            cls._stage_table = f"stageeu_{cls._table_name}"

        print(f"stage table is {cls._stage_table}\n")
        print(f"target table is {cls._target_schema}.{cls._table_name}\n")

    def gen_normal_job_def(self, mode='dev'):

        if "id" in self._column_list:
            self.key_col = "id"
        else:
            self.key_col = "default_key"

        if "created_at" in self._column_list:
            self.cdc_column = "created_at"
            self.cdc_type = 'timestamp'

        if "updated_at" in self._column_list:
            if self.cdc_column:
                self.cdc_column = self.cdc_column + ", updated_at"
            else:
                self.cdc_column = "updated_at"

            self.cdc_type = 'timestamp'

        default_datafile_loc = SimpleNamespace(datafile_loc="")
        loc = pgparse.ns_to_json(default_datafile_loc)

        if self._table_name in self.cdc_is_pk_tbl:
            self.cdc_column = self.cdc_is_pk_tbl[self._table_name]
            self.cdc_type = 'primary_key'



        etl_mode = 'incr'
        if not self.cdc_column:
            etl_mode = 'full'

        if mode.lower() == 'dev':
            print(mode.lower())
            self._job_name = f"data_replication_test_{self._table_name}"
            self._job_argument = f"source_system:rds;source_object:{self._table_name};target_system:redshift;" \
                                 f"stage_object:{self._stage_table};target_object:test_{self._table_name};" \
                                 f"highwatermark:;timestamp_col:{self.cdc_column};key_col:{self.key_col};cdc_type:{self.cdc_type};" \
                                 f"etl_mode:{etl_mode};table_size:{self._table_size};optimize_level:0;blacklist_cols:"

            self._system_parameter = f"{str(loc)}"

        else:

            self._job_name = f"data_replication_{self._table_name}"
            self._job_argument = f"source_system:rds;source_object:{self._table_name};target_system:redshift;" \
                                 f"stage_object:{self._stage_table};target_object:{self._target_schema}.{self._table_name};" \
                                 f"highwatermark:;timestamp_col:{self.cdc_column};key_col:{self.key_col};cdc_type:{self.cdc_type};" \
                                 f"etl_mode:{etl_mode};table_size:{self._table_size};optimize_level:0;exact_match:0;blacklist_cols:"

            self._system_parameter = f"{str(loc)}"

        if mode.lower() == 'dev':
            count = 0
            crt_string = f"create table test_{self._table_name} ("
            for index, val in self._column_list.items():
                crt_string += f"\"{index}\" {self._post_red_column_type_mapping[val.data_type]}"
                count += 1
                if count < len(self._column_list):
                    crt_string += " ,"

            crt_string += f" );"

            self._tbl_creation.append(crt_string)
            self._tbl_creation.append(f"create table {self._stage_table} as select * from test_{self._table_name} limit 1;")

        else:
            count = 0
            crt_string = f"create table {self._target_schema}.{self._table_name} ("
            for index, val in self._column_list.items():
                crt_string += f"\"{index}\" {self._post_red_column_type_mapping[val.data_type]}"
                count += 1
                if count < len(self._column_list):
                    crt_string += " ,"

            crt_string += f" )"

            self._tbl_creation.append(crt_string)
            self._tbl_creation.append(f"create table {self._stage_table} as select * from {self._target_schema}.{self._table_name} limit 1;")

        print(crt_string)

    @db.connect('meta')
    def gen_hist_job_def(self, mode='dev', db_instance=None):

        self._counter = int(self._min_key_col)
        etl_mode = 'hist'
        seq = "metaschema.scd_job_id_seq"

        while self._counter < self._max_key_col:
            #print(self._counter, self._max_key_col)

            if int(self._counter) + int(self._batch_size) > self._max_key_col:
                highwatermark_num = self._max_key_col
            else:
                highwatermark_num = int(self._counter) + int(self._batch_size)

            default_datafile_loc = SimpleNamespace(datafile_loc="",
                                                   lowwatermark=str(self._counter),
                                                   highwatermark=str(highwatermark_num))

            loc = pgparse.ns_to_json(default_datafile_loc)
            print(default_datafile_loc.lowwatermark, default_datafile_loc.highwatermark)

            if mode.lower() == 'dev':
                #print(mode.lower())
                self._job_name = f"data_replication_hist_test_{self._table_name}"
                self._job_argument = f"source_system:rds;source_object:{self._table_name};target_system:redshift;" \
                                     f"stage_object:{self._stage_table};target_object:test_{self._table_name};" \
                                     f"timestamp_col:{self.key_col};key_col:{self.key_col};etl_mode:{etl_mode};" \
                                     f"optimize_level:0;cdc_type:id;table_size:large"

                self._system_parameter = f"{str(loc)}"

            else:

                self._job_name = f"data_replication_hist_{self._table_name}"
                self._job_argument = f"source_system:rds;source_object:{self._table_name};target_system:redshift;" \
                                     f"stage_object:{self._stage_table};target_object:{self._target_schema}.{self._table_name};" \
                                     f"timestamp_col:{self.key_col};key_col:{self.key_col};etl_mode:{etl_mode};" \
                                     f"optimize_level:0;cdc_type:id;table_size:large"

                self._system_parameter = f"{str(loc)}"

            try:

                self._package_name = self._package_mapping[self._job_type]
                #print(self._package_name)
                self._job_id = int(db_instance.session.execute(f"select nextval('{seq}')").fetchall()[0][0])
                db_instance.session.add(Job(str(self._job_id), self._job_name, '100', '20', '127.0.0.1', '0', 'Package',
                                            self._package_name, self._job_argument, self._system_parameter))
                db_instance.session.commit()

            except Exception as e:
                raise e

            self._counter += int(self._batch_size) + 1

    @db.connect('meta')
    def execute_cmd(self, db_instance=None):

            if self._table_name:
                if_table_exist_query = db_instance.session.query(Job).filter(Job.job_name == self._job_name)
                for _row in if_table_exist_query.all():
                    if self._job_name == _row.job_name:
                        self._found = True
                        print(f"{self._job_name} is already existed.  Ignoring adding this job to the metadata DB")
                        break
                    else:
                        self._found = False

                if not self._found:
                    self.execute_crt_cmd()
                    seq = "metaschema.scd_job_id_seq"
                    self._job_id = int(db_instance.session.execute(f"select nextval('{seq}')").fetchall()[0][0])

                    #job_id, job_name, schedule_id, priority_value, server_node, hold_flag, job_type, job_command, job_argument, system_parameter

                    if not (self._job_id and self._job_name and self._job_argument and self._system_parameter):
                        raise ("Missing Parameters!!!!!")

                    try:

                        self._package_name = self._package_mapping[self._job_type]
                        print(self._package_name)
                        db_instance.session.add(Job(str(self._job_id), self._job_name, '0', '20', '127.0.0.1', '0', 'Package',
                                                    self._package_name, self._job_argument, self._system_parameter))
                        db_instance.session.commit()

                    except Exception as e:
                        raise e

                    for item in self._tbl_creation:
                        print(item)

    @db.connect('rds')
    def get_min_max_key_col(self, db_instance=None):

        if "id" in self._column_list:
            self.key_col = "id"
        elif "created_at" in self._column_list:
            self.key_col = "created_at"
        else:
            self.key_col = "default_key"

        query = f"select min({self.key_col}), max({self.key_col}) from {self._table_name}"
        min_val, max_val = db_instance.session.execute(query).fetchall()[0]
        self._min_key_col = int(min_val)
        self._max_key_col = int(max_val)

        return min_val, max_val




    @db.connect('meta')
    def execute_bulk_cmd(self, db_instance=None):


        seq = "metaschema.scd_job_id_seq"
        self._job_id = int(db_instance.session.execute(f"select nextval('{seq}')").fetchall()[0][0])

        # job_id, job_name, schedule_id, priority_value, server_node, hold_flag, job_type, job_command, job_argument, system_parameter

        if not (self._job_id and self._job_name and self._job_argument and self._system_parameter):
            raise ("Missing Parameters!!!!!")

        try:

            self._package_name = self._package_mapping[self._job_type]
            print(self._package_name)
            db_instance.session.add(
                        Job(str(self._job_id), self._job_name, '0', '20', '127.0.0.1', '0', 'Package',
                            self._package_name, self._job_argument, self._system_parameter))
            db_instance.session.commit()

        except Exception as e:
            raise e

        for item in self._tbl_creation:
            print(item)


    @db.connect('redshift')
    def execute_crt_cmd(self, db_instance=None):
        try:
            for item in self._tbl_creation:
                db_instance.session.execute(item)
                db_instance.session.commit()
        except:
            raise ("Something wrong with table creation in target system")

    @db.connect('rds')
    def get_table_list(self, db_instance=None):
        for item in db_instance.get_table_list().fetchall():
            self._table_name_list.append(item[0])

        #for item in self._table_name_list:
            #print(item)

    @db.connect('rds')
    def get_column_info(self, table_name, db_instance=None):
        self._table_name = table_name
        if table_name not in self._table_name_list:
            raise ("This table is not in RDS!")

        for item in db_instance.get_db_metadata(table_name):
            #table_name, column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
            self._column_list[item[1]] = SimpleNamespace(data_type=item[2],
                                                         character_maximum_length=item[3],
                                                         numeric_precision=item[4],
                                                         numeric_scale=item[5])

        #for index, val in self._column_list.items():
        #    print(index, val)


if __name__ == '__main__':
    args = get_parser()
    jb = JobGeneration()
    jb.set_args(args)
    jb.get_table_list()
    jb.get_column_info(args.table_name)
    if args.job_type == 'history':
        print(jb.get_min_max_key_col())
        jb.gen_hist_job_def(args.environment)
    elif args.job_type == 'normal':
        jb.gen_normal_job_def(args.environment)
        jb.execute_cmd()


    #job_generation().job_generation(args.table_name, args.environment)




