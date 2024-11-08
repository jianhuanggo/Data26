from types import SimpleNamespace
from subprocess import Popen, PIPE, call, STDOUT
from Data.Config import pgconfig
from functools import wraps
from Data.Utils import check


def query_builder(arguments, validation_flg=True) ->str:
    print(arguments.timestamp_col)

    """
    if not arguments.timestamp_col and validation_flg:
        print(f"select count(1) from {arguments.source_object}")
        return f"select count(1) from {arguments.source_object}"
    elif not arguments.timestamp_col and not validation_flg:
        if arguments.etl_mode == 'incr':
            print(f"There is no CDC column provided, can not perform incremental load, defaults to full extraction...")
        print(f"select * from {arguments.source_object}")
        return f"select * from {arguments.source_object}"
        
    """

    try:
        #print(bool(arguments.timestamp_col))
        #print(arguments.timestamp_col is None)

        if arguments.etl_mode == 'full':  # If it's a full mode, then we switch to full mode even we have cdc
                                          # columns to do incremental load.  Overwrite cdc column to None
            cdc_col_list = None

        elif not arguments.timestamp_col or not arguments.highwatermark:   # We can't perform incremental load w/o cdc columns or without highwatermark
            cdc_col_list = None
        else:
            cdc_col_list = arguments.timestamp_col.split(',')
            #print(f"cdc column list is {cdc_col_list}\n")
            #print(f"length of list is {len(cdc_col_list)}\n")

    except Exception as err:
        raise (f"Could not extract column list {err}")

    if cdc_col_list:
        projection_val = ''
        where_clause = ''

        for num, item in enumerate(cdc_col_list):
            if num == len(cdc_col_list) - 1:
                projection_val += f"max({item}), min({item})"
                where_clause += f" {item} >= '{arguments.highwatermark}'"
            else:
                projection_val += f"max({item}), min({item}), "
                where_clause += f" {item} >= '{arguments.highwatermark}' or"

        if arguments.highwatermark:
            where_clause = f"where {where_clause}"
        else:
            where_clause = ''

        projection_val += ', count(1)'

        if validation_flg:
            #return f"select {projection_val} from {arguments.source_object} {where_clause}"
            print(f"select {projection_val} from {arguments.source_object}")
            return f"select {projection_val} from {arguments.source_object}"
        else:
            print(f"select * from {arguments.source_object} {where_clause}")
            return f"select * from {arguments.source_object} {where_clause}"

        #max_min_result = db_instance.session.execute(f"select max({args.key}), min({args.key}) from {args.tablename} "
        #item = query_result.fetchall()
        #min_val = item[0][1]
        #max_val = item[0][0]
    else:
        if validation_flg:
            print(f"select count(1) from {arguments.source_object}")
            return f"select count(1) from {arguments.source_object}"
        else:
            if arguments.etl_mode == 'incr':
                print(f"There is no CDC column provided or highwatermark is not populdated, can not perform incremental load, defaults to full extraction...")
            print(f"select * from {arguments.source_object}")
            return f"select * from {arguments.source_object}"


def query_builder_v2(arguments, mode="source") ->str:
    #print(arguments.key_col, arguments.timestamp_col, arguments.source_object)

    #if hasattr(arguments, "timestamp_col") and arguments.timestamp_col:
        #cdc_col_list = arguments.timestamp_col.split(',')


    if not arguments.source_object or not arguments.target_object:
        raise ("Source object or target object is not presented in the command argument")

    if mode == 'source':
        print(f"argument key column is {arguments.key_col} and argument source object is {arguments.source_object}")
        if not arguments.timestamp_col:
            return f"select 'not available', 'not available', count(1) from {arguments.source_object}"
        else:
            return f"select min({arguments.key_col}), max({arguments.key_col}), count(1) from {arguments.source_object}"
    else:
        print(f"argument key column is {arguments.key_col} and argument source object is {arguments.target_object}")
        if not arguments.timestamp_col:
            return f"select 'not available', 'not available', count(1) from {arguments.target_object}"
        else:
            return f"select min({arguments.key_col}), max({arguments.key_col}), count(1) from {arguments.target_object}"


def query_builder_v3(arguments, mode="source") -> str:
    # print(arguments.key_col, arguments.timestamp_col, arguments.source_object)

    # if hasattr(arguments, "timestamp_col") and arguments.timestamp_col:
    # cdc_col_list = arguments.timestamp_col.split(',')

    if not arguments.source_object or not arguments.target_object:
        raise ("Source object or target object is not presented in the command argument")

    if mode == 'source':
        #print(f"argument key column is {arguments.key_col} "
        #      f"argument cdc columns are {arguments.timestamp_col}"
        #      f"and argument source object is {arguments.source_object} "
        #      f"and etl_mode is {arguments.etl_mode}")

        if arguments.etl_mode == 'full':
            #print(f"select 'not available', 'not available', count(1) from {arguments.source_object}")
            return f"select 'not available', 'not available', count(1) from {arguments.source_object}"
        elif arguments.etl_mode == 'incr':
            if hasattr(arguments, "timestamp_col") and arguments.timestamp_col:
                cdc_col_list = arguments.timestamp_col.split(',')
                if len(cdc_col_list) < 2:
                    #print(f"select max({cdc_col_list[0]}),'not available', count(1) from {arguments.source_object}")
                    return f"select max({cdc_col_list[0]}),'not available', count(1) from {arguments.source_object}"
                elif len(cdc_col_list) > 2:
                    #print(f"select 'not available', 'not available', count(1) from {arguments.source_object}")
                    return f"select 'not available', 'not available', count(1) from {arguments.source_object}"
                else:
                    print(f"select max({cdc_col_list[0]}), max({cdc_col_list[1]}), count(1) from {arguments.source_object}")
                    return f"select max({cdc_col_list[0]}), max({cdc_col_list[1]}), count(1) from {arguments.source_object}"
            else:
                print(f"WARNING!!! 'incremental' etl mode is selected but cdc column is not provideds")
                #print(f"select 'not available', 'not available', count(1) from {arguments.source_object}")
                return f"select 'not available', 'not available', count(1) from {arguments.source_object}"
                #raise ("CRITICAL!!!!  Incremental loads needs CDC columns but there is none provided!!!")
                #print(f"select 'not available', 'not available', count(1) from {arguments.source_object}")
                #return f"select 'not available', 'not available', count(1) from {arguments.source_object}"
    else:

        #print(f"argument key column is {arguments.key_col} "
        #      f"and argument source object is {arguments.target_object} "
        #      f"and etl_mode is {arguments.etl_mode}")

        if arguments.etl_mode == 'full':
            #print(f"select 'not available', 'not available', count(1) from {arguments.target_object}")
            return f"select 'not available', 'not available', count(1) from {arguments.target_object}"
        elif arguments.etl_mode == 'incr':
            if hasattr(arguments, "timestamp_col") and arguments.timestamp_col:
                cdc_col_list = arguments.timestamp_col.split(',')
                if len(cdc_col_list) < 2:
                    #print(f"select max({cdc_col_list[0]}),'not available', count(1) from {arguments.target_object}")
                    return f"select max({cdc_col_list[0]}),'not available', count(1) from {arguments.target_object}"
                elif len(cdc_col_list) > 2:
                    #print(f"select 'not available', 'not available', count(1) from {arguments.target_object}")
                    return f"select 'not available', 'not available', count(1) from {arguments.target_object}"
                else:
                    #print(f"select max({cdc_col_list[0]}),max({cdc_col_list[1]}), count(1) from {arguments.target_object}")
                    return f"select max({cdc_col_list[0]}),max({cdc_col_list[1]}), count(1) from {arguments.target_object}"
            else:
                #print(f"select 'not available', 'not available', count(1) from {arguments.target_object}")
                return f"select 'not available', 'not available', count(1) from {arguments.target_object}"


class QueryFactory:

    def __init__(self, arguments):
        self.arguments = arguments
        self.validation_query_projection = None
        self.validation_query_from = None
        self.validation_query_condition = None

        self.cdc_query_projection = None
        self.cdc_query_from = None
        self.cdc_where_clause = None
        self.cdc_query_condition = None

    def qb_count_validation_ts(self, table_size="normal", target="source") -> str:

        if not self.arguments.source_object or not self.arguments.target_object:
            raise ("Source object or target object is not presented in the command argument")

        if self.arguments.etl_mode == 'incr':
            if hasattr(self.arguments, "timestamp_col") and self.arguments.timestamp_col:
                cdc_col_list = self.arguments.timestamp_col.split(',')
                if len(cdc_col_list) == 1:
                    self.validation_query_projection = f"max({cdc_col_list[0]}), 'NA', count(1)"
                    self.validation_query_condition = f" where {cdc_col_list[0]} >= {self.arguments.highwatermark}"
                else:
                    self.validation_query_projection = f"max({cdc_col_list[0]}), max({cdc_col_list[1]}), count(1)"
                    self.validation_query_condition = f" where {cdc_col_list[0]} >= {self.arguments.highwatermark} or " \
                                                      f"{cdc_col_list[1]} >= {self.arguments.highwatermark}"
                if not self.arguments.highwatermark:
                    self.validation_query_condition = ''
            else:
                print(f"WARNING!!! 'incremental' etl mode is selected but cdc column is not provided")
                self.validation_query_projection = f"'NA', 'NA', count(1)"
                self.validation_query_condition = ''
        else:
            self.validation_query_projection = f"'NA', 'NA', count(1)"
            self.validation_query_condition = ''

        if target == "source":
            self.validation_query_from = str(self.arguments.source_object)
        elif target == "stage":
            self.validation_query_from = str(self.arguments.stage_object)
        else:
            self.validation_query_from = str(self.arguments.target_object)

        if table_size == "normal" or table_size == '':
            self.validation_query_condition = ''

        print(f"select {self.validation_query_projection} from {self.validation_query_from} {self.validation_query_condition}")
        return f"select {self.validation_query_projection} from {self.validation_query_from} {self.validation_query_condition}"

    def qb_count_validation_pk(self, table_size="normal", target="source") -> str:

        if not self.arguments.source_object or not self.arguments.target_object:
            raise ("Source object or target object is not presented in the command argument")

        if self.arguments.etl_mode == 'incr':
            if hasattr(self.arguments, "timestamp_col") and self.arguments.timestamp_col:
                print(self.arguments.timestamp_col)
                if ',' in self.arguments.timestamp_col:
                    raise ("For primary key based data extraction, its primary key should only have one column")
                else:
                    self.validation_query_projection = f"max({self.arguments.timestamp_col.strip()}), 'NA', count(1)"
                    if self.arguments.highwatermark:
                        self.validation_query_condition = f" where {self.arguments.timestamp_col.strip()} > '{self.arguments.highwatermark}'"
                    else:
                        self.validation_query_condition = ''

            else:
                print(f"WARNING!!! 'incremental' etl mode is selected but cdc column is not provided")
                self.validation_query_projection = f"'NA', 'NA', count(1)"
                self.validation_query_condition = ''
        else:
            self.validation_query_projection = f"'NA', 'NA', count(1)"
            self.validation_query_condition = ''

        if target == "source":
            self.validation_query_from = str(self.arguments.source_object)
        elif target == "stage":
            self.validation_query_from = str(self.arguments.stage_object)
        else:
            self.validation_query_from = str(self.arguments.target_object)

        if table_size == "normal" or table_size == '':
            self.validation_query_condition = ''

        print(f"select {self.validation_query_projection} from {self.validation_query_from} {self.validation_query_condition}")
        return f"select {self.validation_query_projection} from {self.validation_query_from} {self.validation_query_condition}"

    def qb_cdc_extraction_ts(self, *, column_list):

        print(self.arguments.timestamp_col)
        try:
            self.cdc_query_projection = ','.join(["\"" + item[0] + "\"" for item in column_list])

            if self.arguments.etl_mode == 'full':  # If it's a full mode, then we switch to full mode even we have cdc
                self.cdc_where_clause = ''
                self.cdc_query_condition = ''

            elif not self.arguments.timestamp_col or not self.arguments.highwatermark:  # We can't perform incremental load w/o cdc columns or without highwatermark
                self.cdc_where_clause = ''
                self.cdc_query_condition = ''
            else:
                cdc_col_list = self.arguments.timestamp_col.split(',')
                self.cdc_query_condition = ''

                for num, item in enumerate(cdc_col_list):
                    if num == len(cdc_col_list) - 1:
                        self.cdc_query_condition += f" {item} >= '{self.arguments.highwatermark}'"
                    else:
                        self.cdc_query_condition += f" {item} >= '{self.arguments.highwatermark}' or"

                if self.arguments.highwatermark:
                    self.cdc_where_clause = f" where "
                else:
                    self.cdc_where_clause = ''

        except Exception as err:
            raise (f"Could not extract column list {err}")

        print(f"select {self.cdc_query_projection} from {self.arguments.source_object}{self.cdc_where_clause}{self.cdc_query_condition}")
        return f"select {self.cdc_query_projection} from {self.arguments.source_object}{self.cdc_where_clause}{self.cdc_query_condition}"

    def qb_cdc_extraction_pk(self, *, column_list):

        print(self.arguments.timestamp_col)
        if ',' in self.arguments.timestamp_col:
            raise ("For primary key based data extraction, its primary key should only have one column")
        try:
            self.cdc_query_projection = ','.join(["\"" + item[0] + "\"" for item in column_list])

            if self.arguments.etl_mode == 'full':  # If it's a full mode, then we switch to full mode even we have cdc
                self.cdc_where_clause = ''
                self.cdc_query_condition = ''

            elif not self.arguments.timestamp_col or not self.arguments.highwatermark:  # We can't perform incremental load w/o cdc columns or without highwatermark
                self.cdc_where_clause = ''
                self.cdc_query_condition = ''
            else:
                self.cdc_query_condition = f" {self.arguments.timestamp_col} > {self.arguments.highwatermark}"
                self.cdc_where_clause = f" where "

        except Exception as err:
            raise (f"Could not extract column list {err}")

        print(f"select {self.cdc_query_projection} from {self.arguments.source_object}{self.cdc_where_clause}{self.cdc_query_condition}")
        return f"select {self.cdc_query_projection} from {self.arguments.source_object}{self.cdc_where_clause}{self.cdc_query_condition}"


def delete_tbl(db_type):
    conf = pgconfig.conf_with_db_conn()
    #print(conf)

    para_url = {"rds":         "SCOOT_RDS_URL",
                "meta":        "SCOOT_META_URL",
                "redshift":    "SCOOT_REDSHIFT_URL",
                }

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            arg_parameter = 'tablename'
            func_params = func.__code__.co_varnames

            session_in_args = arg_parameter in func_params and func_params.index(arg_parameter) < len(args)
            session_in_kwargs = arg_parameter in kwargs

            if session_in_kwargs or session_in_args:
                try:
                    query = f"delete from {kwargs[arg_parameter]}"
                    loadConf = [conf.parameters['SCOOT_CLIENT_DBSHELL'].strip(), conf.parameters[para_url[db_type]], "-c", query]
                    #print(loadConf)
                    p2 = Popen(loadConf)
                    p2.wait()

                except Exception as err:
                    print(f"Could not execute {query}, {err}!")
                    raise
                else:
                    print(f"{query} is executed.")
            else:
                raise ("Argument 'tablename' is not found.  Pls check again")

            return func(*args, **kwargs)

        return wrapper
    return decorator

def truncate_tbl(db_type):
    conf = pgconfig.conf_with_db_conn()
    #print(conf)

    para_url = {"rds":         "SCOOT_RDS_URL",
                "meta":        "SCOOT_META_URL",
                "redshift":    "SCOOT_REDSHIFT_URL",
                }

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            arg_parameter = 'tablename'
            func_params = func.__code__.co_varnames

            session_in_args = arg_parameter in func_params and func_params.index(arg_parameter) < len(args)
            session_in_kwargs = arg_parameter in kwargs

            if session_in_kwargs or session_in_args:
                try:
                    query = f"truncate {kwargs[arg_parameter]}"
                    loadConf = [conf.parameters['SCOOT_CLIENT_DBSHELL'].strip(), conf.parameters[para_url[db_type]], "-c", query]
                    #print(loadConf)
                    p2 = Popen(loadConf)
                    p2.wait()

                except Exception as err:
                    print(f"Could not execute {query}, {err}!")
                    raise
                else:
                    print(f"{query} is executed.")
            else:
                raise ("Argument 'tablename' is not found.  Pls check again")

            return func(*args, **kwargs)

        return wrapper
    return decorator

@delete_tbl('redshift')
@check.check_test_table
def test(tablename):
    print(tablename)

if __name__ == '__main__':
    test(tablename='test1_batteries')
"""
    args1 = SimpleNamespace(highwatermark='2019-02-25 19:56:10.085183',
                            source_object='batteries',
                            source_system='rds',
                            target_object='test_batteries',
                            target_system='redshift',
                            timestamp_col='created_at,updated_at')

    print(query_builder(args1, False))

    args2 = SimpleNamespace(highwatermark='',
                            source_object='batteries',
                            source_system='rds',
                            target_object='test_batteries',
                            target_system='redshift',
                            timestamp_col='created_at,updated_at')

    print(query_builder(args2, False))
"""


