import numpy as np
import pandas as pd
from types import SimpleNamespace
from sqlalchemy.exc import DataError, DatabaseError, IntegrityError
from DataValidator.Model import Table_MetaData, Table_Active_Replica
from DataValidator.Core import validate, cli
from Data.Utils import db

THRESHOLD_VAL = 0.2
NUM_OF_REC_2_CHECK = 200


def gen_query(source_tbl_name, target_tbl_name, key_col, exclusion_col_list):
    column_list = db.get_column_name_by_order(source_tbl_name)
    column_list.sort(key=lambda x: x[1])
    max_id, min_id = get_max_min_id(f"select max({key_col}), min({key_col}) from {target_tbl_name}")
    if max_id is None:
        return None, None, None
    starting_num = generate_starting_num(min_id, max_id)
    column_list_str = ','.join(["\"" + item[0] + "\"" for item in column_list if item[0] not in exclusion_col_list])

    order_by_clause = f" order by {key_col}"

    if int(max_id) - int(min_id) < NUM_OF_REC_2_CHECK:
        where_clause = ''
        starting_num = min_id
        end_num = max_id
    else:
        starting_num = starting_num
        end_num = starting_num + NUM_OF_REC_2_CHECK
        where_clause = f" where {key_col} between {starting_num} and {end_num}"

    return f"select {column_list_str} from {source_tbl_name}{where_clause}{order_by_clause}",\
           f"select {column_list_str} from {target_tbl_name}{where_clause}{order_by_clause}",\
           SimpleNamespace(source_object=source_tbl_name,
                target_object=f"test_{target_tbl_name}",
                key_col=key_col,
                start_check_num=starting_num,
                end_check_num=end_num)

def generate_starting_num(local_min, local_max):
    return round(int(local_max * THRESHOLD_VAL))

@db.connect('redshift')
def get_max_min_id(query, db_instance=None):
    try:
        result = db_instance.session.execute(query).fetchall()
        #print(result[0][0], result[0][1])
        return result[0][0], result[0][1]
    except (DatabaseError, DataError, IntegrityError) as err:
        raise ("Something wrong with query")


#@db.connect('redshift')
def sql2pandas(query, db_instance=None):
    try:
        df = pd.read_sql_query(query, con=db_instance.engine)
    except Exception as err:
        raise("Could not read content")

    return df

@db.connect('meta')
def get_table_list(db_instance=None):

    table_list = []
    try:

        """
        query = db_instance.session.query(Table_MetaData).join(Job, Job_Instance.job_id == Job.job_id).filter(
            and_(Job_Instance.hold_flag == '0',
                 Job_Instance.ignore_flag == '0', Job.hold_flag == '0'),
            Job_Instance.status == 'NEW').order_by(cast(Job_Instance.priority_value, Integer),
                                                   cast(Job_Instance.job_instance_id, Integer))

        """
        #redshift_table_list = db_instance.session.query(Table_MetaData).filter(Table_MetaData.database_name == 'Redshift')
        #rds_table_list = db_instance.session.query(Table_MetaData).filter(Table_MetaData.database_name == 'RDS')

        query = db_instance.session.query(Table_Active_Replica)

        for _row in query.all():
            yield (_row.source_tbl_name, _row.target_tbl_name, _row.key_col)

    except (DataError, DatabaseError, IntegrityError) as err:
        raise (f"Could not get table list {err}")


if __name__ == '__main__':
    args = cli.data_validator_parser()

    table_list = []

    if not args.auto_flag:
        table_list.append((args.source_table, args.target_table, args.key_col))

    else:
        for item in get_table_list():
            table_list.append(item)

    for item in table_list:
        print("\n")
        print(f"Table Name: {item[0]}")
        #exit(0)
        source_sql, target_sql, key_meta = gen_query(source_tbl_name=item[0], target_tbl_name= item[1],
                                                     key_col=item[2], exclusion_col_list=['serial_number'])
        if source_sql is None and target_sql is None and key_meta is None:
            continue
        print(source_sql)
        print(target_sql)

        sql2pandas_rds = db.connect('rds')(sql2pandas)
        sql2pandas_red = db.connect('redshift')(sql2pandas)

        if int(args.mode_flag) == 1:
            diff_1 = set(sql2pandas_rds(source_sql)) - set(sql2pandas_red(target_sql))


            diff_2 = set(sql2pandas_red(target_sql)) - set(sql2pandas_rds(source_sql))

            print("Result")
            print('-' * 60)
            print(f"The difference between table1 minus table2 is {diff_1}")
            print(f"The difference between table2 minus table1 is {diff_2}")

        else:
            result = validate.data_content_validation(sql2pandas_rds(source_sql), sql2pandas_red(target_sql))

            if result is not None:
                print(result)
            else:
                print(f"The data content between {key_meta.source_object} and {key_meta.target_object} between {key_meta.key_col} # "
                      f"{key_meta.start_check_num} and {key_meta.end_check_num} matches 100% ")


# to run the command
#