from Data.Utils import db
from Data.Utils import query as qy
from types import SimpleNamespace
import pandas as pd
import numpy as np
from Data.Utils import default
import datetime
import subprocess
from Data.Utils import pgprocess


@db.connect('rds')
def incr_count_validation(args, db_instance=None):
    validation_query = qy.query_builder(args, True)
    #print(validation_query)
    result = db_instance.session.execute(validation_query)

    item = result.fetchall()
    min_val1 = item[0][0]
    max_val1 = item[0][1]
    min_val2 = item[0][2]
    max_val2 = item[0][3]
    record_count = item[0][4]

    print(min_val1, max_val1, min_val2, max_val2, record_count)


@db.connect('rds')
def start_end2end_count_validation(args, db_instance=None):
    #print("Start end to end Data validation...")
    validation_query = qy.query_builder_v3(args, 'source')
    #print(f"ZZZZZZZZ!!!!!{validation_query}")

    try:
        #print(validation_query)
        query_result = db_instance.session.execute(validation_query)
        item = query_result.fetchall()
        #print(item[0][0], item[0][1], item[0][2])
        #print(f"The number of records in source table is {item[0][2]}")

    except Exception as e:
        raise (f"Something wrong with executing count validation query! {e}")

    return_list = default.set_default(item[0], "not available", datetime.datetime.max)

    #for item in return_list:
        #print(item)

    #return item[0][0], item[0][1], item[0][2]
    return return_list[0], return_list[1], return_list[2]

"""
def start_end2end_count_validation_v2(*, args, db_client_dbshell, db_url):
    #print("Start collection stats from source table...")

    #validation_query = qy.query_builder_v3(args.arguments, 'target')
    validation_query = qy.QueryFactory(args.arguments).qb_count_validation(target='source')
    print(f"here: {validation_query}")

    try:
        result_list = process.run_query(db_client_dbshell=db_client_dbshell, db_url=db_url, query=validation_query)

    except Exception as e:
        raise e

    # return item[0][0], item[0][1], item[0][2]
    return result_list[0], result_list[1], result_list[2]
"""

def end2end_count_validation_v2(*, args, db_client_dbshell, db_url, target):
    #print("Start collection stats from target table...")

    if args.arguments.cdc_type == 'timestamp':
        validation_query = qy.QueryFactory(args.arguments).qb_count_validation_ts(table_size=args.arguments.table_size,
                                                                                  target=target)
    else:
        validation_query = qy.QueryFactory(args.arguments).qb_count_validation_pk(table_size=args.arguments.table_size,
                                                                                  target=target)
    print(f"here: {validation_query}")

    # validation_query = qy.query_builder_v3(args.arguments, 'target')
    # print(validation_query)

    try:
        result_list = pgprocess.run_query(db_client_dbshell=db_client_dbshell, db_url=db_url, query=validation_query)

    except Exception as e:
        raise e

    return result_list[0], result_list[1], result_list[2]


@db.connect('redshift')
def end_end2end_count_validation(args, db_instance=None):
    #print("Verifying data.....")
    validation_query = qy.query_builder_v3(args, 'target')
    #print(validation_query)

    try:
        #print(validation_query)
        query_result = db_instance.session.execute(validation_query)
        print(query_result)
        item = query_result.fetchall()
        #print(item)
        #print(item[0][0], item[0][1], item[0][2])
        print(f"The number of records in source table is {item[0][2]}")

    except Exception as e:
        raise (f"Something wrong with executing count validation query! {e}")

    return item[0][0], item[0][1], item[0][2]





def end_end2end_count_validation_v1(args):
    #print("Verifying data.....")
    validation_query = qy.query_builder_v3(args.arguments, 'target')
    #print(validation_query)

    try:
        #print(args)

        db_client_dbshell = args.parameters['SCOOT_CLIENT_DBSHELL']
        username = args.parameters['SCOOT_REDSHIFT_USERNAME']
        passwd = args.parameters['SCOOT_REDSHIFT_PASS']
        host = args.parameters['SCOOT_REDSHIFT_HOST']
        port = args.parameters['SCOOT_REDSHIFT_PORT']
        database = args.parameters['SCOOT_REDSHIFT_DB']

        postgresql_url = f"postgresql://{username}:{passwd}@{host}:{port}/{database}"

        exec_query = [db_client_dbshell, postgresql_url, "-c", validation_query]

        proc = subprocess.Popen(exec_query, stdout=subprocess.PIPE)
        proc.wait()
        reader = proc.stdout
        result_list = [item.strip() for item in reader.read().decode('utf-8').split('\n')[2].split('|')]
        #print(result_list)

        
        #query_result = db_instance.session.execute(validation_query)
        #print(query_result)
        #item = query_result.fetchall()
        #print(item)
        #print(item[0][0], item[0][1], item[0][2])
        #print(f"The number of records in source table is {item[0][2]}")


    except Exception as e:
        raise (f"Something wrong with executing count validation query! {e}")

    #return item[0][0], item[0][1], item[0][2]
    return result_list[0], result_list[1], result_list[2]

def data_content_validation(df1, df2):
    assert (df1.columns == df2.columns).all(), "DataFrame column names are different"
    if any(df1.dtypes != df2.dtypes):
        "Data Types are different, trying to convert"
        df2 = df2.astype(df1.dtypes)
    if df1.equals(df2):
        return None
    else:
        # need to account for np.nan != np.nan returning True
        diff_mask = (df1 != df2) & ~(df1.isnull() & df2.isnull())
        ne_stacked = diff_mask.stack()
        changed = ne_stacked[ne_stacked]
        changed.index.names = ['id', 'col']
        difference_locations = np.where(diff_mask)
        changed_from = df1.values[difference_locations]
        changed_to = df2.values[difference_locations]
        return pd.DataFrame({'from': changed_from, 'to': changed_to},
                            index=changed.index)

def csv_to_csv_diff(filename1, filename2):
    set1 = set(pd.read_csv(filename1, index_col=False, header=None)[0])
    set2 = set(pd.read_csv(filename2, index_col=False, header=None)[0])

    #print(set1 - set2)
    #print(set2 - set1)

    if (bool(set1 - set2) is False) and (bool(set2 - set1)) is False:
        return False
    else:
        return True


if __name__ == '__main__':

    csv_to_csv_diff('/Users/jianhuang/scoot_data/data/data_mover/batteries/save204b80_batteries.csv',
                    '/Users/jianhuang/scoot_data/data/data_mover/batteries/saveb46e24_batteries.csv')


    args1 = SimpleNamespace(highwatermark='2019-02-25 19:56:10.085183',
                            timestamp_col='created_at,updated_at',
                            source_object='batteries',
                            target_object='test_batteries',
                            key_col='id')
    incr_count_validation(args1)

    start_end2end_count_validation(args1)
    end_end2end_count_validation(args1)


