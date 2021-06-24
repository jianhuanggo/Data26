from Data.Utils import db
import logging
import argparse
import sys

_version_ = 0.5

def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='Daemon Job generation script')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-s', '--source_schema', action='store', dest='source_schema',
                               required=True, help='Source Schema')
        argparser.add_argument('-t', '--target_schema', action='store', dest='target_schema',
                               required=True, help='Target Schema')
        logging.debug('parsing argparser arguments')
        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    # print(args.daemon_type)
    return args


@db.connect('redshift')
def schema_compare(*, schema_name_src, schema_name_tgt, db_instance=None):

    schema_1_tbl_list = []
    schema_2_tbl_list = []

    for item in db_instance.get_table_list_by_schema(schema_name=schema_name_src).fetchall():
        schema_1_tbl_list.append(*item)

    for item in db_instance.get_table_list_by_schema(schema_name=schema_name_tgt).fetchall():
        schema_2_tbl_list.append(*item)

    print(schema_1_tbl_list)
    print(schema_2_tbl_list)

    diff_lists(list1=schema_1_tbl_list, list2=schema_2_tbl_list, schema_1=schema_name_src, schema_2=schema_name_tgt)


def diff_lists(*, list1, list2, schema_1, schema_2):
    diff_1 = []
    diff_2 = []
    for item in list1:
        if item not in list2 and item != '_sdc_rejected':
            diff_1.append(item)

    for item in list2:
        if item not in list1 and item != '_sdc_rejected':
            diff_2.append(item)

    print(f"\n\nFollowing tables in {schema_1} but not in {schema_2}...")
    for item in diff_1:
        print(item)

    print(f"\n\nFollowing tables in {schema_2} but not in {schema_1}...")
    for item in diff_2:
        print(f"The table count for {item} is {get_count(schema_2, item)}\n")


@db.connect('redshift')
def get_count(schema_name, table_name, db_instance=None):
    return db_instance.session.execute(f"select count(1) from {schema_name}.{table_name}").fetchall()[0][0]


@db.connect('rds')
def get_data(db_instance=None):
    select_query = db_instance.session.execute(f"select id, expires_on from identity_documents")
    count = 0
    for _row in select_query.fetchall():
        if count < 10:
            if _row[1] and str(_row[1]).split('-')[0] == '9999':
                print(_row)

            # if str(_row[0]) == '7438':
                #print(str(_row[1]))
        else:
            break


if __name__ == '__main__':
    get_data()
    exit(0)
    args = get_parser()
    schema_compare(schema_name_src=args.source_schema, schema_name_tgt=args.target_schema)
