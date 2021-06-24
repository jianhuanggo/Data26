from DataMover.Core import move, cli, s3_redshift
from DataValidator.Core import validate
from types import SimpleNamespace
from Data.Utils import db
import datetime
from dateutil.parser import parse
from Data.Utils import check, pgprocess, pgyaml, pgfile
import random


def run(command_args, system_parameter, logger=None):

    print(f"this is command args: {command_args}")
    print(f"this is system parameters: {system_parameter}")

    new_args = SimpleNamespace(arguments='')
    stage_object = f"stage_{command_args.source_object}"
    if not hasattr(command_args, "stage_object") or not command_args.stage_object:
        command_args.stage_object = stage_object

    if not hasattr(system_parameter, "daemon_id") or not system_parameter.daemon_id:
        system_parameter.daemon_id = random.randint(1, 10000)

    new_args.arguments = SimpleNamespace(source_system=command_args.source_system,  # source system is drop box
                                         source_object=command_args.source_object,  # source object is csv
                                         target_system=command_args.target_system,  # target
                                         target_object=command_args.target_object,
                                         yaml_location=command_args.yaml_location,
                                         datafile_location=command_args.datafile_location)

    yaml_filepath = new_args.arguments.yaml_location + '/'

    file_list = pgfile.get_all_file_in_dir(new_args.arguments.yaml_location)
    for item in file_list:
        yaml_filepath = new_args.arguments.yaml_location + '/' + item
        pgfile.file_move(yaml_filepath, '/daemon_name/processing/filename')
        if pgfile.isfileexist(yaml_filepath):
            break

    list =[]
    yaml_content = pgyaml.yaml_load(yaml_filepath)
    full_filename = new_args.arguments.datafile_location + '/' + yaml_content['data_file']
    if not pgfile.isfileexist(full_filename):
        raise ("datafile is not found")
    else:
        new_args.yaml_file = full_filename
    if s3_redshift.s3_to_redshift(new_args).move():
        print("This proceess is succcesful and pls return")
        return True, SimpleNamespace()
    else:
        return False, None


def clean_up(*, yaml_file_path, data_file_path):
    try:
        pgfile.remove_file(yaml_file_path)
        for item in data_file_path:
            pgfile.remove_file(item)
    except Exception as err:
        raise err


if __name__ == '__main__':

    new_args = cli.get_table_mover_parser()
    #args.stage_table = f"stage_{args.source_table}"

    new_args.arguments = SimpleNamespace(
                                        source_object=new_args.source_table,
                                        source_system='drop_box',
                                        target_object=new_args.target_table,
                                        target_system='redshift'
                                        )

    if new_args == new_args:
        print(f"This is the right path, ")
    else:
        print(f"this is the wrong path, ")


    new_args.system_parameter = SimpleNamespace(datafile_loc='')
    new_args.logger = None
    conn = move.Postgres2Redshift(new_args)
    conn.setup_postgresql()

    max_cdc_col1_src, max_cdc_col2_src, start_cnt = conn.end2end_count_validation('source')

    print(max_cdc_col1_src, max_cdc_col2_src, start_cnt)

    conn.extract(optimize_level=0, col_blacklist=new_args.arguments.blacklist_cols)
    conn.upload_s3()

    conn.setup_redshift()
    print(conn.s3_bucket_location + conn.extract_filename)
    conn.copy2redshift(s3_bucket_location=conn.s3_bucket_location + conn.extract_filename,
                       tablename=new_args.arguments.stage_object,
                       opt=new_args)
    conn.apply_change()

    if new_args.arguments.table_size == 'large':
        max_cdc_col1_tgt, max_cdc_col2_tgt, end_cnt = conn.end2end_count_validation('stage')
    else:
        max_cdc_col1_tgt, max_cdc_col2_tgt, end_cnt = conn.end2end_count_validation('target')

    print(max_cdc_col1_src, max_cdc_col2_src, start_cnt)

    if conn.cdc_count_comparison(start_cnt=start_cnt, end_cnt=end_cnt):
        print("validation is successful")
    else:
        print("validation is not successful")

    rtn_hwm = None

    if new_args.arguments.cdc_type == 'timestamp':
        if max_cdc_col1_tgt == 'NA' and max_cdc_col2_tgt == 'NA':
            rtn_hwm = new_args.arguments.highwatermark
        elif max_cdc_col1_tgt != 'NA' and max_cdc_col2_tgt != 'NA':
            rtn_hwm = conn.cdc_time_comparison(max_cdc_col1=max_cdc_col1_tgt, max_cdc_col2=max_cdc_col2_tgt)
        elif max_cdc_col1_tgt == 'NA':
            rtn_hwm = max_cdc_col2_tgt
        else:
            rtn_hwm = max_cdc_col1_tgt
    elif new_args.arguments.cdc_type == 'primary_key':
        rtn_hwm = conn.get_hwm_id(target='stage')

    print(rtn_hwm)

