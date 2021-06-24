from DataMover.Core import move, cli
from DataValidator.Core import validate
from types import SimpleNamespace
from Data.Utils import db
import datetime
from dateutil.parser import parse
from Data.Utils import check, pgprocess


def run(command_args, system_parameter, logger=None):

    #print(f"this is command args: {command_args}")
    #print(f"this is system parameters: {system_parameter}")

    new_args = SimpleNamespace(arguments='')
    stage_object = f"stage_{command_args.source_object}"
    if not hasattr(command_args, "stage_object") or not command_args.stage_object:
        command_args.stage_object = stage_object

    if not hasattr(command_args, "blacklist_cols"):
        command_args.blacklist_cols = ''

    if not hasattr(command_args, "exact_match"):
        command_args.exact_match = True

    if hasattr(system_parameter, "highwatermark") and system_parameter.highwatermark:
        command_args.highwatermark = system_parameter.highwatermark
    else:
        command_args.highwatermark = ''

    new_args.arguments = SimpleNamespace(highwatermark=command_args.highwatermark,
                                         source_system=command_args.source_system,
                                         source_object=command_args.source_object,
                                         target_system=command_args.target_system,
                                         stage_object=command_args.stage_object,
                                         target_object=command_args.target_object,
                                         timestamp_col=command_args.timestamp_col,
                                         cdc_type=command_args.cdc_type,
                                         key_col=command_args.key_col,
                                         etl_mode=command_args.etl_mode,
                                         table_size=command_args.table_size,
                                         optimize_level=command_args.optimize_level,
                                         blacklist_cols=check.check_const_list(pgprocess.get_param_in_list(command_args.blacklist_cols),
                                                                               pgprocess.get_param_in_list(command_args.key_col),
                                                                               msg="Primary Key can't be in the blacklist!"),
                                         exact_match=bool(int(command_args.exact_match)))

    new_args.system_parameter = SimpleNamespace(datafile_loc=system_parameter.datafile_loc)
    #print(f"the datafileloc: {new_args.system_parameter}")
    #print(f"the highwatermark: {new_args.arguments.highwatermark}"
    new_args.logger = logger

    connection = move.Postgres2Redshift(new_args)
    connection.setup_postgresql()
    logger.info("Get table stats for validation purpose...")

    max_cdc_col1_src, max_cdc_col2_src, start_cnt = connection.end2end_count_validation('source')

    logger.info(f"The number of records in source table is {int(start_cnt)}")
    logger.info("Starting the data extraction process...")
    #connection.args.logger.info("Starting the data extraction process...")
    return_code, to_continue = connection.extract(optimize_level=int(new_args.arguments.optimize_level),
                                                  col_blacklist=new_args.arguments.blacklist_cols)

    if not return_code:
        return False, None

    if to_continue:
        logger.info("New data found or optimizer level is not set to 1")
        connection.upload_s3()
        logger.info("Completed!")
        connection.setup_redshift()
        logger.info("Starting loading to target system...")
        connection.copy2redshift(s3_bucket_location=connection.s3_bucket_location + connection.extract_filename,
                                 tablename=new_args.arguments.stage_object,
                                 opt=new_args)
        logger.info("Starting applying changes...")
        if not connection.apply_change():
            return False, None

        logger.info("Gather table statistics...")

        print(new_args.arguments.table_size)

        if new_args.arguments.table_size == 'large':
            max_cdc_col1_tgt, max_cdc_col2_tgt, end_cnt = connection.end2end_count_validation('stage')
        else:
            max_cdc_col1_tgt, max_cdc_col2_tgt, end_cnt = connection.end2end_count_validation('target')

        logger.info(f"The number of records in target table is {int(end_cnt)}")

        print(max_cdc_col1_src, max_cdc_col2_src, start_cnt)
        print(max_cdc_col1_tgt, max_cdc_col2_tgt, end_cnt)

        logger.info("Verifying data...")

        rtn_hwm = None
        if connection.cdc_count_comparison(start_cnt=start_cnt,
                                           end_cnt=end_cnt,
                                           exact_match=new_args.arguments.exact_match):
            print("validation is successful")

            if new_args.arguments.cdc_type == 'timestamp':
                if max_cdc_col1_tgt == 'NA' and max_cdc_col2_tgt == 'NA':
                    rtn_hwm = new_args.arguments.highwatermark
                elif max_cdc_col1_tgt != 'NA' and max_cdc_col2_tgt != 'NA':
                    rtn_hwm = connection.cdc_time_comparison(max_cdc_col1=max_cdc_col1_tgt,
                                                             max_cdc_col2=max_cdc_col2_tgt)
                elif max_cdc_col1_tgt == 'NA':
                    rtn_hwm = max_cdc_col2_tgt
                else:
                    rtn_hwm = max_cdc_col1_tgt
            elif new_args.arguments.cdc_type == 'primary_key' and new_args.arguments.etl_mode != 'full':
                if new_args.arguments.etl_mode != 'full':
                    rtn_hwm = connection.get_hwm_id(target='stage')

            print(f"high water mark is: {rtn_hwm}")
            if not rtn_hwm:
                rtn_hwm = new_args.arguments.highwatermark

            return True, SimpleNamespace(datafile_loc=connection.complete_path,
                                         affected_rowcount=start_cnt,
                                         highwatermark=str(rtn_hwm))
        else:
            print("validation is not successful")
            return False, None

    else:
        logger.info("Optimizer level is set to 1 and there is no difference")
        return True, SimpleNamespace(datafile_loc=connection.complete_path,
                                     affected_rowcount=start_cnt,
                                     highwatermark=new_args.arguments.highwatermark)


if __name__ == '__main__':

    new_args = cli.get_table_mover_parser()
    #args.stage_table = f"stage_{args.source_table}"

    col_bl = 'manufacturer_serial_number'
    print(pgprocess.get_param_in_list(col_bl))
    new_args.arguments = SimpleNamespace(highwatermark='2019-04-05 00:00:00.000000',
                                     source_object=new_args.source_table,
                                     source_system='rds',
                                     stage_object=new_args.stage_table,
                                     target_object=new_args.target_table,
                                     target_system='redshift',
                                     timestamp_col='created_at, updated_at',
                                     #timestamp_col='id',
                                     cdc_type='timestamp',
                                     key_col=new_args.key_col,
                                     table_size='normal',
                                     etl_mode='incr',
                                     blacklist_cols=check.check_const_list(pgprocess.get_param_in_list(col_bl),
                                                                           pgprocess.get_param_in_list(new_args.key_col),
                                                                           msg="Primary Key can't be in the blacklist!"))

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

