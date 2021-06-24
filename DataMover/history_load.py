from DataMover.Core import history_load, cli
from DataValidator.Core import validate
from types import SimpleNamespace
from Data.Utils import pgprocess
from Data.Utils import db


def run(command_args, system_parameter, logger=None):

    #print(f"this is command args: {command_args}")
    #print(f"this is system parameters: {system_parameter}")
    new_args = SimpleNamespace(arguments='')

    daemon_id = str(system_parameter.daemon_name.split('_')[-1])

    new_args.arguments = SimpleNamespace(source_system=command_args.source_system,
                                         source_object=command_args.source_object,
                                         target_system=command_args.target_system,
                                         stage_object=f"{command_args.stage_object}_{daemon_id}",
                                         target_object=command_args.target_object,
                                         timestamp_col=command_args.timestamp_col,
                                         key_col=command_args.key_col,
                                         etl_mode=command_args.etl_mode,
                                         optimize_level=command_args.optimize_level)



    new_args.system_parameter = SimpleNamespace(datafile_loc=system_parameter.datafile_loc,
                                                lowwatermark=system_parameter.lowwatermark,
                                                highwatermark=system_parameter.highwatermark,
                                                daemon_id=daemon_id
                                                )


    print(f"datafileloc: {new_args.system_parameter}")
    print(f"lowwatermark: {new_args.system_parameter.lowwatermark}")
    print(f"highwatermark: {new_args.system_parameter.highwatermark}")
    print(f"daemon_id: {new_args.system_parameter.daemon_id}")

    new_args.logger = logger

    connection = history_load.Postgres2Redshift_hist(new_args)
    connection.setup_postgresql()
    connection.setup_redshift()

    logger.info("Check for Staging tables...")

    logger.info("Get table stats for validation purpose...")

    is_failed, start_validation_cnt = connection.start_end2end_count_validation_v2()

    if not is_failed:
        raise ("Can't check data")

    logger.info(f"The number of records in source table is {int(start_validation_cnt)}")
    logger.info("Starting the data extraction process...")

    #connection.args.logger.info("Starting the data extraction process...")
    return_code, to_continue = connection.extract()

    if not return_code:
        return False, None

    connection.upload_s3()
    logger.info("Completed!")

    logger.info("Starting loading to target system...")

    connection.setup_staging_env()
    connection.copy2redshift(s3_bucket_location=connection.s3_bucket_location + connection.extract_filename,
                             tablename=new_args.arguments.stage_object, opt=new_args)
    logger.info("Starting applying changes...")

    if not connection.apply_change():
        return False, None

    logger.info("Verifying data...")

    is_failed, end_validation_cnt = connection.end_end2end_count_validation_v2()
    if not is_failed:
        raise ("record count doesn't match")

    logger.info(f"The number of records in target table is {int(end_validation_cnt)}")

    if start_validation_cnt is None:
        start_validation_cnt = 0

    if end_validation_cnt is None:
        end_validation_cnt = 0

    print(f"Expecting {start_validation_cnt} records and getting {end_validation_cnt} records")

    if int(start_validation_cnt) == int(end_validation_cnt):
        print("successfully loadded")
        return True, SimpleNamespace(datafile_loc=connection.complete_path,
                                     affected_rowcount=start_validation_cnt,
                                     highwatermark=system_parameter.highwatermark)
    else:
        return False, None


if __name__ == '__main__':

    args = cli.get_table_mover_parser()
    #args.stage_table = f"stage_{args.source_table}"
    args.arguments = SimpleNamespace(source_object=args.source_table, source_system='rds',
                                     stage_object=args.stage_table, target_object=args.target_table,
                                     target_system='redshift', timestamp_col='id', key_col=args.key_col)

    args.system_parameter = SimpleNamespace(datafile_loc='', highwatermark=200000, lowwatermark=1)
    args.logger=None
    conn = history_load.Postgres2Redshift_hist(args)
    conn.setup_postgresql()
    conn.setup_redshift()
    #print(args)
    is_failed, start_validation_cnt = conn.start_end2end_count_validation_v2()

    if not is_failed:
        raise ("Can't check data")

    conn.extract()
    conn.upload_s3()

    print(conn.s3_bucket_location + conn.extract_filename)

    conn.copy2redshift(s3_bucket_location=conn.s3_bucket_location + conn.extract_filename,
                       tablename=args.arguments.stage_object,
                       opt=args)
    #conn.copy2redshift(s3_bucket_location=conn.s3_bucket_location + conn.extract_filename, tablename=args.stage_table, opt=args)
    conn.apply_change()

    is_failed, end_validation_cnt = conn.end_end2end_count_validation_v2()

    if not is_failed:
        raise ("record count doesn't match")

    if start_validation_cnt is None:
        start_validation_cnt = 0

    if end_validation_cnt is None:
        end_validation_cnt = 0

    if int(start_validation_cnt) == int(end_validation_cnt):
        print("successfully loadded")

