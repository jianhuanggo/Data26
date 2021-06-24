from DataMover import table_mover as tm
from Data.Utils import pgparse


'''
This is a template for A task which can be called from daemon framework directly.  Other form of tasks can exist 
using system call

You should always define a run function which includes the starting point of the task.  The content of task is imported 
into this script

'''


def table_mover(daemon_name=None, job_argument=None, system_parameter=None, logger=None):
    logger.info(f"In table_mover, {job_argument}")
    command_arg = pgparse.parse_argument(job_argument)
    logger.info(f"In table_mover, {system_parameter}")
    #system_parameter = parse.parse_argument(system_parameter)
    system_parameter = pgparse.json_to_ns(system_parameter)
    system_parameter.daemon_name = daemon_name

    #combine = {**command_arg, **system_parameter}
    #logger.info(f"{command_arg.source_system}, {command_arg.source_object}, {command_arg.target_system}, {command_arg.target_object},"
    #            f"{command_arg.highwatermark}, {command_arg.timestamp_col}, {command_arg.key_col}, {command_arg.etl_mode}, {command_arg.optimize_level}")
    return tm.run(command_arg, system_parameter, logger)
    #table_mover.run(command_arg['source_system'], command_arg['source_object'], command_arg['target_system'], command_arg['target_object'])

"""
@db.connect('meta')
def table_mover(task_args=None, db_instance=None):
    query = db_instance.session.query(Job_Instance).filter(Job_Instance.job_id == '0')
    print(query)
    for _row in query.all():
        #command_arg = parse.parse_argument(task.argument_string)
        command_arg = parse.parse_argument(_row.job_argument)
        print(command_arg['source_system'], command_arg['source_object'], command_arg['target_system'],
              command_arg['target_object'])
        table_mover.run(command_arg['source_system'], command_arg['source_object'], command_arg['target_system'],
                        command_arg['target_object'])

"""
