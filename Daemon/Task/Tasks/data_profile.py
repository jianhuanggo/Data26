from DataProfile import data_profile as tm
from Data.Utils import pgparse


'''
This is a template for A task which can be called from daemon framework directly.  Other form of tasks can exist 
using system call

You should always define a run function which includes the starting point of the task.  The content of task is imported 
into this script

'''


def table_profile(daemon_name=None, job_argument=None, system_parameter=None, logger=None):
    logger.info(f"In data profiler, {job_argument}")
    command_arg = pgparse.parse_argument(job_argument)
    logger.info(f"In data profiler, {system_parameter}")
    #system_parameter = parse.parse_argument(system_parameter)
    system_parameter = pgparse.json_to_ns(system_parameter)
    system_parameter.daemon_name = daemon_name

    return tm.run(command_arg, system_parameter, logger)

