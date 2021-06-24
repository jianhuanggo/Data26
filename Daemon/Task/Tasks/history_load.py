from DataMover import history_load as hl
from Data.Utils import pgparse


def history_load(daemon_name=None, job_argument=None, system_parameter=None, logger=None):
    #logger.info(f"In table_mover, {job_argument}")
    command_arg = pgparse.parse_argument(job_argument)
    system_parameter = pgparse.json_to_ns(system_parameter)
    system_parameter.daemon_name = daemon_name

    return hl.run(command_arg, system_parameter, logger)
