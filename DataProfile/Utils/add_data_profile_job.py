import yaml
import logging
import argparse
import sys
from Data.Utils import pgyaml
from Data.Utils import pgfile, pgparse, db
from Daemon.Model import Job

from types import SimpleNamespace


_version_ = 0.1


def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='Data Profile Job Manager')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-f', '--yaml_filepath', action='store', dest='yaml_filepath', required=True,
                               help='Yaml file for job creation')
        logging.debug('parsing argparser arguments')
        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    # print(args.daemon_type)
    return args


class DataProfileJobAdd:
    def __init__(self):
        self._job_id = None
        self._parameter_value = None

    @property
    def job_id(self):
        return self._job_id

    @property
    def parameter_value(self):
        return self._parameter_value

    def get_parameter_value(self, yaml_filepath: str):
        if not pgfile.isfileexist(yaml_filepath):
            raise ("The file does not exist...")

        yaml_content = pgyaml.yaml_load(yaml_filename=yaml_filepath)

        self._parameter_value = pgparse.convert_to_parameter_str(SimpleNamespace(db_system=str(yaml_content['db_system']),
                                                                                 query_condition=str(yaml_content['query_condition']),
                                                                                 threshold_value=str(yaml_content['threshold_value']),
                                                                                 matching_operator=str(yaml_content['matching_operator'])))

        self._job_id = yaml_content['Jod_id']

        return str(self._job_id), self._parameter_value

    @db.connect('meta')
    def job_add(self, db_instance=None):

        try:

            db_instance.session.add(Job(str(self._job_id), 'data_profile' + str(self._job_id), '0', '20',
                                            '127.0.0.1', '0', 'Package', 'data_profiler',
                                            self._parameter_value, '{"indicator": false}'))

            db_instance.session.commit()

        except Exception as err:
            raise err

        else:
            print(f"Job id# {self._job_id} is added successfully")


if __name__ == '__main__':
    args = get_parser()
    profile_job_add = DataProfileJobAdd()
    print(profile_job_add.get_parameter_value(args.yaml_filepath))
    print(profile_job_add.job_id, profile_job_add.parameter_value)
    profile_job_add.job_add()

