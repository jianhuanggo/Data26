import logging
import argparse
import sys
from Data.Utils import pgparse
from Data.Utils import db
from Daemon.Model import Job
from sqlalchemy import cast, Integer
from Data.Utils import pgparse

_version_ = 0.1


def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='MetaData Manager')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-f', '--field', action='store', dest='field_text', required=True,
                               help='New fields to be added')
        logging.debug('parsing argparser arguments')
        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    # print(args.daemon_type)
    return args


class MetaDataManager:
    def __init__(self, args):
        self.args = args
        self.new_field_ns = None
        self.existing_field_ns = None
        self.new_text = ''

    @db.connect('meta')
    def job_add_arg_field(self, db_instance=None):
        print(self.args.field_text)
        try:
            self.new_field_ns = pgparse.parse_argument(self.args.field_text)

        except Exception as err:
            raise err

        try:

            query = db_instance.session.query(Job).order_by(cast(Job.priority_value, Integer), cast(Job.job_id, Integer))

            for _row in query.all():
                self.new_text = ''
                print(_row.job_id, _row.job_argument)
                try:
                    self.existing_field_ns = pgparse.parse_argument(_row.job_argument)
                except Exception as err:
                    raise err

                #print(self.existing_field_ns)

                for key, val in self.new_field_ns.__dict__.items():
                    if hasattr(self.existing_field_ns, key):
                        print(f"{key} is already in the existing argument")
                    else:
                        self.new_text += f";{key}:{val}"

                if not self.new_text:
                    print("Nothing to add")
                    continue

                new_job_argument = _row.job_argument + self.new_text
                print(f"this is the new text: {new_job_argument}")

                update_query = db_instance.session.query(Job).\
                    filter(Job.job_id == _row.job_id).\
                    update({'job_argument': str(new_job_argument)})

                if update_query:
                    print("New argument(s) have been added")
                else:
                    print("These new arguments already existed in the current arguement.  No change is made.")

                db_instance.session.commit()


        except Exception as err:
            raise err


class JobConfiguration:
    def __init__(self):
        self.job_argument_config = []


    @db.connect('meta')
    def get_config(self, db_instance=None):

        query = db_instance.session.query(Job).order_by(cast(Job.priority_value, Integer), cast(Job.job_id, Integer))
        for _row in query.all():
            self.job_argument_config.append([_row.job_id, pgparse.parse_argument(_row.job_argument), _row.job_name])

    def print_config(self):
        for item in self.job_argument_config:
            if not hasattr(item[1], 'exact_match'):
                item[1].exact_match = '0'
                print(f"update metaschemaeu.scd_job set job_argument = "
                      f"'{pgparse.convert_to_parameter_str(item[1])}' where job_id = '{item[0]}' "
                      f"and job_name = '{item[2]}'")




if __name__ == '__main__':
    jb = JobConfiguration()
    jb.get_config()
    jb.print_config()

    #args = get_parser()
    #MetaDataManager(args).job_add_arg_field()


"""


def table_mover(daemon_name=None, job_argument=None, system_parameter=None, logger=None):
    logger.info(f"In table_mover, {job_argument}")
    command_arg = parse.parse_argument(job_argument)
    logger.info(f"In table_mover, {system_parameter}")
    #system_parameter = parse.parse_argument(system_parameter)
    system_parameter = parse.json_to_ns(system_parameter)
    system_parameter.daemon_name = daemon_name


"""