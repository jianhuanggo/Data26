import logging
import argparse
import sys
from Data.Utils import pgparse
from Data.Utils import db
from Daemon.Model import Job
from sqlalchemy import cast, Integer

_version_ = 0.1


def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='MetaData Manager')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('-f', '--field', action='store', dest='field_text', required=True,
                               help='New fields to be added')
        argparser.add_argument('-i', '--job_id', action='store', dest='job_id', help='New fields to be added')
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
        print(self.args.job_id)

        try:
            self.new_field_ns = pgparse.parse_argument(self.args.field_text)

        except Exception as err:
            raise err

        try:
            if self.args.job_id:
                query = db_instance.session.query(Job).filter(Job.job_id == str(self.args.job_id)).order_by(cast(Job.priority_value, Integer),
                                                                cast(Job.job_id, Integer))
            else:
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


if __name__ == '__main__':
    args = get_parser()
    MetaDataManager(args).job_add_arg_field()
