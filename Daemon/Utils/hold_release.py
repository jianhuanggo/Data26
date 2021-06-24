import logging
import sys
from Daemon.Utils import base
import argparse
from Daemon.Model import Job, Job_Instance
from Data.Utils import db

_version_ = 0.1


def get_parser():
    try:
        logging.debug('defining argparse arguments')

        argparser = argparse.ArgumentParser(description='MetaData Manager')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        argparser.add_argument('request', action='store', choices=['hold', 'release'],
                               help='hold or release jobs on data pipeline')
        logging.debug('parsing argparser arguments')
        args = argparser.parse_args()

    except Exception as err:
        logging.critical("Error creating/parsing arguments:\n%s" % str(err))
        sys.exit(100)

    # print(args.daemon_type)
    return args


class hold_release(base.JobManagement):
    def __init__(self):
        super().__init__()
        self.sql_hold_queue = []
        self.sql_release_queue = []

    def command(self):
        SQL_hold_command1 = '''update metaschema.scd_job set hold_flag = '1000' where hold_flag = '0' '''
        SQL_hold_command2 = '''update metaschemasa.scd_job set hold_flag = '1000' where hold_flag = '0' '''
        SQL_hold_command3 = '''update metaschemaeu.scd_job set hold_flag = '1000' where hold_flag = '0' '''

        self.sql_hold_queue.append(SQL_hold_command1)
        self.sql_hold_queue.append(SQL_hold_command2)
        self.sql_hold_queue.append(SQL_hold_command3)

        SQL_release_command1 = '''update metaschema.scd_job set hold_flag = '0' where hold_flag = '1000' '''
        SQL_release_command2 = '''update metaschemasa.scd_job set hold_flag = '0' where hold_flag = '1000' '''
        SQL_release_command3 = '''update metaschemaeu.scd_job set hold_flag = '0' where hold_flag = '1000' '''

        self.sql_release_queue.append(SQL_release_command1)
        self.sql_release_queue.append(SQL_release_command2)
        self.sql_release_queue.append(SQL_release_command3)


    @db.connect('meta')
    def job_hold(self, db_instance=None):
        try:
            db_instance.session.query(Job).filter(Job.hold_flag == '0').update({'hold_flag': '1000'})
            db_instance.session.commit()

        except Exception as err:
            db_instance.session.rollback()
            raise err
        else:
            print("command executed successfully")

    @db.connect('meta')
    def job_release(self, db_instance=None):
        try:
            db_instance.session.query(Job).filter(Job.hold_flag == '1000').update({'hold_flag': '0'})
            db_instance.session.commit()

        except Exception as err:
            db_instance.session.rollback()
            raise err
        else:
            print("command executed successfully")

    @db.connect('meta')
    def hold_all(self, db_instance=None):

        self.command()
        try:
            for item in self.sql_hold_queue:
                db_instance.session.execute(item)
                db_instance.session.commit()
        except Exception as err:
            db_instance.session.rollback()
            raise err

        else:
            print("all jobs are on hold")


    @db.connect('meta')
    def release_all(self, db_instance=None):

        self.command()
        try:
            for item in self.sql_release_queue:
                db_instance.session.execute(item)
                db_instance.session.commit()
        except Exception as err:
            db_instance.session.rollback()
            raise err

        else:
            print("all jobs are released")


if __name__ == '__main__':

    hold_release().hold_all()
    hold_release().release_all()
    exit(0)

    args = get_parser()
    if args.request == 'hold':
        hold_release().job_hold()
    elif args.request == 'release':
        hold_release().job_release()

    hold_release().command()
