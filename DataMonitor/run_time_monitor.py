import logging
import argparse
import sys
from Data.Utils import pgprocess
from pprint import pprint

_version_ = 0.5


sql_tag = {
    "job_failure": "sql/failed_job.sql",
    "run_time_na": "sql/run_time_na.sql",
    "run_time_sa": "sql/run_time_sa.sql",
    "run_time_eu": "sql/run_time_eu.sql",
    "longest_job": "sql/longest_job.sql"
}


sql_tag_comments = {
    "job_failure": "find failed jobs across all 3 regions",
    "run_time_na": "fetch latest job runs and display job duration for NA region",
    "run_time_sa": "fetch latest job runs and display job duration for SA region",
    "run_time_eu": "fetch latest job runs and display job duration for EU region",
    "longest_job": "fetch the longest job from each region"
}


class MyParser(argparse.ArgumentParser):

    def __init__(self):
        super().__init__()
        self.args = None

    def get_parser(self):
        try:
            logging.debug('defining argparse arguments')

            argparser = argparse.ArgumentParser(description='Run Time Monitor Commands:')
            argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                                   help='show current version')
            argparser.add_argument('-f', '--sqlfilename', action='store', dest='sql_filename',
                                   required=False, help='The name of sql to be run')
            argparser.add_argument('-t', '--database', action='store', choices=['job_failure', 'run_time_na',
                                                                                'run_time_sa',
                                                                                'run_time_eu',
                                                                                'longest_job'],
                                   required=False, dest='sql_tag', help='SQL tags, each tag attach to a query logic')
            argparser.add_argument('-list', action='store_true',
                                   required=False, help='list the comments abt each SQL tag')

            logging.debug('parsing argparser arguments')

            if len(sys.argv) == 1:
                argparser.print_help(sys.stderr)
                sys.exit(1)

            self.args = argparser.parse_args()

        except Exception as err:
            logging.critical("Error creating/parsing arguments:\n%s" % str(err))
            sys.exit(100)
        else:
            return self.args

    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


if __name__ == '__main__':

    args = MyParser().get_parser()

    if args.list:
        pprint(sql_tag_comments)
        exit(0)
    if args.sql_filename:
        filename = args.sql_filename
    else:
        filename = sql_tag[args.sql_tag]

    pgprocess.wrap_run_query_v2_from_file(db_system='meta', filepath=filename)
