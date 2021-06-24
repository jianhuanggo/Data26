import argparse
import logging
import sys
import socket

_version_ = 0.1


class CLI(object):
    def __init__(self):
        pass

    @classmethod
    def get_get_table_mover_parser(cls, dag_parser=False):
        try:
            logging.debug('defining argparse arguments')

            argparser = argparse.ArgumentParser(description='Data mover')
            argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                                   help='show current version')
            argparser.add_argument('-s', '--source', action='store', dest='source_table', required=True,
                                   help='Source table to be extracted ')
            argparser.add_argument('-t', '--target', action='store', dest='target_table', required=True,
                                   help='Target table to be loaded')
            argparser.add_argument('-m', '--stage', action='store', dest='stage_table', required=True,
                                   help='Stage table to be loaded')
            argparser.add_argument('-i', '--id', action='store', dest='key_col', required=True,
                                   help='key col of the table')
            argparser.add_argument('-d', '--debug', action='store', dest='debug', required=False,
                                   help='Target table to be loaded')

            logging.debug('Parsing argparser arguments')
            args = argparser.parse_args()

            if args.debug:
                loggingLevel = logging.DEBUG
                logging.basicConfig(level=loggingLevel)

        except Exception as err:
            logging.critical(f"Error creating/parsing arguments: {str(err)}\n")
            sys.exit(100)

        print(f"Moving data from {args.source_table} in RDS to {args.target_table} in Redshift")

        return args


def get_table_mover_parser():
    return CLI.get_get_table_mover_parser()
