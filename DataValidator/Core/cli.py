import argparse
import logging
import sys

_version_ = 0.1


class CLI(object):
    def __init__(self):
        pass

    @classmethod
    def data_validator_parser(cls, dag_parser=False):
        try:
            logging.debug('defining argparse arguments')

            argparser = argparse.ArgumentParser(description='Data Validator')
            argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                                   help='show current version')
            argparser.add_argument('-s', '--source', action='store', dest='source_table', required=True,
                                   help='Source table which will be checking against ')
            argparser.add_argument('-t', '--target', action='store', dest='target_table', required=True,
                                   help='Target table which will be performing data verification on')
            argparser.add_argument('-m', '--mode', action='store', dest='mode_flag', required=True,
                                   help='Target table which will be performing data verification on')
            argparser.add_argument('-a', '--auto', action='store', dest='auto_flag',
                                   help='auto mode, gets a list of tables from metadata db')
            argparser.add_argument('-k', '--pk', action='store', dest='key_col',
                                   help='primary key for the table')
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

        return args


def data_validator_parser():
    return CLI.data_validator_parser()
