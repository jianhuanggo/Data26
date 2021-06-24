
import argparse
import logging
import sys
import Data.Utils.StrFunc as StrFunc
import socket

_version_ = 0.1


class CLI(object):
    def __init__(self):
        pass

    @classmethod
    def get_parser(cls, dag_parser=False):
        try:
            logging.debug('defining argparse arguments')

            argparser = argparse.ArgumentParser(description='JDaemon does all the heavy lifting')
            argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                                   help='show current version')
            argparser.add_argument('request', action='store', choices=['start', 'stop', 'restart', 'status'],
                                   help='start, stop, restart, or check status on the daemon')
            argparser.add_argument('-t', '--time_interval', action='store', type=int, dest='polling_interval',
                                   default=90,
                                   help='seconds to sleep between polling attempts')
            argparser.add_argument('-i', '--daemon_id', action='store', type=int, dest='daemon_id', default=0,
                                   help='integer id for the daemon (default=0)')
            argparser.add_argument('-d', '--debug', action='store_true', dest='debug', default=False,
                                   help='emit debug messages')
            argparser.add_argument('-y', '--worker_type', action='store', choices=['worker',
                                                                                   'watcher',
                                                                                   'jobdispatcher',
                                                                                   'sweeper',
                                                                                   'dataprofiler'],
                                   required=True, dest='daemon_type', help='type of daemon')

            logging.debug('parsing argparser arguments')
            args = argparser.parse_args()

            if args.debug:
                loggingLevel = logging.DEBUG
                logging.basicConfig(level=loggingLevel)

        except Exception as err:
            logging.critical("Error creating/parsing arguments:\n%s" % str(err))
            sys.exit(100)

        # hostname may contain dots, these are to be replaced by underscores
        args.daemon_name = "{0}_daemon_{1}".format(StrFunc.wordreplace(socket.gethostname(), '.', '_'),
                                                    str(args.daemon_id))
        logging.debug('args.daemon_name: {0}'.format(args.daemon_name))

        #print(args.daemon_type)
        return args


def get_parser():
    return CLI.get_parser()
