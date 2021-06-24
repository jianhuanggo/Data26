import argparse
import logging
import sys
import socket
import json
from pprint import pformat

_version_ = 0.1


class CLI(object):
    def __init__(self):
        pass

    @classmethod
    def get_parser_producer(cls, dag_parser=False):
        try:
            logging.debug('defining argparse arguments')

            argparser = argparse.ArgumentParser(description='Kafka Producer Parser')
            argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                                   help='show current version')
            argparser.add_argument('-b', '--brokers', action='store', dest='brokers', default=':29092',
                                   help='Bootstrap Kafka Brokers ')
            argparser.add_argument('-t', '--topic', action='store', dest='topic', required=True,
                                   help='The name of Kafka Topic')
            argparser.add_argument('-d', '--debug', action='store_true', dest='debug', default=False,
                                   help='emit debug messages')
            logging.debug('Parsing argparser arguments')
            args = argparser.parse_args()

            if args.debug:
                loggingLevel = logging.DEBUG
                logging.basicConfig(level=loggingLevel)

        except Exception as err:
            logging.critical(f"Error creating/parsing arguments: {str(err)}\n")
            sys.exit(100)

        args.host_name = socket.gethostname()
        logging.debug("Starting Kafka Client from {args.hostname}")

        #{'bootstrap.servers': 'mybroker1,mybroker2'}

        print(args.host_name)
        args.conf = {'bootstrap.servers': args.brokers}
        print(args.conf)

        return args

    @classmethod
    def stats_cb(cls, stats_json_str):
        stats_json = json.loads(stats_json_str)
        print('\nKafka stats: {}\n'.format(pformat(stats_json)))

    @classmethod
    def get_parser_consumer(cls, dag_parser=False):
        try:
            logging.debug('defining argparse arguments')
            argparser = argparse.ArgumentParser(description='Kafka Consumer Parser')
            argparser.add_argument('-v', '--version', action='version',
                                       version='%(prog)s VERSION ' + str(_version_),
                                       help='show current version')
            argparser.add_argument('-b', '--broker', action='store', dest='broker', default=':29092',
                                       help='Bootstrap Kafka Brokers ')
            argparser.add_argument('-t', '--topics', action='store', dest='topic', required=True,
                                       help='The name of Kafka Topics')
            argparser.add_argument('-g', '--group', action='store', dest='group', required=True,
                                       help='The name of Kafka Consumer Group')
            argparser.add_argument('-s', '--stats', action='store', dest='stats', type=int,
                                   help='Enable client statistics at specified interval(ms)')
            argparser.add_argument('-d', '--debug', action='store_true', dest='debug', default=False,
                                       help='emit debug messages')

            logging.debug('Parsing argparser arguments')
            args = argparser.parse_args()

            if args.debug:
                loggingLevel = logging.DEBUG
                logging.basicConfig(level=loggingLevel)

        except Exception as err:
            logging.critical(f"Error creating/parsing arguments: {str(err)}\n")
            sys.exit(100)

        args.host_name = socket.gethostname()
        logging.debug("Starting Kafka Client from {args.hostname}")

        args.topics = []
        args.topics.append(args.topic)
        print(args.topics)

        print(args.host_name)

        args.conf = {'bootstrap.servers': args.broker, 'group.id': args.group, 'session.timeout.ms': 6000,
                'auto.offset.reset': 'earliest'}

        if args.stats:
            args.conf['stats_cb'] = CLI.stats_cb
            args.conf['statistics.interval.ms'] = int(args.stats)
        print(args.conf)

        return args


def get_parser_producer():
    return CLI.get_parser_producer()


def get_parser_consumer():
    return CLI.get_parser_consumer()
