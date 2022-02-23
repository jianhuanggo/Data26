# from scrapy.crawler import CrawlerProcess
# from pgscrapy.spiders.pgspider import PGScrapy
#
#
# process = CrawlerProcess()
# #process.crawl(PGScrapy, arg1=val1, arg2=val2)
# process.crawl(PGScrapy)
# process.start()
import collections

from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy.settings import Settings
import os
import inspect
import argparse
import pathlib
import Data.Utils.StrFunc as StrFunc
from Meta import pggenericfunc
from scrapy.utils.project import get_project_settings
from pgscrapy.spiders.pgspider import PGScrapy
from scrapy.crawler import CrawlerProcess
from Data.Utils import pgfile, pgdirectory, pgyaml

_version_ = 0.1


def _get_config():
    return pgyaml.yaml_load(yaml_filename=__file__.split('.')[0] + ".yml")


def _command_line():
    try:
        _config = _get_config()
        argparser = argparse.ArgumentParser(description='pgspider command line')
        argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(_version_),
                               help='show current version')
        # argparser.add_argument('request', action='store', choices=['start', 'stop', 'restart', 'status'],
        #                        help='start, stop, restart, or check status on the daemon')
        argparser.add_argument('-f', '--output_file', action='store', type=str, dest='pg_output_filename',
                               default="auto", help='pgsider default output filename')
        argparser.add_argument('-t', '--file_format', action='store', type=str, dest='pg_file_format', default="json",
                               help='pgsider default output file format')
        argparser.add_argument('-l', '--log_file', action='store', type=str, dest='pg_log_filename', default="auto",
                               help='pgsider default log file')
        argparser.add_argument('-d', '--data_dir', action='store', type=str, dest='pg_data_dir', default=_config.get("data_dir", ""),
                               help='pgsider default data directory')
        argparser.add_argument('-e', '--log_dir', action='store', type=str, dest='pg_log_dir', default=_config.get("log_dir", ""),
                               help='pgsider default log directory')
        argparser.add_argument('-i', '--input_filepath', action='store', type=str, dest='pg_input_filepath',
                               help='pgsider input yaml file location and filename')
        # argparser.add_argument('-y', '--worker_type', action='store', choices=['worker',
        #                                                                        'watcher',
        #                                                                        'jobdispatcher',
        #                                                                        'sweeper',
        #                                                                        'dataprofiler'],
        #                        required=True, dest='daemon_type', help='type of daemon')

        print('parsing argparser arguments')
        args = argparser.parse_args()
        return args
    except Exception as err:
        pggenericfunc.pg_error_logger(None, inspect.currentframe().f_code.co_name, err)
        return False


def run():
    try:
        args = _command_line()
        if args.pg_output_filename == "auto":
            args.pg_output_filename = f"{pgfile.get_random_filename('pgspider')}.json"

        if args.pg_log_filename == "auto":
            args.pg_log_filename = f"{''.join(args.pg_output_filename.split('.')[:-1])}.log"

        if not pgdirectory.isdirectoryexist(args.pg_data_dir):
            pggenericfunc.pg_error_logger(None, inspect.currentframe().f_code.co_name, f"directory {args.pg_data_dir} doesn't exist")
            return False
            #raise(f"directory {args.pg_data_dir} doesn't exist")

        if not pgdirectory.isdirectoryexist(args.pg_log_dir):
            pggenericfunc.pg_error_logger(None, inspect.currentframe().f_code.co_name,
                                          f"directory {args.pg_log_dir} doesn't exist")
            return False

        _pgspider_setting = get_project_settings()
        _pgspider_setting['FEED_FORMAT'] = args.pg_file_format
        #s['LOG_LEVEL'] = 'INFO'
        _pgspider_setting['FEED_URI'] = pathlib.Path(args.pg_data_dir, args.pg_output_filename)
        _pgspider_setting['LOG_FILE'] = pathlib.Path(args.pg_log_dir, args.pg_log_filename)
            #f"{args.pg_log_dir}/{args.pg_log_filename}"

        # print(_pgspider_setting['FEED_FORMAT'])
        # print(_pgspider_setting['FEED_URI'])
        # print(_pgspider_setting['LOG_FILE'])

        process = CrawlerProcess(_pgspider_setting)
        process.crawl(PGScrapy, input_yaml_file=args.pg_input_filepath)
        process.start()
    except Exception as err:
        pggenericfunc.pg_error_logger(None, inspect.currentframe().f_code.co_name, err)
        return False


if __name__ == "__main__":
    run()

# #spider = FollowAllSpider(domain='scrapinghub.com')
# spider = PGScrapy()
# crawler = Crawler(get_project_settings())
# #crawler.configure()
# crawler.crawl(spider)
# crawler.start()
# #log.start()
# reactor.run() # the script will block here
