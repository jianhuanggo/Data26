# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
import os
from pathlib import Path
import asyncio
from time import sleep
import logging
import inspect
from scrapy import signals
from scrapy.http import HtmlResponse
from pyppeteer import launch
from bs4 import BeautifulSoup
from pprint import pprint
from .items import PgscrapyItem, PgscrapyPastActivityItem
from pgscrapyext.pgscrapydownloader import pgpyppeteer
from pgscrapyext.pgscrapydownloader import pgstealth
from pgscrapyext.pgscrapydownloader import pgscrapydownloader
from pgscrapyext.pgscrapyformatter import pgscrapyformatter
# from pgscrapyext.pgscrapyformatter.pgscrapyformatter import pg_custom_paniniamerica_checker, \
#     pg_custom_paniniamerica_recent_sales_formatter, pg_custom_paniniamerica_activity_header, pg_custom_paniniamerica_activity_formatter
from Data.Utils import pgdirectory
#from pgmeta import pgmeta

logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('pyppeteer').setLevel(logging.WARNING)


class PyppeteerDownloaderMiddleware:

    def __init__(self, logger=None):
        self.loop = asyncio.get_event_loop()
        self._logger = logger

    def process_request(self, request, spider):
        try:
            return self.loop.run_until_complete(pgpyppeteer.PGScrapyPpeteer().process(request, spider))
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None


class PgStealthDownloaderMiddleware:

    def __init__(self, logger=None):
        self.loop = asyncio.get_event_loop()
        self._logger = logger

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        try:
            #scrapyitem_pb2.PGScrapyItem()

            print(request)
            return self.loop.run_until_complete(pgstealth.PGScrapyStealth().process(request, spider))

        except Exception as err:
            pgmeta.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        print("I'm here 666666")
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class PgWebScrapingJSDownloaderMiddleware:

    def __init__(self, logger=None):
        self.loop = asyncio.get_event_loop()
        self._logger = logger

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        try:

            #x = PgscrapyPastActivityItem()
            #Path(os.path.join(request.dirpath, request.filename))
            if Path.exists:
                return pgscrapydownloader.PGWebScrapingDownloaderExt().run_scrapy(request)

                #return self.loop.run_until_complete(pgscrapydownloader.PGWebScrapingDownloader().process("test"))

                #return self.loop.run_until_complete(pgstealth.PGScrapyStealth().process(request, spider))

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    #def process_response(self, response, spider):
    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest

        #print(request.dirpath)
        print("I'm here AAAAAAAA")
        #print(request.filename)
        #_content = response.body.decode('utf-8')

        #print(f"the content length is {len(response.request.output)}")
        #raise SystemExit("trying.........")

        """
            _soup = BeautifulSoup(response.body.decode('utf-8'), 'html.parser')
    
    
            #_entire_finding = _soup.find_all("div")
            #print(_entire_finding)
            #_entire_finding = _content.find_all("div")
    
            #print(response.body)
            #print(_entire_finding)
    
            pgformatter = pgscrapyformatter.PGWebScrapingFormatterExt()
            #print(os.path.join(request.dirpath, request.filename))
            #_parse_content = pgformatter.scrapy_run(_soup.find_all("div"), pg_custom_paniniamerica_checker, pg_custom_paniniamerica_recent_sales_formatter)
            _parse_content = pgformatter.scrapy_run(_soup.find_all("div"), request.pagetag, request.pagetag)
            response.content_json = _parse_content
            response.pageitem = request.pageitem
            
        """

        pgformatter = pgscrapyformatter.PGWebScrapingFormatterExt()
        _parse_content_lst = []
        print(f"directory path is {response.body.decode('utf-8')}")

        ### assume it is a file if the path is not a directory
        if pgdirectory.isdirectoryexist(response.body.decode('utf-8')):
            for filename in pgdirectory.files_in_dir(response.body.decode('utf-8')):
                print(filename)
                with open(os.path.join(response.body.decode('utf-8'), filename)) as file:
                    _soup = BeautifulSoup(file.read(), 'html.parser')
                    _parse_content_lst.append(pgformatter.scrapy_run(_soup.find_all("div"), request.pagetag, request.pagetag))
        else:
            with open(os.path.join(response.body.decode('utf-8'))) as file:
                _soup = BeautifulSoup(file.read(), 'html.parser')
                _parse_content_lst.append(
                    pgformatter.scrapy_run(_soup.find_all("div"), request.pagetag, request.pagetag))

        # for item in response.request.output:
        #     _soup = BeautifulSoup(item.decode('utf-8'), 'html.parser')
        #     _parse_content_lst.append(pgformatter.scrapy_run(_soup.find_all("div"), request.pagetag, request.pagetag))
        print(len(_parse_content_lst))

        #raise SystemExit("ending here3333333......")

        #_entire_finding = _soup.find_all("div")
        #print(_entire_finding)
        #_entire_finding = _content.find_all("div")

        #print(response.body)
        #print(_entire_finding)


        #print(os.path.join(request.dirpath, request.filename))
        #_parse_content = pgformatter.scrapy_run(_soup.find_all("div"), pg_custom_paniniamerica_checker, pg_custom_paniniamerica_recent_sales_formatter)

        response.content_json = _parse_content_lst
        response.pageitem = request.pageitem

        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class PgscrapySpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class PgscrapyDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
