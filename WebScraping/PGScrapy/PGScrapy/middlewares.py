# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import asyncio
import logging
import inspect
from scrapy import signals
from scrapy.http import HtmlResponse
from pyppeteer import launch
from WebScraping.PGScrapyExt.PGScrapyDownloader import pgpyppeteer
from WebScraping.PGScrapyExt.PGScrapyDownloader import pgstealth

from Meta import pggenericfunc

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

            return self.loop.run_until_complete(pgstealth.PGScrapyStealth().process(request, spider))
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        print("I'm here 5555555")
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
        print(f"in process_request: {request}")

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





"""
import os
import logging
import warnings
from collections import defaultdict
#from six.moves.urllib.parse import urlparse
#from w3lib.http import basic_auth_header
from scrapy import signals
from scrapy.resolver import dnscache
from scrapy.exceptions import ScrapyDeprecationWarning
#from twisted.internet.error import ConnectionRefusedError, ConnectionDone

    async def _process_request(self, request, spider):
        options = {
            'headless': False,
        }
        browser = await launch(options)
        page = await browser.newPage()
        response = await page.goto(
            request.url,
            options={
                'timeout': 20000,
            }
        )

        content = await page.content()
        body = str.encode(content, 'utf-8')
        await page.close()
        await browser.close()

        return HtmlResponse(
            page.url,
            status=response.status,
            headers=response.headers,
            body=body,
            encoding='utf-8',
            request=request
        )
"""