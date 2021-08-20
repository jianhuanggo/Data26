import time
import asyncio
import logging
import inspect
from types import SimpleNamespace
from pyppeteer import launch
from scrapy.http import HtmlResponse
from pyppeteer_stealth import stealth
from Meta import pggenericfunc
from WebScraping import pgwebscrapingcommon, pgscrapybase
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator

logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('pyppeteer').setLevel(logging.WARNING)

_RESULT="SUCCESS"
#https://javascriptwebscrapingguy.com/avoid-being-blocked-with-puppeteer/
#Best implementation: JavaScript


class PGScrapyStealth(pgscrapybase.PGScrapyBase, pgwebscrapingcommon.PGWebScrapingCommon):
    def __init__(self, project_name: str = "scrapystealth", logging_enable: str = False):
        super(PGScrapyStealth, self).__init__(project_name=project_name,
                                              object_short_name="PG_SCPY_STLH",
                                              config_file_pathname=__file__.split('.')[0] + ".ini",
                                              logging_enable=logging_enable,
                                              config_file_type="ini")

        ### Common Variables
        self._name = "scrapystealth"
        self._data = {}
        self._pattern_match = {}
        self._parameter = {}

        ### Specific Variables
        #if not pgdirectory.createdirectory(self._parameter['default_dirpath']):
        #    exit(1)


    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    def process(self, request, spider=None):
        return self._process_request(request, spider)

    async def _process_request(self, request, spider=None):
        try:
            browser = await launch(headless=True)
            page = await browser.newPage()
            page.setDefaultNavigationTimeout(0)

            await stealth(page)  # <-- Here
            response = await page.goto(request.url)
            # await page.screenshot({"path": "realtor.png", "fullPage": True})
            _content = await page.content()
            values = await page.evaluate('''() => [...document.querySelectorAll('.table')]
                               .map(element => element.getAttribute('data-options'))
                ''')
            body = str.encode(_content)
            await page.close()
            await browser.close()

            return HtmlResponse(page.url,
                                status=response.status,
                                headers=response.headers,
                                body=body,
                                encoding='utf-8',
                                request=request)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    # @pgoperation.pg_retry(3)


if __name__ == '__main__':
    test = PGScrapyStealth()
    _url = "https://www.realtor.com/soldhomeprices/Suwanee_GA"
    for i in range(11, 12):
        url = f"{_url}/pg-{i}" if i >= 2 else _url
        _request = SimpleNamespace(url=url)
        html = asyncio.get_event_loop().run_until_complete(test.process(_request))
        print(f"got data from {url}")
        time.sleep(2)
        with open(f"Suwanee-realtor-{i}.html", 'w') as file:
            file.write(html)

