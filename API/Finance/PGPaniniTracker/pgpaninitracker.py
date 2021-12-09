import time
import json
import asyncio
import logging
import inspect
from types import SimpleNamespace
from pyppeteer import launch
from scrapy.http import HtmlResponse
from pyppeteer_stealth import stealth
from Meta import pggenericfunc
from WebScraping import pgwebscrapingcommon, pgscrapybase
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from Data.Utils import pgfile

logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('pyppeteer').setLevel(logging.WARNING)

"""
Update chromoedriver 
1) download appropriate driver from https://chromedriver.chromium.org/downloads
2) rename the file to /Users/jianhuang/chromedriver
3) give permission to run:  xattr -d com.apple.quarantine /Users/jianhuang/chromedriver
"""

__version__ = "1.8"

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

    def pg_dump(self, entity_name: str, pg_data):
        with open(f"/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/{entity_name}.txt", 'w') as file:
            file.write(pg_data)

    def process(self, request, spider=None):
        return self._process_request_stealth(request, spider)

    def _process_request_selenium(self, request, spider=None):
        def pg_time_delay(pg_driver, pg_timeout: int):
            try:
                element = WebDriverWait(pg_driver, pg_timeout).until(
                    EC.presence_of_element_located((By.ID, "myDynamicElement"))
                )
            except Exception as err:
                time.sleep(pg_timeout)

        def get_status(logs):
            for log in logs:
                if log['message']:
                    d = json.loads(log['message'])
                    try:
                        content_type = 'text/html' in d['message']['params']['response']['headers']['content-type']
                        response_received = d['message']['method'] == 'Network.responseReceived'
                        if content_type and response_received:
                            return d['message']['params']['response']['status']
                    except Exception as err:
                        pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        try:
            capabilities = DesiredCapabilities.CHROME.copy()
            capabilities['goog:loggingPrefs'] = {'performance': 'ALL'}

            browser = WebDriver('/Users/jianhuang/chromedriver', desired_capabilities=capabilities)

            print(request.url)
            browser.get(request.url)
            pg_time_delay(browser, 30)

            #browser.implicitly_wait(30)
            logs = browser.get_log('performance')
            print(logs)
            self.pg_dump(pgfile.get_random_filename("selenium"), browser.page_source)
            print(browser.page_source)
            #print(get_status(logs))

            #_driver = webdriver.Chrome('/Users/jianhuang/chromedriver', options=disable_alert())
            #_elements = {}
            #_driver.get(request.url)
            #print(_driver.page_source)
            #print(f"\n\n\n\n\n{_driver.headers}")
            exit(0)
            """
            body = str.encode(_driver.page_source)

            return HtmlResponse(request.url,
                                status=response.status,
                                headers=,
                                body=body,
                                encoding='utf-8',
                                request=request)
            """
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    async def _process_request_stealth(self, request, spider=None):
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
            #print(values)
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
    def run_stealth(self, request):
        _result = asyncio.get_event_loop().run_until_complete(self.process(request))
        self.pg_dump("stealth", _result.body.decode('utf-8'))

    def run_selenium(self, request):
        return self._process_request_selenium(request)
        #return asyncio.get_event_loop().run_until_complete(self.process(request))


if __name__ == '__main__':
    test = PGScrapyStealth()
    #_url = "https://www.realtor.com/soldhomeprices/Suwanee_GA"
    #_url = "https://wax.atomichub.io/market?collection_name=mlb.topps&order=asc&sort=price&symbol=WAX"

    #_url = "https://www.paniniamerica.net/blockchain/public-auctions/public-auctions/public-auctions.html?sortBy=end_time&p=2&sport=Football"

    #_url = "https://www.paniniamerica.net/blockchain/public-auctions/public-auctions/public-auctions.html?sport=Football&p=1&sortBy=priceL"

    #_url = "https://www.realtor.com/soldhomeprices/Alpharetta_GA/pg-2"
    #_url = "https://www.paniniamerica.net/blockchain/public-auctions/sales/recent-sales.html"

    #_url = "https://www.indeed.com/jobs?q=aws&fromage=3&remotejob=032b3046-06a3-4876-8dfd-474eb5e7ed11&vjk=c255d87b8462a684"

    #_url = "https://opensea.io/assets?search[sortAscending]=false&search[sortBy]=LAST_SALE_DATE"

    _url = "https://www.forbes.com/fintech/2021/#9b6b99531a63"


    _request = SimpleNamespace(url=_url)
    #_result = test.run_stealth(_request)
    _result = test.run_selenium(_request)
    #print(_result.body)

    exit(0)

    for i in range(11, 12):
        url = f"{_url}/pg-{i}" if i >= 2 else _url
        _request = SimpleNamespace(url=url)
        html = asyncio.get_event_loop().run_until_complete(test.process(_request))
        print(f"got data from {url}")
        time.sleep(2)
        with open(f"Suwanee-realtor-{i}.html", 'w') as file:
            file.write(html)

