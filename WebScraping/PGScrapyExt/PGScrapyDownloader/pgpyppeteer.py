from pyppeteer import launch

from scrapy.http import HtmlResponse
import asyncio
import inspect
import logging
from Meta import pggenericfunc
from WebScraping import pgwebscrapingcommon, pgscrapybase
from WebScraping import scrapyitem_pb2

logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('pyppeteer').setLevel(logging.WARNING)


class PGScrapyPpeteer(pgscrapybase.PGScrapyBase, pgwebscrapingcommon.PGWebScrapingCommon):
    def __init__(self, project_name: str = "scrapyppeteer", logging_enable: str = False):
        super(PGScrapyPpeteer, self).__init__(project_name=project_name,
                                              object_short_name="PG_SCPY_PPET",
                                              config_file_pathname=__file__.split('.')[0] + ".ini",
                                              logging_enable=logging_enable,
                                              config_file_type="ini")

        ### Common Variables
        self._name = "scrapyppeteer"
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
            options = {'headless': False}
            browser = await launch(options)
            page = await browser.newPage()
            response = await page.goto(
                request.url,
                options={
                    'timeout': 300000,
                }
            )
            content = await page.content()
            values = await page.evaluate('''() => [...document.querySelectorAll('.table')]
                               .map(element => element.getAttribute('data-options'))
                ''')
            body = str.encode(content, 'utf-8')
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


if __name__ == '__main__':
    test = PGScrapyPpeteer()
    test._item.id = "test"
    test.seralize_item("/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/WebScraping/PGScrapyExt/PGScrapyDownloader/test.pb")

    test1 = PGScrapyPpeteer()
    test1.deseralize_item("/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/WebScraping/PGScrapyExt/PGScrapyDownloader/test.pb")
    print(test1._item.id)