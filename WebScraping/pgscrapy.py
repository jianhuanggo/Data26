import uuid
import scrapy
from scrapy.crawler import CrawlerProcess
from pprint import pprint
from Meta import pgclassdefault
import Data.Utils.pgdirectory as pgdirectory
import Data.Logging.pglogging as pglogging
import json


class PGScrapy(pgclassdefault.PGClassDefault, scrapy.Spider):
    name = 'pgscrapy'
    start_urls = ['https://free-proxy-list.net']

    def __init__(self, project_name="pgscrapy" + str(uuid.uuid4().hex[:6]), logging_enable=False):
        super(PGScrapy, self).__init__(project_name=project_name,
                                       object_short_name="PG_SCY",
                                       config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml",
                                       logging_enable=logging_enable)
        #self.parse = self.get_proxy


    @classmethod
    def set_parse(cls, func_name: str) -> bool:
        try:
            parse = func_name
            return True
        except Exception as err:
            return False

    @staticmethod
    def parse(response):
        _start_urls = ['https://free-proxy-list.net']
        _headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'cookie': 'CONSENT=YES+UA.en+; VISITOR_INFO1_LIVE=7woZWjMqhVc; _ga=GA1.2.309153073.1579374852; endscreen-gh=true; LOGIN_INFO=AFmmF2swRAIgFIayDoYF3kfp-7bTpN-P6yJhJi5t3Ze1R5LSxZZobPgCIDL02ytlRy0-sYgFNNys8riGkzj0yZy6H3vM0AJL78m6:QUQ3MjNmelZGZjFPSDQ1X0oyd1d2MWRBR2YtVnE2Y0NyTHJ4WGlxTUZEQTVDV3hSQkFOU1BsaHZ5U2pSbW91dy10bXNJbFFMYTEwMGozNGhwSFlkalk2UllmUlA3RzBWZU1hNlFOVEI3R2hCcUNtZEg1SUluLVl5aFdydGY2WjVZcTNmWWpYSnQ2VnFRakNWTzZxanJISnd1Z1NXRUdMNWNVMXMxR1dDSzk3c0tfRVl5NmxEWGNENnd2blFoM0dwbG9JNUNwaTBfTXN5; SID=tQd3UMX__-JBh21ggvPrm0ieZEIsHFK2JKa9ZpZkxtu34SMWdUJpiPRvIzHkFkNY2UYvSg.; __Secure-3PSID=tQd3UMX__-JBh21ggvPrm0ieZEIsHFK2JKa9ZpZkxtu34SMWBwgcJXiZNemGkh6QqMdtXw.; HSID=AuTpyVP56bcmNUc98; SSID=AhEjxf-UdRF8iDxyG; APISID=Ldjz1ogvjgtip72f/AxXApI-7pppAvz-A1; SAPISID=a-Mi48PHKTvsLcTU/A0E7y0fhZox6pSbKQ; __Secure-HSID=AuTpyVP56bcmNUc98; __Secure-SSID=AhEjxf-UdRF8iDxyG; __Secure-APISID=Ldjz1ogvjgtip72f/AxXApI-7pppAvz-A1; __Secure-3PAPISID=a-Mi48PHKTvsLcTU/A0E7y0fhZox6pSbKQ; PREF=al=ru&f5=30&hl=en; _gid=GA1.2.274668459.1581758680; YSC=_ieg6bWQ2nY; SIDCC=AN0-TYt6S9EpxlP7-RWM2TgfMPte97mp0wubDjp80YgitYbYNyDVlRTluLbK3ierJ91mJCb8adLm',
            'pragma': 'no-cache',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
        }
        table = response.css('table')
        rows = table.css('tr')
        cols = [row.css('td::text').getall() for row in rows]

        proxies = []

        for col in cols:
            if col and col[4] == 'elite proxy' and col[6] == 'yes':
                proxies.append('https://' + col[0] + ':' + col[1])

        pprint(proxies)

    def parse_1(self, response):

        proxies = []
        print('proxies:', len(proxies))

        for proxy in proxies:
            test_url = 'https://scrapingkungfu.herokuapp.com/api/request'

            # use your video url here
            video_url = 'https://www.youtube.com/watch?v=PZYHR64117Q'

            yield scrapy.Request(video_url, dont_filter=True, headers=self.headers, meta={'proxy': proxy},
                                 callback=self.check_response)

    def check_response(self, response):
        print('\n\nRESPONSE:', response.status)




if __name__ == '__main__':
    # run spider
    process = CrawlerProcess()
    #Test = PGScrapy_old("test")
    print(isinstance(PGScrapy, scrapy.Spider))
    process.crawl(PGScrapy)
    process.start()
