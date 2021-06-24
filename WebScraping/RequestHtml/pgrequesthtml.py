import hashlib
import inspect
import requests_html
from datetime import datetime
from time import mktime
from collections import deque
from pprint import pprint
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from newscatcher import Newscatcher
from newscatcher import urls as newscatcher_urls
from Data.Utils import pgparse
from WebScraping import pgwebscrapingbase, pgwebscrapingcommon
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation

"""
>>> from requests_html import AsyncHTMLSession
>>> asession = AsyncHTMLSession()

>>> async def get_pythonorg():
...    r = await asession.get('https://python.org/')

>>> async def get_reddit():
...    r = await asession.get('https://reddit.com/')

>>> async def get_google():
...    r = await asession.get('https://google.com/')

>>> session.run(get_pythonorg, get_reddit, get_google)

"""


__version__ = "1.7"

#deque(map(self.do_someting, native_object))


class PGRequestHtml(pgwebscrapingbase.PGWebScrapingBase, pgwebscrapingcommon.PGWebScrapingCommon):
    def __init__(self, project_name: str = "requesthtml", logging_enable: str = False):

        super(PGRequestHtml, self).__init__(project_name=project_name,
                                            object_short_name="PG_WS_RH",
                                            config_file_pathname=__file__.split('.')[0] + ".ini",
                                            logging_enable=logging_enable,
                                            config_file_type="ini")

        ### Common Variables
        self._name = "requesthtml"
        self._data = {}
        self._pattern_match = {}
        self._parameter = {}



        ### Specific Variables
        self._client = requests_html.HTMLSession()
        self._async_client = requests_html.AsyncHTMLSession()
        self._base_url = None
        self._response = None


        """
        def step1(city: str, state_abbr: str, page_num_func: Callable):
            url_base = "https://www.realtor.com/soldhomeprices/"

            for pn in range(1, page_num_func()):
                page_num = f"/pg-{pn}" if pn >= 2 else ""
                uri = f"{city}_{state_abbr}{page_num}"
                print(f"{url_base}{uri}")

                r = s.get(f"{url_base}{uri}")
                print(r.text)

                exit(0)
                for item in a:
                    re.search(f"Photo of (.*)' class=\('js-srp-listing-photos", str(item)).group(1)
                print(list(map(lambda x: re.search(f"{prefix}(.*){surfix}", str(x)).group(1), r.html.find(sel))))
                filename = f"addr_{city}_{state_abbr}_{pn}_{pgfile.get_random_string()}.txt"

                with open(f"{pgdirectory.add_splash_2_dir(output_dir)}{filename}", 'w') as file:
                    for addr in list(
                            map(lambda x: re.search(f"{prefix}(.*){surfix}", str(x)).group(1), r.html.find(sel))):
                        file.write(f"{addr}\n")
        """

        ### setting default


    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    @property
    def process(self) -> Callable:
        return self._process

    def get_all_link(self):
        try:
            if self._response:
                return self._response.html.absolute_links

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

    @pgoperation.pg_retry(3)
    def get_response(self, url: str = None) -> bool:
        _base_url = url or self._base_url
        print(_base_url)
        try:
            if _base_url:
                # method – method for the new Request object.
                # url – URL for the new Request object.
                # params – (optional) Dictionary or bytes to be sent in the query string for the Request.
                # data – (optional) Dictionary, list of tuples, bytes, or file-like object to send in the body of the Request.
                # json – (optional) json to send in the body of the Request.
                # headers – (optional) Dictionary of HTTP Headers to send with the Request.
                # cookies – (optional) Dict or CookieJar object to send with the Request.
                # files – (optional) Dictionary of 'filename': file-like-objects for multipart encoding upload.
                # auth – (optional) Auth tuple or callable to enable Basic/Digest/Custom HTTP Auth.
                # timeout (float or tuple) – (optional) How long to wait for the server to send data before giving up, as a float, or a (connect timeout, read timeout) tuple.
                # allow_redirects (bool) – (optional) Set to True by default.
                # proxies – (optional) Dictionary mapping protocol or protocol and hostname to the URL of the proxy.
                # stream – (optional) whether to immediately download the response content. Defaults to False.
                # verify – (optional) Either a boolean, in which case it controls whether we verify the server’s TLS certificate, or a string, in which case it must be a path to a CA bundle to use. Defaults to True.
                # cert – (optional) if String, path to ssl client cert file (.pem). If Tuple, (‘cert’, ‘key’) pair.
                _header = {'accept': "*/*",
                           'accept-encoding': "gzip, deflate, br",
                           'accept-language': "en-US,en;q=0.9",
                           'referer': "https://www.google.com/",
                           'User-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36"
                           }

                _response = self._client.get(url=_base_url, headers=_header, proxies=None, timeout=100)
                if self.parse_response(_response.text):
                    print(_response)
                    self._response = _response
                    return True
                else:
                    return False

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False


    def get_tasks(self):
        return self.get_urls()

    def set_settings(self, topic=None, country=None, language=None) -> bool:
        try:
            if "topic" in self._config.parameters["config_file"]['default']:
                self.set_param(**{'topic': self._config.parameters["config_file"]['default']['topic']})

            if "language" in self._config.parameters["config_file"]['default']:
                self.set_param(**{'language': self._config.parameters["config_file"]['default']['language']})

            if "country" in self._config.parameters["config_file"]['default']:
                self.set_param(**{'country': self._config.parameters["config_file"]['default']['country']})

            _topic = topic or self._topic or 'tech'
            _country = country or self._country or 'US'
            _language = language or self._language or 'EN'

            self.set_param(**{'country': _country, 'topic': _topic, 'language': _language})

            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def get_urls(self, topic=None, country=None, language=None) -> list:
        try:
            self.set_settings(topic=topic, country=country, language=language)
            return newscatcher_urls(topic=self.topic, country=self.country, language=self.language)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return []

    #@pgoperation.pg_retry(3)
    def _process_item(self, link: str) -> bool:
        try:
            print(f"link: {link}")
            nc = Newscatcher(website=link).get_news()
            if nc is not None:
                for newsfeed in nc['articles']:
                    _tag = []
                    _tag += set(pgparse.pg_flatten_object(newsfeed['tags'])) if "tag" in newsfeed else []
                    _tag += set(pgparse.pg_flatten_object(newsfeed['authors'])) if "authors" in newsfeed else []
                    #_tag += [pgparse.pg_common_text_extract(newsfeed['summary'], {'prefix': "<p>", 'surfix': "</p>"})] if "summary" in newsfeed else []

                    _id = str(newsfeed['id']) if "id" in newsfeed else hashlib.sha256(newsfeed['title'].encode()).hexdigest()
                    self._data[_id] = {'id': _id,
                                       'title': newsfeed['title'] if "title" in newsfeed else "na",
                                       'timestamp': str(datetime.fromtimestamp(mktime(newsfeed['published_parsed']))),
                                       'tags': _tag,
                                       'authors': list(pgparse.pg_flatten_object(newsfeed['authors'])) if "authors" in newsfeed else "na",
                                       'summary': pgparse.pg_common_text_extract(newsfeed['summary'],
                                                                                 {'prefix': "<p>",
                                                                                  'surfix': "</p>"}) if "summary" in newsfeed else "na",
                                       'link': newsfeed['link'] if "link" in newsfeed else "na"
                                      }
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        try:
            if isinstance(iterable, (tuple, list)):
                for _link in iterable:
                    self._process_item(_link)
            elif isinstance(iterable, str):
                self._process_item(iterable)
            else:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                              f"Unexpected type for iterable, expecting tuple, list, str, got {type(iterable)}")

                return False
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


class PGRequestHtmlExt(PGRequestHtml):
    def __init__(self, project_name: str = "requesthtmlext", logging_enable: str = False):
        super(PGRequestHtmlExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGRequestHtmlSingleton(PGRequestHtml):

    __instance = None

    @staticmethod
    def get_instance():
        if PGRequestHtmlSingleton.__instance == None:
            PGRequestHtmlSingleton()
        else:
            return PGRequestHtmlSingleton.__instance

    def __init__(self, project_name: str = "newscatchersingleton", logging_enable: str = False):
        super(PGRequestHtmlSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGRequestHtmlSingleton.__instance = self


if __name__ == '__main__':
    test = PGRequestHtml()
    test.set_param(**{'base_url': "https://www.realtor.com/soldhomeprices/Johns-Creek_GA"})
    test.get_response()

    print(test._response)





