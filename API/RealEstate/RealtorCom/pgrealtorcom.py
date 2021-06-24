import re
import asyncio
import json
import uuid
import inspect
from requests_html import HTMLSession
from typing import Union, Tuple, Callable, Iterable, Dict
from types import SimpleNamespace
from redfin import Redfin
from Data.Utils import pgyaml
from Data.Utils import pgdirectory
from Meta import pggenericfunc
from Data.Utils import pgoperation
from Meta import pgclassdefault
from API.RealEstate import pgrealestatebase
from pprint import pprint
from pyppeteer import launch
from pyppeteer_stealth import stealth


__version__ = "1.7"

#r = s.get('https://www.realtor.com/soldhomeprices/Duluth_GA')
#prefix = "Photo of "
#surfix = "' class=\('js-srp-listing-photos'"


async def get_data(url: str) -> str:
    browser = await launch(headless=True)
    page = await browser.newPage()
    await stealth(page)  # <-- Here
    await page.goto(url)
    # await page.screenshot({"path": "realtor.png", "fullPage": True})
    html = await page.content()
    await browser.close()
    return html


class PGRealtorComWeb(pgrealestatebase.PGRealestateBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str = "realtorcomweb", logging_enable: str = False):

        super(PGRealtorComWeb, self).__init__(project_name=project_name,
                                              object_short_name="PG_RCW",
                                              config_file_pathname=__file__.split('.')[0] + ".ini",
                                              logging_enable=logging_enable,
                                              config_file_type="ini")

        ### Common Variables
        self._name = "realtorcomweb"
        self._data = {}
        self._pattern_match = {'address_parser': {'prefix': "Photo of ",
                                                  'surfix': "' class=\('js-srp-listing-photos'"
                                                  }
                               }

        ### Specific Variables
        self._url_base = "https://www.realtor.com/soldhomeprices/"
        self._selector = 'img.js-srp-listing-photos.main-photo'
        self._response = None
        self._client = HTMLSession()
        self._city_name = None
        self._state_abbr = None
        self._matching_text = []



    @staticmethod
    def page_number():
        page_num = 38 + 1
        return page_num

    def data_extract(self, input_string: str, method_cnt: str) -> str:
        method_dict = {'0': re.search(f"{self._pattern_match['address_parser']['prefix']}(.*){self._pattern_match['address_parser']['surfix']}", input_string).group(1),
                       }
        try:
            print(method_dict.get(method_cnt, "no more methods"))
            return method_dict.get(method_cnt, "no more methods")
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

    def get_tasks(self):
        try:
            if self._response:
                for item in self._response.html.find(self._selector):
                    self._matching_text.append(item)

                if self._matching_text:
                    return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def process(self) -> Callable:
        return self._process

    def _process(self, iterable: dict = {}, *args: object, **kwargs: object) -> bool:
        try:

            _city_name = iterable['city_name'] if iterable and "city_name" in iterable else self._city_name
            _state_abbr = iterable['state_abbr'] if iterable and "state_abbr" in iterable else self._state_abbr

            print(_city_name)
            print(_state_abbr)
            for pn in range(1, self.page_number()):
                _uri_page_num = f"/pg-{pn}" if pn >= 2 else ""
                uri = f"{_city_name}_{_state_abbr}{_uri_page_num}"


                print(f"{self._url_base}{uri}")

                self._response = self._client.get(f"{self._url_base}{uri}")
                html = asyncio.get_event_loop().run_until_complete(get_data(f"{self._url_base}{uri}"))
                self._response._text = html

                print(self._response.text)

                if not self._matching_text:
                    self.get_tasks()

                print(self._matching_text)
                exit(0)
                if self._matching_text:
                    for _matching_string in self._matching_text:

                        method_cnt = 0
                        while True:
                            _address = self.data_extract(input_string=_matching_string, method_cnt=str(method_cnt).strip())
                            print(_address)

                            if _address:
                                break
                            method_cnt += 1

                        self._data[str(uuid.uuid4().hex[:8])] = _address

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False


"""
def step1(city: str, state_abbr: str, page_num_func: Callable):
    url_base = "https://www.realtor.com/soldhomeprices/"

    for pn in range(38, page_num_func()):
        page_num = f"/pg-{pn}" if pn >= 2 else ""
        uri = f"{city}_{state_abbr}{page_num}"
        r = self._client.get(f"{url_base}{uri}")
        print(list(map(lambda x: re.search(f"{prefix}(.*){surfix}", str(x)).group(1), r.html.find(sel))))
        filename = f"addr_{city}_{state_abbr}_{pn}_{pgfile.get_random_string()}.txt"

        with open(f"{pgdirectory.add_splash_2_dir(output_dir)}{filename}", 'w') as file:
            for addr in list(map(lambda x: re.search(f"{prefix}(.*){surfix}", str(x)).group(1), r.html.find(sel))):
                file.write(f"{addr}\n")
"""


class PGRealtorCom(pgrealestatebase.PGRealestateBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str = "realtorcom", logging_enable: str = False):

        super(PGRealtorCom, self).__init__(project_name=project_name,
                                           object_short_name="PG_RF",
                                           config_file_pathname=__file__.split('.')[0] + ".ini",
                                           logging_enable=logging_enable,
                                           config_file_type="ini")

        ### Common Variables
        self._name = "realtorcom"
        self._data = {}
        self._pattern_match = {'address_parser': {'prefix': "",
                                                  'surfix': " \d{5}"
                                                  }
                              }

        ### Specific Variables


    @property
    def name(self):
        return self._name

    def get_tasks(self):
        try:
            pass

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return True

    def process(self) -> Callable:
        return self._process

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:

        try:
            pass

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return True


class PGRealtorComExt(PGRealtorCom):
    def __init__(self, project_name: str = "redfinext", logging_enable: str = False):
        super(PGRealtorComExt, self).__init__(project_name=project_name, logging_enable=logging_enable)


class PGRealtorComWebSingleton(PGRealtorComWeb):

    __instance = None

    @staticmethod
    def get_instance():
        if PGRealtorComWebSingleton.__instance == None:
            PGRealtorComWebSingleton()
        else:
            return PGRealtorComWebSingleton.__instance

    def __init__(self, project_name: str = "pgrealtorcomwebsingleton", logging_enable: str = False):
        super(PGRealtorComWebSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGRealtorComWebSingleton.__instance = self


if __name__ == '__main__':
    test = PGRealtorComWeb()
    test.set_param(**{'city_name': "John-Creek", 'state_abbr': 'GA'})
    print(test._city_name)
    test._process()
    print(test._data)
