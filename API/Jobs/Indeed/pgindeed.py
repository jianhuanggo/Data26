import time
import json
import asyncio
import logging
import inspect
from types import SimpleNamespace
from Meta import pggenericfunc
from API.Jobs import pgjobscommon, pgjobsbase
from indeed import IndeedClient
from Data.Utils import pgfile

__version__ = "1.8"

"""
pip install indeed

"""


class PGIndeed(pgjobsbase.PGJobsbase, pgjobscommon.PGJobsCommon):
    def __init__(self, project_name: str = "scrapystealth", logging_enable: str = False):
        super(PGIndeed, self).__init__(project_name=project_name,
                                       object_short_name="PG_SCPY_STLH",
                                       config_file_pathname=__file__.split('.')[0] + ".ini",
                                       logging_enable=logging_enable,
                                       config_file_type="ini")

        ### Common Variables
        self._name = "pgindeed"
        self._client = IndeedClient(publisher = ************3506)
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

    def get_offers(self, params):
        try:
            # loop through each offer element
            for elm in self._client.search(**params)['results']:
                # let's parse the offer
                offer = (elm['jobtitle'],
                         elm['formattedLocation'],
                         elm['snippet'],
                         elm['url'],
                         elm['indeedApply'],
                         elm['jobkey'],
                         elm['date'])
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None


    def get_search(self):
        parameters = {
            'q': "python developer",
            'l': "Austin, TX",
            'sort': "date",
            'fromage': "5",
            'limit': "25",
            'filter': "1",
            'userip': "192.186.176.550:60409",
            'useragent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2)"
        }
        self.get_offers(parameters)


    def process(self, request, spider=None):
        try:
            pass
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None


if __name__ == '__main__':
    test = PGScrapyStealth()

