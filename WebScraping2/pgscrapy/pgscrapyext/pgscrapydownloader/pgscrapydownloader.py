import os
import bs4
import inspect
from pathlib import Path
from pprint import pprint
import collections
import json
import pandas as pd
from scrapy.http import HtmlResponse
from Naked.toolshed.shell import execute_js
from bs4 import BeautifulSoup
from pydantic.generics import GenericModel
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List, Dict, AnyStr
#from pgmeta import pgclassdefault, pggenericfunc, pgmeta
from Meta import pgclassdefault, pggenericfunc
from Meta.pggenericfunc import check_args
from pgscrapyext.pgscrapydownloader import pgscrapydownloaderbase, pgscrapydownloadercommon
#import pgscrapydownloaderbase, pgscrapydownloadercommon


__version__ = "1.8"


DataFrame = TypeVar('DataFrame')
_NUM_SAMPLES = 5


class Data(GenericModel, Generic[DataFrame]):
    symbol: str
    data_frame: Optional[DataFrame]
    size: int = None
    """
    class Config:
        json_encoders = {
            Optional[DataFrame]: lambda x: x.to_json(),
        }
    """


class PGWebScrapingDownloader(pgscrapydownloaderbase.PGWebScrapingDownloaderBase, pgscrapydownloadercommon.PGWebScrapingDownloaderCommon):
    def __init__(self, project_name: str = "scrapingdownloader", logging_enable: str = False):
        super(PGWebScrapingDownloader, self).__init__(project_name=project_name,
                                                      object_short_name="PG_WS_DLR",
                                                      config_file_pathname=__file__.split('.')[0] + ".ini",
                                                      logging_enable=logging_enable,
                                                      config_file_type="ini")

        ### Common Variables
        self._data = {}
        self._parameter = {}  # if test_flag set to true, percentage of data will be used as testing dataset

        ### Specific Variables
        self._data_inputs = {}


    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    def run(self, dirpath: str, filename: str):
        Path(os.path.join(dirpath, filename))

        if Path.exists:
            if success := execute_js(file_path="/home/pant/anaconda3/envs/pgscrapydownloader_1/pgscrapedownloadertest.js", arguments="whtisup"):
            #if success := execute_js("/Users/jianhuang/opt/anaconda3/envs/pgscrapydownloader_1/pgscrapedownloadertest.js"):
                print("successfully acquired the data ")
            else:
                print("failed to acquire data")

    def run_scrapy(self, request, dirpath: str, filename: str):

        Path(os.path.join(dirpath, filename))
        #_argument = f"{request.url} /home/pant/anaconda3/envs/pgscrapydownloader_1/data/test51.html"
        _argument = f"{request.url} {os.path.join(dirpath, filename)}"
        #print(_argument)

        try:
            if Path.exists:
                if success := execute_js(file_path=os.path.join(self._config.parameters['config_file']['default']['downloader_binary'], "pgscrapedownloadertest.js"), arguments=_argument):
                #if True:
                    print("successfully acquired the data ")
                    with open(os.path.join(dirpath, filename)) as file:
                        return HtmlResponse(url=request.url,
                                            status=200,
                                            headers=None,
                                            body=file.read().encode("utf-8"),
                                            encoding='utf-8',
                                            request=request)

                else:
                    print("failed to acquire data")
                    return HtmlResponse(url=request.url,
                                        status=404,
                                        headers=None,
                                        body="failed to acquire data".encode("utf-8"),
                                        encoding='utf-8',
                                        request=request)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        exit(0)

    def get_tasks(self, pgdataset: Union[pd.DataFrame, dict, str], parameters: dict = None) -> bool:
        check_args(inspect.currentframe().f_code.co_name,
                   {'pgdataset': pgdataset, 'parameters': parameters, }, False)

        try:
            if isinstance(pgdataset, str):
                ### header is required
                self._data_inputs[parameters['name']] = {'parameter': parameters, 'data': pd.read_csv(pgdataset, header=0)}
            elif isinstance(pgdataset, dict):
                self._data_inputs[parameters['name']] = {'parameter': parameters,
                                                        'data': pd.DataFrame.from_dict(pgdataset, orient='index')}
            elif isinstance(pgdataset, pd.DataFrame):
                self._data_inputs[parameters['name']] = {'parameter': parameters, 'data': pgdataset}

            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _process(self, pgdataset: Any, parameters: Dict, *args, **kwargs):

        check_args(inspect.currentframe().f_code.co_name,
                   {'pgdataset': pgdataset, 'parameters': parameters, }, False)

        try:
            yield "next"

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def process(self, name: AnyStr = None, *args: object, **kwargs: object):
        try:
            yield self._process(name, {})

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


class PGWebScrapingDownloaderExt(PGWebScrapingDownloader):
    def __init__(self, project_name: str = "scrapingdownloaderext", logging_enable: str = False):
        super(PGWebScrapingDownloaderExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGWebScrapingDownloaderSingleton(PGWebScrapingDownloader):
    __instance = None

    @staticmethod
    def get_instance():
        if PGWebScrapingDownloaderSingleton.__instance == None:
            PGWebScrapingDownloaderSingleton()
        else:
            return PGWebScrapingDownloaderSingleton.__instance

    def __init__(self, project_name: str = "scrapingdownloadersingleton", logging_enable: str = False):
        super(PGWebScrapingDownloaderSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGWebScrapingDownloaderSingleton.__instance = self


if __name__ == '__main__':

    test = PGWebScrapingDownloaderExt()
    test.run("/home/pant/anaconda3/envs/pgscrapydownloader_1/data", "test51.html")