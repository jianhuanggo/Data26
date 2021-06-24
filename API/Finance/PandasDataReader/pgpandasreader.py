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
from API.Finance import pgfinancecommon, pgfinancebase
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
import pandas_datareader as pdr
import datetime as dt

#pip install pandas-datareader
_version__ = "1.7"

#deque(map(self.do_someting, native_object))


class PGPandasDataReader(pgfinancebase.PGFinanceBase, pgfinancecommon.PGFinanceCommon):
    def __init__(self, project_name: str = "pandasdatareader", logging_enable: str = False):

        super(PGPandasDataReader, self).__init__(project_name=project_name,
                                                 object_short_name="PG_PD_READER",
                                                 config_file_pathname=__file__.split('.')[0] + ".ini",
                                                 logging_enable=logging_enable,
                                                 config_file_type="ini")

        ### Common Variables
        self._name = "pandasdatareader"
        self._data = {}
        self._pattern_match = {}
        self._parameter = {}

        ### Specific Variables
        # tiingo - python
        self._data_inputs = {}

        self._get_func_map = {'nasdaq_all_symbol': self.get_all_nasdaq_symbols,
                              'crypto_symbol':     pdr.DataReader,
                              }

        self._get_func_parameter = {'nasdaq_all_symbol': (3, 30, 1),
                                    'crypto_symbol': self.get_crypto_symbol
                                    }

        self._set_func_map = {'nasdaq_all_symbol': NotImplemented,
                              'crypto_symbol':     self.set_crypto_symbol,
                              }

        self._set_func_parameter = {'nasdaq_all_symbol': NotImplemented,
                                    'crypto_symbol': NotImplemented
                                    }

        self.get_config(profile="default")


    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    @property
    def process(self) -> Callable:
        return self._process

    def get_all_nasdaq_symbols(self, retry_count=3, timeout=30, pause=1):
        self._data_inputs['nasdaq_symbols'] = pdr.nasdaq_trader.get_nasdaq_symbols(retry_count=retry_count, timeout=timeout, pause=pause)

    def get_crypto_symbol(self) -> list:
        return self._data_inputs['crypto_symbol'] if "crypto_symbol" in self._data_inputs else []

    def set_crypto_symbol(self, crypto_curr: str, fiat_curr: str) -> bool:
        try:
            if "crypto_symbol" in self._data_inputs:
                #self._data_inputs['crypto_symbol'].append((f"{crypto_curr}-{fiat_curr}", 'yahoo', dt.datetime(2016, 1, 1), dt.datetime.now()))
                self._data_inputs['crypto_symbol'].append(
                     {'crypto_symbol': f"{crypto_curr}-{fiat_curr}",
                      'data_source': 'yahoo',
                      'start_time': dt.datetime(2016, 1, 1),
                      'end_time': dt.datetime.now(),
                      }
                    )

            else:
                self._data_inputs['crypto_symbol'] = [
                    {'crypto_symbol': f"{crypto_curr}-{fiat_curr}",
                     'data_source': 'yahoo',
                     'start_time': dt.datetime(2016, 1, 1),
                     'end_time': dt.datetime.now(),
                     }
                    ]

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def get_config(self, profile: str = "default") -> bool:
        try:
            if profile in self._config.parameters["config_file"]:
                for key in self._config.parameters["config_file"][profile]:
                    for element in self._config.parameters["config_file"][profile][key].split(','):
                        self._set_func_map.get(key, "other")(element.split('-')[0], element.split('-')[1])

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    # single iterable - list/tuple, each item in the iterable will be assigned to one client
    def get_tasks(self):
        return list(self._data_inputs.keys())

    # invoke self._get_func_parameter to get a list of items to be processed
    def process(self, item: str, *args: object, **kwargs: object) -> bool:
        try:
            print(item)
            #self._data[kwargs['type']] = list(map(lambda x: self._func_map[x](self._func_parameter[x]), iterable))

            for sub_item in self._get_func_parameter[item]():
                print(f"sub_item: {sub_item}")
                tuple(val for val in sub_item.values())

                if item in self._data:
                    self._data[item][sub_item['crypto_symbol']] = self._get_func_map[item](*tuple(val for val in sub_item.values()))
                else:
                    self._data[item] = {sub_item['crypto_symbol']: self._get_func_map[item](*tuple(val for val in sub_item.values()))}

                    #self._get_func_map[item](*tuple(val for val in sub_item.values()))

                """
                if item in self._data:
                    #self._data[item].append(self._get_func_map[item](*sub_item))
                    self._data[item].append({sub_item['crypto_symbol']:
                                                 self._get_func_map[item](*tuple(val for val in sub_item.values()))
                                             }
                                            )
                else:
                    #self._data[item] = [self._get_func_map[item](*sub_item)]
                    self._data[item] = [{sub_item['crypto_symbol']:
                                             self._get_func_map[item](*tuple(val for val in sub_item.values()))
                                         }
                                        ]
                """

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


class PGPandasDataReaderExt(PGPandasDataReader):
    def __init__(self, project_name: str = "pandasdatareaderext", logging_enable: str = False):
        super(PGPandasDataReaderExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGPandasDataReaderSingleton(PGPandasDataReader):

    __instance = None

    @staticmethod
    def get_instance():
        if PGPandasDataReaderSingleton.__instance == None:
            PGPandasDataReaderSingleton()
        else:
            return PGPandasDataReaderSingleton.__instance

    def __init__(self, project_name: str = "pandasdatareadersingleton", logging_enable: str = False):
        super(PGPandasDataReaderSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGPandasDataReaderSingleton.__instance = self


if __name__ == '__main__':
    a = {'a': "ok",
         'b': "ok1",
         'c': "ok2"

    }

    print(tuple(val for val in a.values()))


    test = PGPandasDataReaderSingleton()
    #test.set_param(**{'base_url': "https://www.realtor.com/soldhomeprices/Johns-Creek_GA"})
    #test.set_crypto_symbol('BTC', 'USD')
    task_list = test.get_tasks()
    print(task_list)
    test.process(*task_list)
    print(test.data['crypto_symbol'].keys())







