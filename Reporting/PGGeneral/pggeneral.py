import hashlib
import inspect
import numpy as np
import scipy.stats as st
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from Reporting import pgreportingcommon, pgreportingbase
from Meta import pgclassdefault, pggenericfunc
from Data.Connect import pgdatabase
from Data.Utils import pgoperation
import datetime as dt


_version__ = "1.8"

#deque(map(self.do_someting, native_object))


class PGReportingGeneral(pgreportingbase.PGReportingBase, pgreportingcommon.PGReportingCommon):
    def __init__(self, project_name: str = "pgrptgeneral", logging_enable: str = False):

        super(PGReportingGeneral, self).__init__(project_name=project_name,
                                                 object_short_name="PG_RPT_GENERAL",
                                                 config_file_pathname=__file__.split('.')[0] + ".ini",
                                                 logging_enable=logging_enable,
                                                 config_file_type="ini")

        ### Common Variables
        self.get_config(profile="default")
        self._name = "pandasdatareader"
        self._data = {}
        self._pattern_match = {}
        self._parameter = {}

        ### Specific Variables
        # tiingo - python
        self._data_inputs = {}

        self._get_func_map = {}

        self._get_func_parameter = {}

        self._set_func_map = {}

        self._set_func_parameter = {}




    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    @property
    def process(self) -> Callable:
        return self._process

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
    @pgdatabase.db_session('mysql')
    def get_conf_interval(self, method: str, method_input, db_session=None):
        def get_conf_interval_db(input_query: str):
            _query_result = db_session.simple_query(input_query)
            if _query_result and len(_query_result[0]) != 1:
                raise "this function expects a query return only one column"

            if not isinstance(_query_result[0][0], int) and not _query_result[0][0].isdigit():
                raise "this function expects a number column, please try to transform the data to number format"
            #print(_query_result)

            return _query_result
        try:

            if method == "database":
                method_input = [int(x[0]) for x in get_conf_interval_db(method_input)]

            if len(method_input) < 10:
                #print("too few data points... get min and mean instead")
                return [min(method_input), np.mean(method_input), len(method_input)]

            #elif method == "list":
            if isinstance(method_input, list):
                return st.t.interval(alpha=0.95, df=len(method_input)-1, loc=np.mean(method_input), scale=st.sem(method_input)), len(method_input)
            else:
                raise "expecting a list as input"
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False








    def _process(self, item: str, *args: object, **kwargs: object) -> bool:
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


class PGReportingGeneralExt(PGReportingGeneral):
    def __init__(self, project_name: str = "pgrptgeneralext", logging_enable: str = False):
        super(PGReportingGeneralExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGReportingGeneralSingleton(PGReportingGeneral):

    __instance = None

    @staticmethod
    def get_instance():
        if PGReportingGeneralSingleton.__instance == None:
            PGReportingGeneralSingleton()
        else:
            return PGReportingGeneralSingleton.__instance

    def __init__(self, project_name: str = "pgrptgeneralsingleton", logging_enable: str = False):
        super(PGReportingGeneralSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGReportingGeneralSingleton.__instance = self


if __name__ == '__main__':
    a = {'a': "ok",
         'b': "ok1",
         'c': "ok2"

    }

    print(tuple(val for val in a.values()))


    test = PGReportingGeneralSingleton()
    #test.set_param(**{'base_url': "https://www.realtor.com/soldhomeprices/Johns-Creek_GA"})
    #test.set_crypto_symbol('BTC', 'USD')
    task_list = test.get_tasks()
    print(task_list)
    test.process(*task_list)
    print(test.data['crypto_symbol'].keys())







