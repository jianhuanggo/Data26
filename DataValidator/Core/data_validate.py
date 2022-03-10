import os
import inspect
import time
import json
import spacy
import pandas as pd
from typing import Union
import argparse
from typing import List, Union, Tuple, Dict
import uuid
import shutil
import asyncio
from os import listdir
from os.path import isfile, join
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
from Data.Connect import pgdatabase
from API.Finance import pgfinancecommon, pgfinancebase
from Data.Utils import pgfile
from DataValidator import pgvalidatecommon, pgvalidatebase



import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


__version__ = "1.8"


class PGValidate(pgvalidatecommon.PGValidateCommon, pgvalidatebase.PGValidateBase):
    def __init__(self, project_name: str = "pgvalidate", logging_enable: str = False):
        super().__init__(project_name=project_name,
                         object_short_name="PG_VAL",
                         config_file_pathname=__file__.split('.')[0] + ".ini",
                         logging_enable=logging_enable,
                         config_file_type="ini")

        ### Common Variables
        self._name = "pgval"


        self._pg_api_key = ["ef53798e3676ea190464e4e8aa099e1f83a005dfd68ea69c46dcb516a6bdc93a",
                            "caf120d0abba96864facdbdeea0f37264202eee384f83832d495102b8cfdd05e"]

        #parameters
        self._pg_symbol_per_call = 20
        self._pg_percent_increase = 1.25


        self._pg_current_day_dir = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGCryptoTracker/Project/Current_Day/"
        self._pg_past_days_dir = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGCryptoTracker/Project/Past_Days/"
        self._pg_notification_dir = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGCryptoTracker/Project/Notification/"
        self._pg_exception_dir = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGCryptoTracker/Project/Exception/"
        self._db_exception_file = os.path.join(self._pg_exception_dir, "db_exception.txt")

        # placeholders
        self._data = {}
        self._pg_exception_url = []
        self._pg_exception = {}
        self._pg_past_day = {}
        self._pg_current_day = {}
        self._pg_notification = {}
        self._data_inputs = {}

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    async def get_current_day_async(self):
        _pg_price_base_url = "https://min-api.cryptocompare.com/data/pricemulti?"
        # _pg_price_symbol_ticket = "fsym=BTC"
        _pg_price_symbol_ticket = "fsyms="
        # _pg_price_currency = "tsyms=USD,JPY,EUR"
        _pg_price_currency = "tsyms=USD"
        _pg_price_api = f"pi_key={self._pg_api_key[0]}"

        if not os.path.exists(os.path.join(self._pg_exception_dir, "crypto_exception.json")):
            if not self.pg_serialize_to_disk({}, os.path.join(self._pg_exception_dir, "crypto_exception.json")):
                raise

        with open(os.path.join(self._pg_exception_dir, "crypto_exception.json"), "r") as json_read_except_file:
            self._pg_exception = json.load(json_read_except_file)
            self._pg_exception = {} if self._pg_exception is None else self._pg_exception


    def data_stats(self, filepath):
        _func = {"save": self.pg_serialize_to_disk,
                 "load": self.pg_deserialize_from_disk}
        def _create_schema(pg_data: Dict) -> pd.DataFrame:
            """
            data = {'Name':['Tom', 'nick', 'krish', 'jack'],
                    'Age':[20, 21, 19, 18]}
            """
            return pd.DataFrame(pg_data)

        def _get_filename(filepath: str) -> str:
            return pgfile.get_file_from_dirpath(filepath)

        def _save(pg_data: pd.DataFrame, filepath) -> bool:
            nonlocal _func
            try:
                _func.get("save")(pg_data.to_dict(), filepath)
                return True
            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
                return False

        def _load(filepath):
            try:
                _func.get("load")(filepath)
                return True
            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
                return False
            

    def compare_prices(self):
        for _past_ticket, _past_price in self._pg_past_day.items():
            _current_price = self._pg_current_day.get(_past_ticket, None)
            if _current_price:
                if _current_price > self._pg_percent_increase * _past_price:
                    self._pg_notification[_past_ticket] = [_past_price, _current_price]

        if self._pg_notification and not self.pg_serialize_to_disk(self._pg_notification,
                                                                   os.path.join(self._pg_notification_dir,
                                                                                f"alert_crypto_price_{time.time()}.json")):
            raise

    #@pgdatabase.db_session('mysql')
    def pg_is_person(self, pg_dataset: Union[pd.DataFrame, List, Tuple, str],
                     acceptance_criterion: int = 90,
                     istrue=lambda x: any(x),
                     db_session=None) -> bool:
        try:
            pg_nlp = spacy.load("en_core_web_sm")
            _func_dict = {"list": pg_nlp,
            }
            if pg_dataset is None:
                return True

            _result = []
            if isinstance(pg_dataset, (List, Tuple)):
                _pg_func = _func_dict.get("list")
                for each_text in pg_dataset:
                    #print(each_text)
                    pg_doc = _pg_func(each_text)
                    #[(ent.label_, ent.label_ == 'PERSON') for ent in pg_doc.ents]
                    _label = [ent.label_ for ent in pg_doc.ents]
                    if _label:
                        _result.append((each_text, _label[0]))

            return True if sum([x[1] == "PERSON" for x in _result]) / len(_result) * 100 >= acceptance_criterion else False

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def validate_suite(self, filepath: str, data_udf=None, acceptance_criterion: int = 100) -> bool:
        try:
            _validaton_test = {0: self.pg_is_person,

            }
            _pg_test_result = []
            if pgfile.isfileexist(filepath):
                _pg_data = data_udf(self.pg_deserialize_from_disk(filepath))
                for _pg_test in _validaton_test.values():
                    _pg_test_result.append(_pg_test(_pg_data))

            #print([item["player_name"] for item in _data])
            #print(self.pg_is_person([item["player_name"] for item in _data]))
            if _pg_test_result is None:
                return True

            return True if sum(_pg_test_result) / len(_pg_test_result) * 100 >= acceptance_criterion else False
            #self.save_to_db("pg_panini_analysis", _data)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


    def _run(self):
        #_pg_result = asyncio.run(self.get_current_day_async())
        # if not self.pg_serialize_to_disk({f"{str(uuid.uuid4().hex[:8])}": _item for _item in self._pg_exception_url},
        #                                  os.path.join(self._pg_exception_dir,
        #                                               f"crypto_exception_url_{time.time()}.json"),
        #                                  ):
        #     raise
        # print(self._pg_exception_url)

        # self.get_current_day()
        # print(self._pg_current_day)




        # _pg_files = [f for f in listdir(self._pg_current_day_dir) if isfile(join(self._pg_current_day_dir, f))]
        # if _pg_files:
        #     print(_pg_files)
        #     if _pg_files:
        #         with open(os.path.join(self._pg_current_day_dir, _pg_files[0]), "r") as json_file:
        #             self._pg_past_day = json.load(json_file)
        #         self.compare_prices()
        #         shutil.move(os.path.join(self._pg_current_day_dir, _pg_files[0]), self._pg_past_days_dir)

        # self.save_current_day_price()
        try:
            _data = self.pg_deserialize_from_disk("/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/analysis/Panini_recent_sale_try18.json")
            #print(_data)
            #exit(0)
            #_data = self.pg_deserialize_from_disk("/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/analysis/test.json")
            self.save_to_db("pg_panini_analysis", _data)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def get_tasks(self, pg_data: Union[str, list, pd.DataFrame, dict], pg_parameters: dict = {}) -> bool:
        pass

    def _process(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        try:
            if not pg_data:
                return None

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def process(self, name: str = None, *args: object, **kwargs: object) -> bool:
        try:
            if not self._data_inputs:
                return True
            if name:
                _item = self._data_inputs[name]
                #print(f"_item: {_item}")
                #exit(0)
                self._process(name, _item['data'], _item['parameter'], )
            else:
                for _index, _data in self._data_inputs.items():
                    _item = self._data_inputs[_index]
                    self._process(name, _item['data'], _item['parameter'])
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def _command_line(self):
        try:
            argparser = argparse.ArgumentParser(description='panini store command line')
            argparser.add_argument('-v', '--version', action='version', version='%(prog)s VERSION ' + str(__version__),
                                   help='show current version')
            argparser.add_argument('-d', '--data_dir', action='store', type=str, dest='pg_data_dir', required=True,
                                   help='panini store data directory')

            argparser.add_argument('-t', '--target_tablename', action='store', type=str, dest='pg_tablename', required=True,
                                   help='panini store target table name')

            # argparser.add_argument('-y', '--worker_type', action='store', choices=['worker',
            #                                                                        'watcher',
            #                                                                        'jobdispatcher',
            #                                                                        'sweeper',
            #                                                                        'dataprofiler'],
            #                        required=True, dest='daemon_type', help='type of daemon')

            args = argparser.parse_args()
            return args
        except Exception as err:
            pggenericfunc.pg_error_logger(None, inspect.currentframe().f_code.co_name, err)
            return False


class PGValidateExt(PGValidate):
    def __init__(self, project_name: str = "pgvalext", logging_enable: str = False):
        super().__init__(project_name=project_name, logging_enable=logging_enable)

        ### Specific Variables
        self._model_subtype = {}
        os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'


class PGValidateSingleton(PGValidate):
    __instance = None

    @staticmethod
    def get_instance():
        if PGValidateSingleton.__instance == None:
            PGValidateSingleton()
        else:
            return PGValidateSingleton.__instance

    def __init__(self, project_name: str = "pgvalsingleton", logging_enable: str = False):
        super(PGValidateSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGValidateSingleton.__instance = self


if __name__ == '__main__':
    test = PGValidateExt()
    print(test.validate_suite("/Users/jianhuang/opt/anaconda3/envs/pg_data/panini/auctions/basketball/a376898a_pgspider.json",
             data_udf=lambda x: [item["player_name"] for item in x]))

    #print(test.pg_is_person(["Robert P.", "Robert DiFilippo", "Aly Samir", "Bollinger, Christopher", "Reese Cassidy", "Meena M.", "Vincent Palmer", "Jian Huang", "Alan Cofer", "Saly"]))




