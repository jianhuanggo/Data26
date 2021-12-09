import os
import inspect
import time
import json
import uuid
import shutil
import aiohttp
import asyncio
from os import listdir
from os.path import isfile, join
from Meta import pgclassdefault, pggenericfunc
from Data.Storage.S3 import pgs3
from Data.Utils import pgoperation
from Data.Connect import pgdatabase
from API.Finance import pgfinancecommon, pgfinancebase
from pycaret.classification import *
from Regex import pgregex
from Reporting.PGGeneral import pggeneral
from Data.Utils import pgfile

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


__version__ = "1.8"


class PGPaniniAnalysis(pgfinancebase.PGFinanceBase, pgfinancecommon.PGFinanceCommon):
    def __init__(self, project_name: str = "pgpaninianalysis", logging_enable: str = False):
        super().__init__(project_name=project_name,
                         object_short_name="PG_PAA",
                         config_file_pathname=__file__.split('.')[0] + ".ini",
                         logging_enable=logging_enable,
                         config_file_type="ini")

        ### Common Variables
        self._name = "pgpaa"


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

    def get_coin_list(self):
        with open("/Users/jianhuang/opt/anaconda3/envs/my_crypto/Test/crypto_list.json", "r") as json_file:
            _pg_data = json.load(json_file)

    def save_current_day_price(self):
        with open(os.path.join(self._pg_current_day_dir, f"crypto_price_{time.time()}.json"), "w") as json_file:
            json.dump(self._pg_current_day, json_file)

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

    @pgdatabase.db_session('mysql')
    def data_analysis(self, db_session=None):
        _db_query ='''select * from
        (select * from (SELECT *, RANK() OVER (ORDER BY pg_time_created DESC ) date_rank FROM pg_crypto_tracker) time_n where date_rank = 1) as pg_time_n LEFT JOIN
        (select * from (SELECT *, RANK() OVER (ORDER BY pg_time_created DESC ) date_rank FROM pg_crypto_tracker) time_n_minus_1 where date_rank = (select count(1) from (SELECT *, RANK() OVER (ORDER BY pg_time_created DESC ) date_rank FROM pg_crypto_tracker) time_n where date_rank = 1) + 1) as pg_time_n_minus_1 
        on pg_time_n.pg_crypto_ticker = pg_time_n_minus_1.pg_crypto_ticker where cast(pg_time_n.pg_crypto_price as float) > 1 * cast(pg_time_n_minus_1.pg_crypto_price as float)
        '''
        try:
            print("""aaaaaaaaaa""")
            print(db_session.simple_query(_db_query))
            #self._pg_notification[_past_ticket] = [_past_price, _current_price]
            #if self._pg_notification and not self.pg_serialize_to_disk(self._pg_notification,
            #                                                           os.path.join(self._pg_notification_dir,
            #                                                                        f"alert_crypto_price_{time.time()}.json")):
            #    raise

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    @pgdatabase.db_session('mysql')
    def pg_panini_get_player_conf_interval(self, player_name: str, db_session=None):
        def get_conf_interval_query(pg_player_name: str, pg_card_set: str) -> str:
            # _db_query =f"select pg_athlete, pg_cardset, pg_price from pg_panini_analysis where pg_athlete like '%{player_name.upper()}%';"
            _db_query = f"select cast(substr(pg_price, 3, length(pg_price)) as UNSIGNED) from pg_panini_analysis where pg_athlete like '%{pg_player_name.upper()}%' and pg_cardset = '{pg_card_set}';"

            try:
                return _pgreport.get_conf_interval(method="database", method_input=_db_query)

            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None


        def get_card_sets(pg_player_name: str) -> list:
            _db_query = f"select distinct pg_cardset from pg_panini_analysis where pg_athlete like '%{pg_player_name.upper()}%';"
            try:
                _db_result = db_session.simple_query(_db_query)
                return [x[0] for x in _db_result]

            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None


        try:
            _pgreport = pggeneral.PGReportingGeneralExt()

            for _pg_card_set in get_card_sets(player_name):
                print(f"{player_name} {_pg_card_set}: {get_conf_interval_query(player_name, _pg_card_set)}")


            #'BC 2021-22 Hoops High Voltage'
            #print(_pgreport.get_conf_interval(method="database", method_input=_db_query))

            #print(db_session.simple_query(_db_query))
            #self._pg_notification[_past_ticket] = [_past_price, _current_price]
            #if self._pg_notification and not self.pg_serialize_to_disk(self._pg_notification,
            #                                                           os.path.join(self._pg_notification_dir,
            #                                                                        f"alert_crypto_price_{time.time()}.json")):
            #    raise

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def save(self, dirpath):
        def _processed(dirpath, processed_file_list: dict = {}):
            #_not_processed = []
            #print(pgfile.get_all_file_in_dir(dirpath), processed_file_list)

            for file_name in pgfile.get_all_file_in_dir(dirpath):
                n = processed_file_list.get(file_name, None)
                if not n and file_name != "_processed._internal":
                    #_not_processed.append(file_name)
                    yield file_name
            #print(_not_processed)
            #return _not_processed
        try:
            _existing_files = self.pg_deserialize_from_disk(f"{dirpath}/_processed._internal")
            for file_name in _processed(dirpath, _existing_files):
                print(file_name)
                if self.save_to_db("pg_panini_analysis", self.pg_deserialize_from_disk(f"{dirpath}/{file_name}")):
                    _existing_files[file_name] = "processed"
                else:
                    print("processed failed")

            self.pg_serialize_to_disk(_existing_files, f"{dirpath}/_processed._internal")

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def run(self):
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

    def clean_data(self, record: dict):

        #print(pgregex.parse_string("clean_column_name", ["image"]))

        # print({''.join(pgregex.parse_string("clean_column_name", [_ind])[0]): _val for _ind, _val in record.items()})

        return {pgregex.parse_string("clean_column_name", [_ind])[0]: _val for _ind, _val in record.items()}

    @pgdatabase.db_session('mysql')
    def save_to_db(self, pg_table_name: str, pg_data: Union[list, dict], exception_dirpath: str = None, db_session=None) -> bool:
        """
        transform {"BTC": 40000, "ETH": 2000, etc...}

        into

        [{"crypto_ticker: "BTC", "crypto_price": "40000", time_created: "1628347833.33536"},
         {"crypto_ticker: "ETH", "crypto_price": "2000", time_created: "1628347833.33536"}...]

        """
        # _db_recording_time = time.time()
        # return db_session.pg_save(pg_table_name,
        #                           [{"crypto_ticker": _key, "crypto_price": format(_val, '.10f'), "time_created": _db_recording_time} for _key, _val in pg_data.items()],
        #                           exception_dirpath)
        #print(pg_data[0])
        #print(self.clean_data(pg_data[0]))
        #print(pg_table_name)
        #exit(0)
        #return db_session.pg_save(pg_table_name, [self.clean_data(x) for x in pg_data], True, exception_dirpath, "bulk")
        return db_session.pg_save(pg_table_name, pg_data, True, exception_dirpath, "bulk")

    def pg_serialize_to_disk(self, pg_data: dict, pg_filepath: str) -> bool:
        try:
            with open(pg_filepath, "w") as json_file:
                json.dump(pg_data, json_file)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def pg_deserialize_from_disk(self, pg_filepath: str):
        try:
            with open(pg_filepath, "r") as json_file:
                #_content = json_file.read()
                return json.loads(json_file.read().replace("][", ","))
                # else:
                #     return json.load(json_file)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None


class PGPaniniAnalysisExt(PGPaniniAnalysis):
    def __init__(self, project_name: str = "pgpaaext", logging_enable: str = False):
        super().__init__(project_name=project_name, logging_enable=logging_enable)

        ### Specific Variables
        self._model_subtype = {}
        os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'


class PGPaniniAnalysisSingleton(PGPaniniAnalysis):
    __instance = None

    @staticmethod
    def get_instance():
        if PGPaniniAnalysisSingleton.__instance == None:
            PGPaniniAnalysisSingleton()
        else:
            return PGPaniniAnalysisSingleton.__instance

    def __init__(self, project_name: str = "pgpaasingleton", logging_enable: str = False):
        super(PGPaniniAnalysisSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGPaniniAnalysisSingleton.__instance = self


if __name__ == '__main__':
    test = PGPaniniAnalysisExt()
    test.save("/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/analysis/")
    #test.pg_panini_get_player_conf_interval("LEBRON")
    exit(0)
    #test.set_profile("default")



    filepath = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Input/heart.csv"
    #test.preprocessing(pd.read_csv(filepath))
    #test.preprocessing(pd.read_csv(filepath))
    #test.get_tasks(filepath)
    test.get_tasks({'output1': ['/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test2/output1_1.csv']})
    #print(f"data input: {test._data_inputs}")
    exit(0)
    test.process("heart")
    print(f"data : {test.data}")
    #test._process("heart", pd.read_csv(filepath))



