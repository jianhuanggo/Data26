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
import cryptocompare
from Meta import pgclassdefault, pggenericfunc
from Data.Storage.S3 import pgs3
from Data.Utils import pgoperation
from Data.Connect import pgdatabase
from API.Finance import pgfinancecommon, pgfinancebase
from pycaret.classification import *

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


__version__ = "1.7"


class PGCryptoTracker(pgfinancebase.PGFinanceBase, pgfinancecommon.PGFinanceCommon):
    def __init__(self, project_name: str = "pgcryptotracker", logging_enable: str = False):
        super().__init__(project_name=project_name,
                         object_short_name="PG_CT",
                         config_file_pathname=__file__.split('.')[0] + ".ini",
                         logging_enable=logging_enable,
                         config_file_type="ini")

        ### Common Variables
        self._name = "pgct"


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

        cryptocompare.cryptocompare._set_api_key_parameter(self._pg_api_key[0])

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

    def get_current_day(self):
        with open("/Users/jianhuang/opt/anaconda3/envs/my_crypto/Test/crypto_tickets.json", "r") as json_file:
            _pg_crypto_ticket = json.load(json_file)
            for item in _pg_crypto_ticket.keys():
                try:
                    n = cryptocompare.get_price(item, currency="USD", full=True)
                    self._pg_current_day[item] = n["RAW"][item]["USD"]["PRICE"]
                except Exception as err:
                    print(n)
                    print(err)
                    continue

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

        async with aiohttp.ClientSession() as session:
            with open("/Users/jianhuang/opt/anaconda3/envs/my_crypto/Test/crypto_tickets.json", "r") as json_file:
                _pg_crypto_ticket = json.load(json_file)
                for _index, _item in enumerate(_pg_crypto_ticket.keys(), start=1):
                    try:
                        _pg_price_symbol_ticket = f"{_pg_price_symbol_ticket}{_item}" if _index % self._pg_symbol_per_call == 1 else f"{_pg_price_symbol_ticket},{_item}"
                        print(_index, _pg_price_symbol_ticket)

                        if _index % self._pg_symbol_per_call == 0:
                            # await asyncio.sleep(1)
                            pg_url = f"{_pg_price_base_url}{_pg_price_symbol_ticket}&{_pg_price_currency}&{_pg_price_api}"
                            print(pg_url)
                            _pg_price_symbol_ticket = "fsyms="

                            for i in range(3):
                                try:
                                    # print(f"i: {i}")
                                    async with session.get(pg_url) as resp:
                                        _result = await resp.json()
                                        _pg_parse = {_key: _val["USD"] for _key, _val in _result.items()}
                                        self._pg_current_day = {**self._pg_current_day, **_pg_parse}
                                        print(_result)
                                        break

                                except Exception as err:
                                    print(err)
                                    if i == 2:
                                        self._pg_exception_url.append(pg_url)
                                    continue

                        if _index > 300:
                            break

                    except Exception as err:
                        if not self._pg_exception.get(_item, None):
                            self._pg_exception[_item] = _item
                        print(err)
                        continue
        if not self.pg_serialize_to_disk(self._pg_exception,
                                         os.path.join(self._pg_exception_dir, "crypto_exception.json")):
            raise
        return self._pg_current_day

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
    def compare_prices_db(self, db_session=None):
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







    def run(self):
        _pg_result = asyncio.run(self.get_current_day_async())
        if not self.pg_serialize_to_disk({f"{str(uuid.uuid4().hex[:8])}": _item for _item in self._pg_exception_url},
                                         os.path.join(self._pg_exception_dir,
                                                      f"crypto_exception_url_{time.time()}.json"),
                                         ):
            raise
        print(self._pg_exception_url)
        print(len(_pg_result))

        # self.get_current_day()
        print(self._pg_current_day)
        _pg_files = [f for f in listdir(self._pg_current_day_dir) if isfile(join(self._pg_current_day_dir, f))]
        if _pg_files:
            print(_pg_files)
            if _pg_files:
                with open(os.path.join(self._pg_current_day_dir, _pg_files[0]), "r") as json_file:
                    self._pg_past_day = json.load(json_file)
                self.compare_prices()
                shutil.move(os.path.join(self._pg_current_day_dir, _pg_files[0]), self._pg_past_days_dir)

        self.save_current_day_price()
        self.compare_prices_db()
        self.save_to_db("pg_crypto_tracker", self._pg_current_day)

    def save_crypto_list(self):
        with open("/Users/jianhuang/opt/anaconda3/envs/my_crypto/Test/crypto_list.json", "w") as json_file:
            json.dump(cryptocompare.get_coin_list(format=False), json_file)

    def get_tasks(self, pg_data: Union[str, list, pd.DataFrame, dict], pg_parameters: dict = {}) -> bool:
        pass

    def _process(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        try:
            #print(self._best_model)
            #print(f"pg_data1: {pg_data}")
            #print(f"pg_data_name1: {pg_data_name}")
            #print(f"pg_parameters1: {pg_parameters}")
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

    @pgdatabase.db_session('mysql')
    def save_to_db(self, pg_table_name: str, pg_data: Union[list, dict], exception_dirpath: str = None, db_session=None) -> bool:
        """
        transform {"BTC": 40000, "ETH": 2000, etc...}

        into

        [{"crypto_ticker: "BTC", "crypto_price": "40000", time_created: "1628347833.33536"},
         {"crypto_ticker: "ETH", "crypto_price": "2000", time_created: "1628347833.33536"}...]

        """
        _db_recording_time = time.time()
        return db_session.pg_save(pg_table_name,
                                  [{"crypto_ticker": _key, "crypto_price": format(_val, '.10f'), "time_created": _db_recording_time} for _key, _val in pg_data.items()],
                                  exception_dirpath)

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
            with open(pg_filepath, "w") as json_file:
                return json.load(json_file)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None


class PGCryptoTrackerExt(PGCryptoTracker):
    def __init__(self, project_name: str = "hubext", logging_enable: str = False):
        super().__init__(project_name=project_name, logging_enable=logging_enable)

        ### Specific Variables
        self._model_subtype = {}
        os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'


class PGCryptoTrackerSingleton(PGCryptoTracker):
    __instance = None

    @staticmethod
    def get_instance():
        if PGCryptoTrackerSingleton.__instance == None:
            PGCryptoTrackerSingleton()
        else:
            return PGCryptoTrackerSingleton.__instance

    def __init__(self, project_name: str = "hubsingleton", logging_enable: str = False):
        super(PGCryptoTrackerSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGCryptoTrackerSingleton.__instance = self


if __name__ == '__main__':
    test = PGCryptoTrackerExt()
    #test.run()
    test.compare_prices_db()
    #test.set_profile("default")

    exit(0)

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









