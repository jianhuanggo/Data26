import os
import inspect
import time
import json
import uuid
import shutil
import aiohttp
import asyncio
from Processing.Template import pgtracker
from os import listdir
from os.path import isfile, join
import cryptocompare
from itertools import repeat
from Meta import pgclassdefault, pggenericfunc
from Data.Storage.S3 import pgs3
from Data.Utils import pgoperation
from Data.Connect import pgdatabase
from API.Finance import pgfinancecommon, pgfinancebase
from pycaret.classification import *

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


__version__ = "1.8"


class PGTracker(pgfinancebase.PGFinanceBase, pgfinancecommon.PGFinanceCommon):
    def __init__(self, project_name: str = "pgtracker", logging_enable: str = False):
        super().__init__(project_name=project_name,
                         object_short_name="PG_T",
                         config_file_pathname=__file__.split('.')[0] + ".ini",
                         logging_enable=logging_enable,
                         config_file_type="ini")

        ### Common Variables
        self._name = "pgt"

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


    def pg_get_url(self, test_index: int):
        _pg_price_base_url = "https://min-api.cryptocompare.com/data/pricemulti?"
        # _pg_price_symbol_ticket = "fsym=BTC"
        _pg_price_symbol_ticket = "fsyms="
        # _pg_price_currency = "tsyms=USD,JPY,EUR"
        _pg_price_currency = "tsyms=USD"
        _pg_price_api = f"pi_key={self._pg_api_key[0]}"
        _list = []

        with open("/Users/jianhuang/opt/anaconda3/envs/my_crypto/Test/crypto_tickets.json", "r") as json_file:
            _pg_crypto_ticket = json.load(json_file)

            for _index, _item in enumerate(_pg_crypto_ticket.items(), start=1):
                if _index % self._pg_symbol_per_call == 0:
                    _list.append(_pg_price_symbol_ticket)
                    _pg_price_symbol_ticket = "fsyms="
                else:
                    _pg_price_symbol_ticket = f"{_pg_price_symbol_ticket}{_item[0]}" if _index % self._pg_symbol_per_call == 1 else f"{_pg_price_symbol_ticket},{_item[0]}"

            return (f"{_pg_price_base_url}{_val}&{_pg_price_currency}&{_pg_price_api}" for _val in _list[:test_index])

    @staticmethod
    def crypto_compare_formatter(data):
        """
        convert inputs of request into
        [{"column 1": column1 value, "column 2": column2 value, etc..}, {"column 1": column1 value, "column 2": column2 value, etc..}...]
        """
        return [{"crypto_ticker": _key, "crypto_price": format(_val["USD"], '.10f')} for _key, _val in data.items()]

    def pg_get_url_full(self, test_index: int = None):
        _pg_price_base_url = "https://min-api.cryptocompare.com/data/pricemultifull?"
        # _pg_price_symbol_ticket = "fsym=BTC"
        _pg_price_symbol_ticket = "fsyms="
        # _pg_price_currency = "tsyms=USD,JPY,EUR"
        _pg_price_currency = "tsyms=USD"
        _pg_price_api = f"pi_key={self._pg_api_key[0]}"
        _list = []

        with open("/Users/jianhuang/opt/anaconda3/envs/my_crypto/Test/crypto_tickets.json", "r") as json_file:
            _pg_crypto_ticket = json.load(json_file)

            for _index, _item in enumerate(_pg_crypto_ticket.items(), start=1):
                if _index % self._pg_symbol_per_call == 0:
                    _list.append(_pg_price_symbol_ticket)
                    _pg_price_symbol_ticket = "fsyms="
                else:
                    _pg_price_symbol_ticket = f"{_pg_price_symbol_ticket}{_item[0]}" if _index % self._pg_symbol_per_call == 1 else f"{_pg_price_symbol_ticket},{_item[0]}"

            return (f"{_pg_price_base_url}{_val}&{_pg_price_currency}&{_pg_price_api}" for _val in _list[:test_index]) if test_index else (f"{_pg_price_base_url}{_val}&{_pg_price_currency}&{_pg_price_api}" for _val in _list)

    @staticmethod
    def crypto_compare_formatter_full(data):
        _list = []
        for _key, _item in data.items():
            if _key == "RAW":
                for _key1, _item1 in _item.items():
                    # print(f"{_key1}: {_item1}\n")
                    # _list.append({_key1:})
                    _list.append({"crypto_ticker": _key1,
                                  "crypto_price": format(_item1["USD"].get("PRICE", None), '.10f') if "PRICE" in _item1[
                                      "USD"] else None,
                                  "crypto_day_volume_in_price": format(_item1["USD"].get("VOLUME24HOURTO", None),
                                                                       '.10f') if "VOLUME24HOURTO" in _item1[
                                      "USD"] else None,
                                  "crypto_day_high_price": format(_item1["USD"].get("PRICE", None),
                                                                  '.10f') if "HIGHDAY" in _item1["USD"] else None,
                                  "crypto_day_low_price": format(_item1["USD"].get("PRICE", None),
                                                                 '.10f') if "LOWDAY" in _item1["USD"] else None})
        return _list

    @pgdatabase.db_session('mysql')
    def compare_prices_db(self, db_session=None):
        _db_query ='''select * from
        (select * from (SELECT *, RANK() OVER (ORDER BY pg_time_created DESC ) date_rank FROM pg_crypto_tracker) time_n where date_rank = 1) as pg_time_n LEFT JOIN
        (select * from (SELECT *, RANK() OVER (ORDER BY pg_time_created DESC ) date_rank FROM pg_crypto_tracker) time_n_minus_1 where date_rank = (select count(1) from (SELECT *, RANK() OVER (ORDER BY pg_time_created DESC ) date_rank FROM pg_crypto_tracker) time_n where date_rank = 1) + 1) as pg_time_n_minus_1 
        on pg_time_n.pg_crypto_ticker = pg_time_n_minus_1.pg_crypto_ticker where cast(pg_time_n.pg_crypto_price as float) > 1 * cast(pg_time_n_minus_1.pg_crypto_price as float)
        '''
        try:
            print(db_session.simple_query(_db_query))
            #self._pg_notification[_past_ticket] = [_past_price, _current_price]
            #if self._pg_notification and not self.pg_serialize_to_disk(self._pg_notification,
            #                                                           os.path.join(self._pg_notification_dir,
            #                                                                        f"alert_crypto_price_{time.time()}.json")):
            #    raise

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def run(self, entity_name: str, test_case: int = None):
        #pgtracker.PGTracker().run(entity_name, self.pg_get_url(10), self.crypto_compare_formatter)
        pgtracker.PGTracker().run(entity_name, self.pg_get_url_full(test_case), self.crypto_compare_formatter_full)
        #_pg_result = asyncio.run(self.get_current_day_async())
        #result = asyncio.run(self.pg_get_data_static_url_async(self.pg_get_url(), self.crypto_compare_formatter))
        #test.save_to_db(entity_name, result)
        print(self._pg_exception_url)

        """
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
        """

    def get_tasks(self, pg_data: Union[str, list, pd.DataFrame, dict], pg_parameters: dict = {}) -> bool:
        pass

    def _process(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        pass

    def process(self, name: str = None, *args: object, **kwargs: object) -> bool:
        pass


if __name__ == '__main__':
    test = PGTracker()
    PGTracker().run("pg_crypto_tracker")


