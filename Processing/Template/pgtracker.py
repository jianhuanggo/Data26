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
from itertools import repeat
from Meta import pgclassdefault, pggenericfunc
from Data.Storage.S3 import pgs3
from Data.Utils import pgoperation
from Data.Connect import pgdatabase
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List, Union
from Processing import pgprocessingcommon, pgprocessingbase
from pycaret.classification import *

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


__version__ = "1.8"


class PGTracker(pgprocessingbase.PGProcessingBase, pgprocessingcommon.PGProcessingCommon):
    def __init__(self, project_name: str = "pgtracker", logging_enable: str = False):
        super().__init__(project_name=project_name,
                         object_short_name="PG_T",
                         config_file_pathname=__file__.split('.')[0] + ".ini",
                         logging_enable=logging_enable,
                         config_file_type="ini")

        ### Common Variables
        self._name = "pgt"

        #parameters
        self._pg_percent_increase = 1.25

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

    def pg_exception_aws_s3(self):
        pass

    def pg_exception_db(self):
        pass

    def pg_exception_file(self, pg_exception_data: dict, pg_filepath: str):
        if os.path.exists(pg_filepath):
            self.pg_serialize_to_disk(pg_exception_data, pg_filepath, "a") if pg_exception_data else self.pg_serialize_to_disk({}, pg_filepath, "a")

    def pg_exception_mysql(self):
        pass

    def pg_exception(self, pg_exception_mode, pg_exception_parameters: dict):
        _pg_exception_func = {"local": self.pg_exception_file,
                              "s3": self.pg_exception_aws_s3,
                              "mysql": self.pg_exception_mysql,
        }
        if _pg_exception_func.get(pg_exception_mode, None)():
            return True
        else:
            return False

    async def pg_get_data_static_url_async(self, pg_urls, pg_data_formatter=None, pg_exception_mode="local"):
        _pg_result = []
        if isinstance(pg_urls, str):
            pg_urls = [pg_urls]
        try:
            async with aiohttp.ClientSession() as session:
                for _item in pg_urls:
                     # await asyncio.sleep(1)
                    for i in range(3):
                        try:
                            # print(f"i: {i}")
                            async with session.get(_item) as resp:
                                _pg_request_result = await resp.json()
                                _pg_formatted_result = pg_data_formatter(_pg_request_result) if pg_data_formatter else _pg_request_result
                                if isinstance(_pg_formatted_result, list):
                                    _pg_result += _pg_formatted_result
                                else:
                                    _pg_result.append(_pg_result)
                                print(_pg_formatted_result)
                                break
                        except Exception as err:
                            print(err)
                            if i == 2:
                                self._pg_exception_url.append(_item)
                            continue

            if self._pg_exception_url:
                if not self.pg_serialize_to_disk(
                    {f"{str(uuid.uuid4().hex[:8])}": _item for _item in self._pg_exception_url},
                    os.path.join(self._pg_exception_dir,
                                 f"crypto_exception_url_{time.time()}.json"), "a"):
                    pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
                    raise
            return _pg_result
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

    def pg_transformation(self):
        pass

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

    def run(self, entity_name: str, urls, func_data_formatter):
        self.save_to_db(entity_name, asyncio.run(self.pg_get_data_static_url_async(urls, func_data_formatter)))

    def example_run(self, entity_name: str):
        result = asyncio.run(self.pg_get_data_static_url_async(self.pg_get_url(), self.crypto_compare_formatter))
        self.save_to_db(entity_name, result)
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

    @pgdatabase.db_session('mysql')
    def save_to_db(self, pg_table_name: str, pg_data: Union[list, dict], exception_dirpath: str = None,
                   db_session=None) -> bool:
        """

        [{"crypto_ticker: "BTC", "crypto_price": "40000", time_created: "1628347833.33536"},
         {"crypto_ticker: "ETH", "crypto_price": "2000", time_created: "1628347833.33536"}...]

        """
        try:
            _db_recording_time = time.time()
            if isinstance(pg_data, dict):
                pg_data = [pg_data]
            return db_session.pg_save(pg_table_name, [{**x, **{"time_created": _db_recording_time}} for x in pg_data], exception_dirpath)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def pg_serialize_to_disk(self, pg_data: dict, pg_filepath: str, pg_mode: str = "w") -> bool:
        try:
            with open(pg_filepath, pg_mode) as json_file:
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


if __name__ == '__main__':
    test = PGTracker()
    test.example_run("test1")
