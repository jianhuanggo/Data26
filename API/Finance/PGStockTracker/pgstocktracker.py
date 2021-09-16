import os
import inspect
import time
import json
import yfinance as yf
from Processing.Template import pgtracker
from Meta import pggenericfunc
from Data.Connect import pgdatabase
from API.Finance import pgfinancecommon, pgfinancebase
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator

__version__ = "1.8"


"""
ETF symbol available JSON:https://fmpcloud.io/api/v3/symbol/available-etfs?apikey=1f3956f8be8d7890a43e5aa2563c406f
Commodities available JSON:https://fmpcloud.io/api/v3/symbol/available-commodities?apikey=1f3956f8be8d7890a43e5aa2563c406f
Euronext available JSON:https://fmpcloud.io/api/v3/symbol/available-euronext?apikey=1f3956f8be8d7890a43e5aa2563c406f
NYSE available JSON:https://fmpcloud.io/api/v3/symbol/available-nyse?apikey=1f3956f8be8d7890a43e5aa2563c406f
AMEX available JSON:https://fmpcloud.io/api/v3/symbol/available-amex?apikey=1f3956f8be8d7890a43e5aa2563c406f
TSX available JSON:https://fmpcloud.io/api/v3/symbol/available-tsx?apikey=1f3956f8be8d7890a43e5aa2563c406f
Market Indexes available JSON:https://fmpcloud.io/api/v3/symbol/available-indexes?apikey=1f3956f8be8d7890a43e5aa2563c406f
Mutual Fund available JSON:https://fmpcloud.io/api/v3/symbol/available-mutual-funds?apikey=1f3956f8be8d7890a43e5aa2563c406f
Nasdaq available JSON:https://fmpcloud.io/api/v3/symbol/available-nasdaq?apikey=1f3956f8be8d7890a43e5aa2563c406f


#All Nasdaq Prices JSON:https://fmpcloud.io/api/v3/quotes/nasdaq?apikey=1f3956f8be8d7890a43e5aa2563c406f
#All Mutual Fund Prices JSON:https://fmpcloud.io/api/v3/quotes/mutual_fund?apikey=1f3956f8be8d7890a43e5aa2563c406f
#All TSX Prices JSON:https://fmpcloud.io/api/v3/quotes/tsx?apikey=1f3956f8be8d7890a43e5aa2563c406f
#All Amex Prices JSON:https://fmpcloud.io/api/v3/quotes/amex?apikey=1f3956f8be8d7890a43e5aa2563c406f
#All NYSE Prices JSON:https://fmpcloud.io/api/v3/quotes/nyse?apikey=1f3956f8be8d7890a43e5aa2563c406f
#All Euronext Prices JSON:https://fmpcloud.io/api/v3/quotes/euronext?apikey=1f3956f8be8d7890a43e5aa2563c406f
#All Commodities Prices JSON:https://fmpcloud.io/api/v3/quotes/commodity?apikey=1f3956f8be8d7890a43e5aa2563c406f
#All Indexes Prices JSON:https://fmpcloud.io/api/v3/quotes/index?apikey=1f3956f8be8d7890a43e5aa2563c406f

"""


def remove_space_dict(pg_dict):
    return {x.replace(" ", ""): y for x, y in pg_dict.items()}


class PGStockTracker(pgfinancebase.PGFinanceBase, pgfinancecommon.PGFinanceCommon):
    def __init__(self, project_name: str = "pgstocktracker", logging_enable: str = False):
        super().__init__(project_name=project_name,
                         object_short_name="PG_SKT",
                         config_file_pathname=__file__.split('.')[0] + ".ini",
                         logging_enable=logging_enable,
                         config_file_type="ini")

        ### Common Variables
        self._name = "pgskt"

        self._pg_api_key = []

        #parameters
        self._pg_symbol_per_call = 20
        self._pg_percent_increase = 1.25
        self._pg_base_dir = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance"

        self._pg_current_day_dir = os.path.join(self._pg_base_dir, "PGCryptoTracker/Project/Current_Day/")
        self._pg_past_days_dir = os.path.join(self._pg_base_dir, "PGCryptoTracker/Project/Past_Days/")
        self._pg_notification_dir = os.path.join(self._pg_base_dir, "PGCryptoTracker/Project/Notification/")
        self._pg_exception_dir = os.path.join(self._pg_base_dir, "PGCryptoTracker/Project/Exception/")
        self._db_exception_file = os.path.join(self._pg_exception_dir, "db_exception.txt")

        # placeholders
        self._data = {}
        self._pg_exception_url = []
        self._pg_exception = {}
        self._pg_past_day = {}
        self._pg_current_day = {}
        self._pg_notification = {}
        self._data_inputs = {}

    def pg_get_url_list_ticker(self, entity_name: str = None, test_index: int = None):
        with open(os.path.join(self._pg_base_dir, "PGStockTracker/Data/ticker_url.json"),
                  "r") as json_file:
            #return list(json.load(json_file).values())
            #print(json.load(json_file)[entity_name])
            #exit(0)
            return json.load(json_file)[entity_name]

    @staticmethod
    def pg_formatter_list_ticker(data):
        """
        convert inputs of request into
        [{"column 1": column1 value, "column 2": column2 value, etc..}, {"column 1": column1 value, "column 2": column2 value, etc..}...]
        """
        return data

    def pg_get_data(self, entity_name: str = None, test_index: int = None):
        @pgdatabase.db_session('mysql')
        def get_ticker_db(entity_name, db_session=None):
            try:
                _db_query = f"select pg_symbol from pg_real_estate.PG_{entity_name}"
                return db_session.simple_query(_db_query)
            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None
        #_stock_ticker
        #for _ind, _item in enumerate(get_ticker_db(entity_name), start=1):
        _ticker_list = get_ticker_db(entity_name)
        _stock_stats_list = []
        #print([' '.join(list(map(lambda x: x[0],_ticker_list[x: x + self._pg_symbol_per_call]))) for x, y in enumerate(_ticker_list) if x % self._pg_symbol_per_call == 0])
        #print(_ticker_list)
        for item in [' '.join(list(map(lambda x: x[0], _ticker_list[x: x + self._pg_symbol_per_call]))) for x, y in enumerate(_ticker_list) if x % self._pg_symbol_per_call == 0]:
            data = yf.download(tickers=item, period="1d")
            try:
                if data is not None:
                    data = data.fillna(-1)

                    _data_index = data.index.values
                    print(f"data_index: {_data_index}")
                    data.reset_index(drop=True, inplace=True)

                    if len(_data_index) > 1:
                        data = data.iloc[-1:, :]
                        _data_index = [_data_index[-1]]

                    #print(data.iloc[1:, :])

                    #data.to_csv('test.csv', index=False)
                    #print(data.columns.get_level_values(0))
                    #print(data.columns.get_level_values(1))

                    data = data.unstack(level=[0]).reset_index(level=0).pivot(columns='level_0').droplevel(1)
                    data.columns = data.columns.droplevel()

                    #print(len(_data_index))
                    #exit(0)
                    #if len(_data_index) != 1:
                    #    continue

                    #print(data.index.values)

                    #data['stock_timestamp'] = _data_index[0] if len(_data_index) == 1 else _data_index
                    _pg_timestamp = str(_data_index[-1]).split('.')[-1] if "." in str(_data_index[-1]) else str(_data_index[-1])
                    data['stock_timestamp'] = _pg_timestamp
                    #data['stock_timestamp'] = data['stock_timestamp'].astype(str)

                    data = data.to_dict('index')
                    _data = [{**{'stock_symbol': _ind}, **remove_space_dict(_val)} for _ind, _val in data.items()]
                    print(f"data: {_data}")
                    _stock_stats_list += _data
                    print(len(_stock_stats_list))

                    #print(_stock_stats_list)
            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
                continue
        return _stock_stats_list


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
    """
    def run_list_ticker(self, entity_name: str, test_case: int = None):
        with open(os.path.join(self._pg_base_dir, "PGStockTracker/Data/ticker_url.json"),
                  "r") as json_file:
            #return list(json.load(json_file).values())
            for _ind, _val in json.load(json_file).items():
                print(_ind, _val, self.pg_get_url_list_ticker(entity_name=_ind))
                self.run(f"PG_{_ind}", self.pg_get_url_list_ticker(entity_name=_ind), self.pg_formatter_list_ticker)

        print(self._pg_exception_url)
    """

    def run_list_ticker(self, entity_name: str, test_case: int = None):
        with open("/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGStockTracker/Data/ticker_url.json",
                  "r") as json_file:
            #return list(json.load(json_file).values())
            for _ind, _val in json.load(json_file).items():
                pgtracker.PGTracker().run(f"PG_{_ind}", self.pg_get_url_list_ticker(entity_name=_ind), self.pg_formatter_list_ticker)

        print(self._pg_exception_url)

    def run(self, entity_name: str, test_case: int = None):
        _test = self.pg_get_data(entity_name)
        print(_test)
        pgtracker.PGTracker().save_to_db(f"PG_{entity_name}_STATS", _test)


    def get_tasks(self, pg_data: Union[str, list, dict], pg_parameters: dict = {}) -> bool:
        pass

    def _process(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        pass

    def process(self, name: str = None, *args: object, **kwargs: object) -> bool:
        pass


if __name__ == '__main__':
    """
    a = [{'pg_stock_symbol': 'ASPCW', 'pg_AdjClose': 0.5300999879837036, 'pg_Close': 0.5300999879837036,
      'pg_High': 0.5300999879837036, 'pg_Low': 0.5300999879837036, 'pg_Open': 0.6200000047683716,
      'pg_Volume': 10000.0, 'pg_stock_timestamp': '2021-09-09', 'pg_time_created': 1631236310.774188},
         {'pg_stock_symbol': 'ASPCW', 'pg_AdjClose': 0.5300999879837036, 'pg_Close': 0.5300999879837036,
          'pg_High': 0.5300999879837036, 'pg_Low': 0.5300999879837036, 'pg_Open': 0.6200000047683716,
          'pg_Volume': 10000.0, 'pg_stock_timestamp': '2021-09-09', 'pg_time_created': 1631236310.774188}
         ]

    _aa = [(x) for y in a for _, x in y.items()]
    _bb = [tuple(x.values()) for x in a]

    print(_aa)
    print(_bb)

    exit(0)
    """
    #PGStockTracker().run_list_ticker("NASDAQ")
    #print(test.pg_get_url())
    #exit(0)
    PGStockTracker().run("NASDAQ")

