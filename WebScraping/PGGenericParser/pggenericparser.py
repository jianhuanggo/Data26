import os
import re
import json
import time
import inspect
import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from collections import Counter
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
from transformers import AutoModelForTokenClassification, AutoTokenizer
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from Meta import pggenericfunc
from Data.Connect import pgdatabase
from WebScraping import pgwebscrapingbase, pgwebscrapingcommon

__version__ = "1.8"


def remove_space_dict(pg_dict):
    return {x.replace(" ", ""): y for x, y in pg_dict.items()}


class PGGenericparser(pgwebscrapingbase.PGWebScrapingBase, pgwebscrapingcommon.PGWebScrapingCommon):
    def __init__(self, project_name: str = "pggenericparser", logging_enable: str = False):
        super().__init__(project_name=project_name,
                         object_short_name="PG_GPS",
                         config_file_pathname=__file__.split('.')[0] + ".ini",
                         logging_enable=logging_enable,
                         config_file_type="ini")

        ### Common Variables
        self._name = "pgskt"

        self._pg_api_key = []

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

    def data_acquisition(self, filepath: str, top_data_num: int = 1):
        try:
            with open(filepath) as file:
                x = file.read()
                soup = BeautifulSoup(x, 'html.parser')
                y = soup.find_all("div")

                ### get 5 layer depth
                a = sorted([(len(x), num) for num, x in enumerate(y)], key=lambda x: x[0], reverse=True)
                print(a)
                return y, [x[1] for x in a[:top_data_num]]
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None, []

    def data_acquisition1(self, filepath: str, top_data_num: int = 1):
        try:
            result = []
            with open(filepath) as file:
                x = file.read()
                soup = BeautifulSoup(x, 'html.parser')
                y = soup.find_all("div")

                for _index, item in enumerate(y):
                    result.append((item, 1, _index, len(item)))

                print([x[1:] for x in result])

                _result1 = [x[0] for x in result]
                # print(_result1)
                print(len(result))

                for _index1, item1 in enumerate(_result1):
                    result.append((item1, 2, _index1, len(item)))

                print([x[1:] for x in result])
                print(len(result))

                a = sorted([x for x in result], key=lambda x: x[3], reverse=True)
                print([x[1:] for x in a])
                print(len(a))
                exit(0)

                ### get 5 layer depth
                a = sorted([(len(x), num) for num, x in enumerate(y)], key=lambda x: x[0], reverse=True)
                print(a)
                return y, [x[1] for x in a[:top_data_num]]
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None, []

    def data_acquisition2(self, filepath: str, top_data_num: int = 1):
        try:
            with open(filepath) as file:
                x = file.read()
                soup = BeautifulSoup(x, 'html.parser')
                y = soup.find_all("div")

                ### get 5 layer depth
                a = sorted([(self.get_count(str(x), 'div'), num) for num, x in enumerate(y)], key=lambda x: x[0], reverse=True)
                print(a)
                # print(y[241])
                # b = a[0][1]
                # print(y[b])
                # exit(0)
                return y, [x[1] for x in a[:top_data_num]]
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None, []

    @staticmethod
    def extract(label, elements):
        # print({f"{label}_{_ind}": _val.text for _ind, _val in enumerate(elements) if _val.text})
        return {f"{label}_{_ind}": _val.text for _ind, _val in enumerate(elements) if _val.text}

    def extract_v2(self, label, elements):
        _total = []
        while True:
            try:
                if isinstance(elements, str):
                    return [(label, elements)]
                elif len(elements) == 0:
                    if label == "img":
                        _elem = elements['src'] or elements.find_all('img')['src']
                        return [(label, _elem)]
                    return [(label, elements.text)]
                elif len(elements) == 1:
                    if label == "img":
                        _elem = elements[0]['src'] or elements.find_all('img')['src']
                        return [(label, _elem)]
                    return [(label, elements.text)]
                else:
                    for sub_item in elements:
                        _total += self.extract_v2(label, sub_item)
                    return _total
            except Exception as err:
                continue

    ### link extractor
    def extract_3(self, label, elements):
        _total = []
        while True:
            try:
                for _a in elements.find_all(label, href=True):
                    if _a.text:
                        _total.append((label, _a['href']))
                        # print(_a['href'])
                return _total
            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
                continue

    def extract_old(self, label, elements):
        _total = []
        if isinstance(elements, str):
            return [elements]
        elif len(elements) == 1:
            return [elements.text]
        else:
            for sub_item in elements:
                _total += self.extract_old(label, sub_item)
            return _total

    @staticmethod
    def f7(seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]

    @staticmethod
    def pg_to_str(pg_data):
        if isinstance(pg_data, (list, tuple)):
            return '_'.join(str(pg_data))
        else:
            return str(pg_data)

    def pg_detect_price(self, pg_data):
        # _price_regx = "(?<=\s\$)(\d+)(?=\s)"
        # _price_regx = f"\$\d*[.,]?\d*"
        # _removal_character = ['(', ')']
        _pg_data = pg_data if isinstance(pg_data, str) else self.pg_to_str(pg_data)
        # print(_pg_data)
        with open('/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/currency.json',
                  'r') as file:
            _pg_currency = json.load(file)
            for _curr in [_val["symbol"] for _, _val in _pg_currency.items()]:
                _price_regx = f"{_curr}\d*[.,]?\d*"
                _result = ''.join(re.findall(_price_regx, _pg_data))
                # print(_result)
                # print(len(_result))
                # print(len(pg_data) * 0.5)
                if len(_result) > len(pg_data) * 0.5:
                    return True
                # print(_pg_data)
                # print(re.findall(_price_regx, _pg_data))
                # print(re.search(r'(?<=\s\$)(\d+)(?=\s)', _pg_data, re.IGNORECASE))
                # print(re.match(_price_regx, _pg_data, re.IGNORECASE))

                # if _pg_data and {_val["symbol"]: _ind for _ind, _val in enumerate(_pg_currency.values())}.get()
                # print({_val["symbol"]: _ind for _ind, _val in enumerate(_pg_currency.values())})
        return False

    @staticmethod
    def assign_label(data: str):
        tokenizer = AutoTokenizer.from_pretrained("dslim/bert-base-NER")
        model = AutoModelForTokenClassification.from_pretrained("dslim/bert-base-NER")

        nlp = pipeline("ner", model=model, tokenizer=tokenizer)
        example = "My name is Wolfgang and I live in Berlin"

        ner_results = nlp(example)
        print(ner_results)

        # print(pg_detect_price("($0.28)"))
        # exit(0)

    @staticmethod
    def pg_detect_default(pg_data):
        return True

    def pg_generate_label(self, pg_index, pg_data, pg_num):
        _pg_label_func = {'0': (self.pg_detect_price, f"price_{pg_num}"),
                          '1': (self.pg_detect_default, f"text_{pg_num}")
                          }
        for _, func in _pg_label_func.items():
            if func[0](pg_data):
                return func[1]

    def f8(self, seq):
        seen = set()
        seen_add = seen.add
        return {self.pg_generate_label(x[0], x[1], ind): self.pg_to_str(x[1]) for ind, x in enumerate(seq) if
                x[1] and not (x[1] in seen or seen_add(x[1]))}

    def extract_data(self, pg_data):
        _summary = []
        for index, item in enumerate(pg_data):
            try:
                _data = []
                # print(item)
                # print(f"item: {item}")
                # print(f"tags: {self.f7(sorted(tag.name for tag in item.find_all()))}")
                for x in self.f7(sorted((tag.name for tag in item.find_all()))):
                    for sub_item in item.find_all(x):
                        _data += self.extract_v2(x, sub_item)

                for x in ('a', 'div'):
                    _result = self.extract_3(x, item)
                    _data += _result if _result else []

                # print(set(_data + extract_v3("div", item)))

                print(self.f8(_data))
            except Exception as err:
                continue
            # exit(0)
            # print({f"attrib_{_ind}": _val for _ind, _val in enumerate(set(_data)) if _val})
        print(_summary)

        # summarizer = pipeline("summarization")
        # print(summarizer("Sam Shleifer writes the best docstring examples in the whole world", min_length=5, max_length=20))

    @staticmethod
    def get_count(pg_data, pg_tag):
        return pg_data.count(f"<{pg_tag}")


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

    def run(self, dirpath: str, filename: str):
        Path(os.path.join(dirpath, filename))

        if Path.exists:
            _pg_data, _pg_data_index = self.data_acquisition(os.path.join(dirpath, filename))
            print(_pg_data_index)
            for item in _pg_data_index:
                self.extract_data(_pg_data[item])

    def get_tasks(self, pg_data: Union[str, list, dict], pg_parameters: dict = {}) -> bool:
        pass

    def _process(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        pass

    def process(self, name: str = None, *args: object, **kwargs: object) -> bool:
        pass


if __name__ == '__main__':
    # assign_label()
    # exit(0)
    test = PGGenericparser()

    _pg_file_dir = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/Finance/PGPaniniTracker/Data/"
    # Path(os.path.join(_pg_file_dir, "selenium.txt"))
    test.run(_pg_file_dir, "selenium.txt")
    test.run(_pg_file_dir, "selenium.txt.backup.08192021")
    test.run(_pg_file_dir, "ad8ef04b_selenium.txt")







