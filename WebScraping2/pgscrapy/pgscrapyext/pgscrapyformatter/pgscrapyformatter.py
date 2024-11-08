import os
import bs4
import inspect
from pathlib import Path
from pprint import pprint
import collections
import pandas as pd
from bs4 import BeautifulSoup
from pydantic.generics import GenericModel
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List, Dict
# from pgmeta.pggenericfunc import check_args
# from pgmeta import pgclassdefault, pggenericfunc
from Meta.pggenericfunc import check_args
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgregex
#from pgutils import pgregex
from pgscrapyext.pgscrapyformatter import pgscrapyformatterbase, pgscrapyformattercommon

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

def pg_explore_data(pg_data):
    _view = collections.defaultdict(list)
    def _explore(item, level):
        _view[level].append(type(item))
        if not isinstance(item, (bs4.element.NavigableString)):
            for child in item.children:
                _explore(child, level + 1)

    _explore(pg_data, 0)
    pprint(_view)


def pg_custom_opensea_sandbox_checker():
    return {"<h6": 6, "ago": 6, "Sale": 6, "(\$[0-9]+(\.[0-9]*)?)": 6}


def pg_custom_paniniamerica_checker():
    return {"(\$ [0-9]+(\.[0-9]*)?)": 6}


# def pg_custom_paniniamerica_activity_data_formatter(pg_record: List):
#     return {list(pg_record[0].values())[0]: f"{list(pg_record[1].values())[0]}:{list(pg_record[2].values())[0]}:{list(pg_record[2].values())[0]}:{list(pg_record[3].values())[0]}",
#             'player_name': list(pg_record[5].values())[0],
#             'serial_num': list(pg_record[6].values())[0],
#             'card_set': list(pg_record[7].values())[0],
#             list(pg_record[8].values())[0]: list(pg_record[9].values())[0],
#             list(pg_record[10].values())[0]: list(pg_record[11].values())[0],
#             list(pg_record[12].keys())[0]: list(pg_record[12].values())[0]}


def pg_custom_paniniamerica_activity_header_formatter(pg_record: List, pg_header: List = None):
    pg_header = ["image_url", "player_name", "serial_number", "price", "card_set", "buyer_name", "seller_name", "date_time"] if not pg_header else pg_header
    return {x[0]: list(x[1].values())[0] for x in zip(pg_header, pg_record)}



def pg_custom_paniniamerica_auction_header(pg_record: List, pg_header: List = None):
    _exclude_list = []
    pg_header = ["link", "end_in", "day", "hour", "minute", "second", "icon", "image_url", "player_name", "serial_number",
                 "card_set", "current_bid_text", "current_bid", "buy_now_text", "buy_now", "crypto_icon",
                 "rarity"] if not pg_header else pg_header
    return {x[0]: list(x[1].values())[0] for x in zip(pg_header, pg_record) if x[0] not in _exclude_list}

    #for item in pg_record:
    #    print(item)
    #return pg_record
    # _formatted_content = {}
    # _num_increment = 1
    # for _item in pg_record:
    #     for _key, _value in _item.items():
    #         _new_key = _key
    #         while _new_key in _formatted_content:
    #             _new_key = f"{_key}_{_num_increment}"
    #             _num_increment += 1
    #         _formatted_content = {**_formatted_content, **{_new_key: _value}}
    # return _formatted_content



def pg_default_formatter(pg_record: List):
    _formatted_content = {}
    _num_increment = 1
    for _item in pg_record:
        for _key, _value in _item.items():
            _new_key = _key
            while _new_key in _formatted_content:
                _new_key = f"{_key}_{_num_increment}"
                _num_increment += 1
            _formatted_content = {**_formatted_content, **{_new_key: _value}}
    return _formatted_content



class PGWebScrapingFormatter(pgscrapyformatterbase.PGScrapingFormatterBase, pgscrapyformattercommon.PGScrapingFormatterCommon):
    def __init__(self, project_name: str = "scrapingformatter", logging_enable: str = False):
        super(PGWebScrapingFormatter, self).__init__(project_name=project_name,
                                              object_short_name="PG_WS_FMR",
                                              config_file_pathname=__file__.split('.')[0] + ".ini",
                                              logging_enable=logging_enable,
                                              config_file_type="ini")

        ### Common Variables
        self._data = {}
        self._parameter = {}  # if test_flag set to true, percentage of data will be used as testing dataset

        ### Specific Variables
        self._data_inputs = {}
        self._data_validator = {"panini_activity": pg_custom_paniniamerica_checker,
                                "panini_auction": pg_custom_paniniamerica_checker}
        self._header_formatter = {"panini_activity": pg_custom_paniniamerica_activity_header_formatter,
                                  "panini_auction": pg_custom_paniniamerica_auction_header}


    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data



    # def pg_data_acquisition(self, filepath: str, custom_validate=None, top_data_num: int = 10):
    #     """
    #     mode for beautifulsoup: html.parser, html5lib
    #
    #     Other ideas:
    #         Get 5 layers deep
    #         Use bs4.element.Comment
    #     """
    #
    #     def _default_validate(pg_data_content: bs4.element.Tag):
    #         return False if not pg_data_content.text else True
    #
    #     def _find_num_record1(pg_data_content: Union[bs4.element.Tag, bs4.element.ResultSet],
    #                           expected_item_num: int = None):
    #         _cache = collections.defaultdict(tuple)
    #
    #         def _extract_stats(pg_element: bs4.element.Tag, level):
    #             _type = {"<class 'bs4.element.Tag'>": "tag",
    #                      "<'bs4.element.NavigableString'>": "navstring",
    #                      "<'bs4.element.Comment'>": "comment"
    #                      }
    #             if isinstance(pg_element, bs4.element.ResultSet):
    #                 for it in pg_element:
    #                     _cache[it] = (len(it), level + 1, _type.get(str(type(it)), "default"))
    #                     _extract_stats(it, level + 1)
    #             elif not isinstance(pg_element, (bs4.element.Comment, bs4.element.NavigableString)):
    #                 for it in pg_element.children:
    #                     _cache[it] = (len(it), level + 1, _type.get(str(type(it)), "default"))
    #                     _extract_stats(it, level + 1)
    #
    #         _extract_stats(pg_data_content, 0)
    #         pprint(sorted([x for x in _cache.values() if x[2] == "tag"]))
    #
    #         exit(0)
    #
    #
    #         _extracted = sorted([(len(_value), _index) for _index, _value in enumerate(pg_data_content[101])
    #                              if not isinstance(_value, bs4.element.Comment)
    #                              and not isinstance(_value, bs4.element.NavigableString)],
    #                             key=lambda x: (x[0], x[1]), reverse=True)
    #         print(_extracted)
    #         _test = pg_data_content[101]
    #         for index, item in enumerate(_test):
    #             if index == 2:
    #                 print(item)
    #                 print(type(item))
    #                 print(len(item))
    #                 if item.children:
    #                     print("aaaa")
    #                     for subitem in item.children:
    #                         for subitem1 in subitem.children:
    #                             print(subitem1, len(subitem1), type(subitem1))
    #                 print(item.children)
    #         # print(type(_test))
    #         # print(len(_test))
    #         exit(0)
    #
    #
    #
    #     def _find_num_record(pg_data_content: Union[bs4.element.Tag, bs4.element.ResultSet]):
    #         _extracted = sorted([(len(_value), _index) for _index, _value in enumerate(pg_data_content)
    #                              if not isinstance(_value, bs4.element.Comment)
    #                              and not isinstance(_value, bs4.element.NavigableString)
    #                              and custom_validate(_value)], key=lambda x: (x[0], x[1]), reverse=True)
    #         print(_extracted)
    #         return [x[1] for x in _extracted[:top_data_num]]
    #
    #     if not custom_validate:
    #         custom_validate = _default_validate
    #
    #     with open(filepath) as file:
    #         _soup = BeautifulSoup(file.read(), 'html.parser')
    #         _entire_finding = _soup.find_all("div")
    #
    #         _find_num_record1(_entire_finding)
    #
    #         exit(0)
    #
    #         #return _entire_finding, _find_num_record(_entire_finding)


    def pg_data_acquisition(self, filepath: str,
                            custom_validate=None,
                            top_data_num: int = 1):
        """
        mode for beautifulsoup: html.parser, html5lib

        Other ideas:
            Get 5 layers deep
            Use bs4.element.Comment
        """

        def _default_validate(pg_data_content: bs4.element.Tag):
            return False if not pg_data_content.text else True

        def _find_num_record(pg_data_content: Union[bs4.element.Tag, bs4.element.ResultSet],
                             validation_dict: Dict = None, top_data_num: int = 1):
            _cache = collections.defaultdict(tuple)


            def _validate_content(pg_content: str) -> bool:
                #print(validation_dict)
                if not validation_dict: return True
                for index, validation in validation_dict.items():
                    if pgregex.pg_count_overlapping(index, pg_content) < validation:
                        return False
                return True

            def _extract_stats(pg_element: bs4.element.Tag, level):
                _type = {"<class 'bs4.element.Tag'>": "tag",
                         "<'bs4.element.NavigableString'>": "navstring",
                         "<'bs4.element.Comment'>": "comment"
                         }
                if isinstance(pg_element, bs4.element.ResultSet):
                    for it in pg_element:
                        _cache[it] = (len(it), level + 1, _type.get(str(type(it)), "default"), it)
                        _extract_stats(it, level + 1)
                elif not isinstance(pg_element, (bs4.element.Comment, bs4.element.NavigableString)):
                    for it in pg_element.children:
                        _cache[it] = (len(it), level + 1, _type.get(str(type(it)), "default"), it)
                        _extract_stats(it, level + 1)

            _extract_stats(pg_data_content, 0)
            _cache_tag = sorted([x for x in _cache.values() if x[2] == "tag"], key=lambda x: x[0])

            _parsed_content = []
            for i in range(len(_cache_tag) - 1, -1, -1):
                #if _validate_content(str(_cache_tag[i][3])):
                if _validate_content(str(_cache_tag[i][3])):
                    _parsed_content.append((i, _cache_tag[i][3]))

            #print(_cache_tag[-2][3])
            #print(pgregex.pg_count_overlapping("<h6", _cache_tag[-2][3]))

            _sorted_parsed_content = sorted(_parsed_content, key=lambda x: len(str(x[1])) // len(x[1]))
            print([x[0] for x in _sorted_parsed_content])


            #print(pgregex.pg_count_overlapping("(\$[0-9]+(\.[0-9]*)?)", _parsed_content[0][1]))
            #print(_parsed_content[1])
            # for item in _sorted_parsed_content:
            #     print(f"-----> {type(item[1])}, {len(item[1])}, {len(str(item[1]))}, {len(str(item[1])) // len(item[1])}")

            return [_sorted_parsed_content[0][1]]


            #return [_parsed_content[12][1]]
            # exit(0)

            # print([(x[0], x[1], x[2]) for x in _cache_tag[len(_cache_tag) - 10:]])
            # print(_cache_tag[-2])
            # print(_cache_tag[len(_cache_tag) - 10:])
            # print(_cache_tag[-5])
            #
            # exit(0)
            #
            #return [_cache_tag[-2][3]]
            # if top_data_num >= len(_cache):
            #     return [x[-1] for x in _cache_tag]
            # else:
            #     return [x[-1] for x in _cache_tag[len(_cache_tag) - top_data_num:]]

            #print(sorted([x for x in _cache.values() if x[2] == "tag"], key=lambda x: x[0])[-top_data_num])

        with open(filepath) as file:
            _soup = BeautifulSoup(file.read(), 'html.parser')
            _entire_finding = _soup.find_all("div")

            return _find_num_record(_entire_finding, custom_validate())
            #return _entire_finding, _find_num_record(_entire_finding)



    def pg_scrapy_acquisition(self, pg_content, custom_validate=None, top_data_num: int = 1):
        """
        mode for beautifulsoup: html.parser, html5lib

        Other ideas:
            Get 5 layers deep
            Use bs4.element.Comment
        """

        def _default_validate(pg_data_content: bs4.element.Tag):
            return False if not pg_data_content.text else True

        def _find_num_record(pg_data_content: Union[bs4.element.Tag, bs4.element.ResultSet],
                             validation_dict: Dict = None, top_data_num: int = 1):
            _cache = collections.defaultdict(tuple)

            def _validate_content(pg_content: str) -> bool:
                #print(validation_dict)
                if not validation_dict: return True
                for index, validation in validation_dict.items():
                    if pgregex.pg_count_overlapping(index, pg_content) < validation:
                        return False
                return True

            def _extract_stats(pg_element: bs4.element.Tag, level):
                _type = {"<class 'bs4.element.Tag'>": "tag",
                         "<'bs4.element.NavigableString'>": "navstring",
                         "<'bs4.element.Comment'>": "comment"
                         }
                if isinstance(pg_element, bs4.element.ResultSet):
                    for it in pg_element:
                        _cache[it] = (len(it), level + 1, _type.get(str(type(it)), "default"), it)
                        _extract_stats(it, level + 1)
                elif not isinstance(pg_element, (bs4.element.Comment, bs4.element.NavigableString)):
                    for it in pg_element.children:
                        _cache[it] = (len(it), level + 1, _type.get(str(type(it)), "default"), it)
                        _extract_stats(it, level + 1)

            _extract_stats(pg_data_content, 0)
            _cache_tag = sorted([x for x in _cache.values() if x[2] == "tag"], key=lambda x: x[0])

            _parsed_content = []
            for i in range(len(_cache_tag) - 1, -1, -1):
                if _validate_content(str(_cache_tag[i][3])):
                    _parsed_content.append((i, _cache_tag[i][3]))

            _sorted_parsed_content = sorted(_parsed_content, key=lambda x: len(str(x[1])) // len(x[1]))
            print([x[0] for x in _sorted_parsed_content])

            return [_sorted_parsed_content[0][1]]

        return _find_num_record(pg_content, custom_validate())

    def pg_extract_data(self, pg_data, custom_parser=None):
        custom_parser = pg_default_formatter if custom_parser is None else custom_parser
        def _get_label(soup_tag: bs4.element.Tag):
            #print(f"inside of _get_label: {soup_tag}")
            # print(soup_tag['src'])
            # print(soup_tag['alt'])
            """
            check for image urls
            """

            if isinstance(soup_tag, (int, tuple, list, str, dict)):
                return {f"pg_generic_{type(soup_tag)}": soup_tag}
            if soup_tag.has_attr('src'):
                if soup_tag.has_attr('alt'):
                    return {soup_tag['alt'].replace(" ", "_"): soup_tag['src']}
                else:
                    return {f"image_url": soup_tag['src']}
            """
            check for links
            not implemented
            """
            # exit(0)
            # print(soup_tag.text, type(soup_tag.text))
            if isinstance(soup_tag.text, list):
                _text = ' '.join(soup_tag.text)
            else:
                _text = soup_tag.text
            if soup_tag.has_attr('class') and soup_tag['class']:
                return {soup_tag['class'][0]: _text.replace("\n", " ").replace("\t", " ")}
            else:
                return {f"pg_generic_{type(_text)}": _text.replace("\n", " ").replace("\t", " ")}

        def _traverse(prev, current):
            nonlocal _record_index
            # print(type(current))
            if isinstance(current, (int, str)):
                _result[_record_index].append(_get_label(current))
            elif isinstance(current, (bs4.element.Comment, bs4.element.NavigableString)):
                # _result[_record_index].append(prev)
                _result[_record_index].append(_get_label(prev))
            elif current and isinstance(current, bs4.element.Tag) and (not current.children or not list(current.children)):
                # _result[_record_index].append(current)
                #print(f"inside _travese111: {current}")
                _result[_record_index].append(_get_label(current))
            # elif current.children and not list(current.children):
            #     _result[_record_index].append(_get_label(current))
            else:
                #if not isinstance(current, (bs4.element.NavigableString)):
                #print(f"current: {current} {list(current.children)}")
                for it in current.children:
                    #print(f"inside _travese: {it} and type is {type(it)}")
                    _traverse(current, it)

        _result = collections.defaultdict(list)
        print(f"total records in pg_data: {len(pg_data)}")

        for data_item in pg_data:
            for _record_index, item in enumerate(data_item):
                #print(item)
                if not isinstance(item, (bs4.element.Comment, bs4.element.NavigableString)):
                    # print(type(item))
                    #print(list(item.children)[0])
                    _traverse(None, item)
                    #print(_result[_record_index])
                    _result[_record_index] = custom_parser(_result[_record_index])
        return _result

    def pg_extract_data1(self, pg_data, custom_parser=None):
        custom_parser = pg_default_formatter if custom_parser is None else custom_parser
        def _get_label(soup_tag: bs4.element.Tag):
            #print(f"this is the soup_tag:    --------->  {soup_tag}")

            #print(f"inside of _get_label: {soup_tag}")
            # print(soup_tag['src'])
            # print(soup_tag['alt'])
            """
            check for image urls
            """

            if isinstance(soup_tag, (int, tuple, list, str, dict)):
                return {f"pg_generic_{type(soup_tag)}": soup_tag}

            # if soup_tag.children:
            #     print("true")
            # else:
            #     print("false")

            if soup_tag.has_attr('src'):
                if soup_tag.has_attr('alt'):
                    return {soup_tag['alt'].replace(" ", "_"): soup_tag['src']}
                else:
                    return {f"image_url": soup_tag['src']}
            """
            check for links
            """
            if soup_tag.has_attr('href'):
                print(soup_tag['href'])
                return {f"links": soup_tag['href']}

            # exit(0)
            # print(soup_tag.text, type(soup_tag.text))

            if isinstance(soup_tag.text, list):
                _text = ' '.join(soup_tag.text)
            else:
                _text = soup_tag.text
            if soup_tag.has_attr('class') and soup_tag['class']:
                return {soup_tag['class'][0]: _text.replace("\n", " ").replace("\t", " ")}
            else:
                return {f"pg_generic_{type(_text)}": _text.replace("\n", " ").replace("\t", " ")}

        def _traverse(prev, current):
            nonlocal _record_index
            # if prev is None:
            #     print(f" in _traverse, the current ............{current} and type ......... {type(current)}")
            if isinstance(current, (int, str)):
                _result[_record_index].append(_get_label(current))
            elif isinstance(current, (bs4.element.Comment, bs4.element.NavigableString)):
                # _result[_record_index].append(prev)
                _result[_record_index].append(_get_label(prev))
            elif current and isinstance(current, bs4.element.Tag) and (not current.children or not list(current.children)):
                # _result[_record_index].append(current)
                #print(f"inside _travese111: {current}")
                _result[_record_index].append(_get_label(current))
            # elif current.children and not list(current.children):
            #     _result[_record_index].append(_get_label(current))
            else:
                #if not isinstance(current, (bs4.element.NavigableString)):
                #print(f"current: {current} {list(current.children)}")
                # if prev is None:
                #     print(f" in _traverse, the current ............{list(current.children)} and type ......... {type(current.children)}")
                if isinstance(current, bs4.element.Tag) and current.has_attr('href'):
                    _result[_record_index].append(_get_label(current))
                for it in current.children:
                    # if prev is None:
                    #     print(f" in _traverse, the current xxxxxxxxxxxx{it} and number of children {len(it)} and type xxxxxxxxx {type(it)}")
                    #     print(f" in _traverse, the current ............{list(it.children)} and type ......... {type(it)}")
                    #print(f"inside _travese: {it} and type is {type(it)}")
                    _traverse(current, it)

        _result = collections.defaultdict(list)
        print(f"total records in pg_data: {len(pg_data)}")

        for data_item in pg_data:
            for _record_index, item in enumerate(data_item):
                #print(item)
                if not isinstance(item, (bs4.element.Comment, bs4.element.NavigableString)):
                    # print(type(item))
                    #print(list(item.children)[0])
                    #print(f"this is the item <---------  {item}")
                    _traverse(None, item)
                    #print(f"aaaaaa{_result[_record_index]}bbbbbbb")
                    #print(_result[_record_index])
                    _result[_record_index] = custom_parser(_result[_record_index])
        return _result

    def test_case(self, dirpath: str, filename: str, custom_record_validate=None, custom_header=None):
        Path(os.path.join(dirpath, filename))

        if Path.exists:
            # _pg_data_content, _pg_data_index = data_acquisition(os.path.join(dirpath, filename))
            #_pg_data_content, _pg_data_index = self.pg_data_acquisition(os.path.join(dirpath, filename))
            _pg_data_content = self.pg_data_acquisition(os.path.join(dirpath, filename), custom_record_validate)
            #self.pg_extract_data(_pg_data_content, pg_custom_paniniamerica_formatter)


            #self.pg_extract_data(_pg_data_content, pg_custom_paniniamerica_recent_sales_formatter)

            return self.pg_extract_data(_pg_data_content, custom_header)

    def scrapy_run(self, pg_content, custom_data_validator: str = None, custom_header_formatter: str =None):

            # _pg_data_content = self.pg_scrapy_acquisition(pg_content, custom_record_validate)
            # return self.pg_extract_data(_pg_data_content, custom_header)
        print(f"formatter is {custom_data_validator} <--> {custom_header_formatter}")
        _pg_data_content = self.pg_scrapy_acquisition(pg_content, self._data_validator.get(custom_data_validator))
        print("testingtestingtestingtestingtestingtesting")
        #print(_pg_data_content)
        print("testingtestingtestingtestingtestingtesting")
        return self.pg_extract_data1(_pg_data_content, self._header_formatter.get(custom_header_formatter))
        # _pg_data_content = self.pg_scrapy_acquisition(pg_content, pg_custom_paniniamerica_checker)
        # return self.pg_extract_data(_pg_data_content, pg_custom_paniniamerica_activity_header_formatter)

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

    def _process(self, pgdataset, parameters: dict, *args, **kwargs) -> Union[float, int, None]:

        check_args(inspect.currentframe().f_code.co_name,
                   {'pgdataset': pgdataset, 'parameters': parameters, }, False)

        try:
            if "prediction" in parameters:
                if not self._val_dimension(pgdataset.to_numpy()[:, : -1], parameters['prediction'].to_numpy()):
                    pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                  "error!! dimension mismatch between training dataset and prediction dataset ")

                if not self._val_min_record(parameters['prediction'], self._min_record_cnt_4_pred):
                    pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                  f"error!! minimum record is less than {self._min_record_cnt_4_pred}")

            _load_model = self.model_load(parameters['name'], {**self._parameter, **parameters})
            _model = _load_model['model'] if _load_model and "model" in _load_model else None

            if not _model:
                print(f"model not found, start training a model for the dataset")
                _model = self.model_train(pgdataset, parameters['validation'], parameters['num_classes'])
                exit(0)
                self.model_save(model=_model, entity_name=parameters['name'], data={'sample_data': pgdataset})

            if "prediction" in parameters:
                return _model.predict(parameters['prediction'])

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def process(self, name: str = None, *args: object, **kwargs: object) -> bool:
        try:
            if name:
                _item = self._data_inputs[name]
                self._data[name] = self._process(_item['data'], _item['parameter'], )
            else:
                for _index, _data in self._data_inputs.items():
                    _item = self._data_inputs[_index]
                    self._data[_index] = self._process(_item['data'], _item['parameter'])
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


class PGWebScrapingFormatterExt(PGWebScrapingFormatter):
    def __init__(self, project_name: str = "scrapingformatterext", logging_enable: str = False):
        super(PGWebScrapingFormatterExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGWebScrapingFormatterSingleton(PGWebScrapingFormatter):
    __instance = None

    @staticmethod
    def get_instance():
        if PGWebScrapingFormatterSingleton.__instance == None:
            PGWebScrapingFormatterSingleton()
        else:
            return PGWebScrapingFormatterSingleton.__instance

    def __init__(self, project_name: str = "scrapingformattersingleton", logging_enable: str = False):
        super(PGWebScrapingFormatterSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGWebScrapingFormatterSingleton.__instance = self


if __name__ == '__main__':
    test = PGWebScrapingFormatter()
    _pg_file_dir = "/home/pant/anaconda3/envs/webscraping/pgweb1/Data/"
    test.test_case(_pg_file_dir, "test31.html", pg_custom_paniniamerica_checker, pg_custom_paniniamerica_recent_sales_formatter)
    #test.test_case(_pg_file_dir, "test42.html", pg_custom_opensea_sandbox_checker)

    exit(0)
    test.set_profile("cloud_storage")
    train_dataset, eval_dataset, num_classes = training_dataset_classification = test.data_gen_dataset(10)
    test.get_tasks(train_dataset, {'name': "city20", 'validation': eval_dataset, 'num_classes': num_classes})
    test.process()









