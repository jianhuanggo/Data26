import re
import sys
import json
import inspect
import pandas as pd
from typing import Union, Tuple, Callable, Iterable, Dict
from types import SimpleNamespace
from redfin import Redfin
from Data.Utils import pgyaml
from Data.Utils import pgdirectory
from Meta import pggenericfunc
from Data.Utils import pgoperation
from Meta import pgclassdefault
from pprint import pprint
from API.RealEstate import pgrealestatebase
from Processing.PGOperation import pgoperations


__version__ = "1.7"


class PGRedfin(pgrealestatebase.PGRealestateBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str = "redfin", logging_enable: str = False):

        super(PGRedfin, self).__init__(project_name=project_name,
                                       object_short_name="PG_RF",
                                       config_file_pathname=__file__.split('.')[0] + ".ini",
                                       logging_enable=logging_enable,
                                       config_file_type="ini")

        ### Common Variables
        self._name = "redfin"
        self._data = {}
        self._data_inputs = {}
        self._pattern_match = {'address_parser': {'prefix': "",
                                                  'surfix': " \d{5}"
                                                  }
                              }

        ### Specific Variables
        self._client = Redfin()
        self._detail_info = None
        self._property_id = None
        self._listing_id = None
        self._operation = pgoperations.PGOperation()

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    @property
    def client(self):
        return self._client

    @property
    def property_id(self):
        return self._property_id

    @property
    def detail_info(self):
        return self._detail_info

    @property
    def listing_id(self):
        return self._listing_id

    @pgoperation.pg_retry(3)
    def create_client(self, full_addr: str) -> bool:
        """Returns True if a redfin client is created successfully otherwise False.

        Args:
            full_addr: A iterable of addresses.
            For example: ["940 Tiverton Ln, Johns Creek GA 30022", "10607 Bent Tree Vw, Johns Creek GA 30097", ...]

        Returns:
            The return value. True for success, False otherwise.

        """
        try:
            _data = self._client.initial_info(self._client.search(full_addr)['payload']['exactMatch']['url'])['payload']
            if _data:
                self._property_id = _data['propertyId']
                self._listing_id = _data['listingId']
                self._detail_info = self._client.below_the_fold(self._property_id)
                return True
            else:
                return False
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          err, "Please make sure address is correct")
            return False

    def get_property_txn_history(self) -> list:
        _data = []
        if not self._detail_info:
            self._detail_info = self._client.below_the_fold(self._property_id)
        else:
            try:
                for item in self._detail_info['payload']['propertyHistoryInfo']['events']:
                    _data_item = "{" + f"\"price\": \"{item.get('price', 'NA')}\", " \
                                       f"\"eventDescription\": \"{item.get('eventDescription', 'NA')}\", " \
                                       f"\"mlsDescription\": \"{item.get('mlsDescription', 'NA')}\", " \
                                       f"\"eventDate\": \"{item.get('eventDate', 'NA')}\", " \
                                       f"\"eventDateString\": \"{item.get('eventDateString', 'NA')}\"" + "}"
                    #print(_data_item)
                    _data.append(json.loads(_data_item))
                return _data
            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger,
                                              inspect.currentframe().f_code.co_name,
                                              err)
                return []

    def get_prop_attr(self) -> dict:
        _data = []
        if not self._detail_info:
            self._detail_info = self._client.below_the_fold(self._property_id)
        else:
            return {**self._detail_info['payload']['publicRecordsInfo']['addressInfo'],
                    **self._detail_info['payload']['publicRecordsInfo']['basicInfo']}

    def get_school_info(self) -> dict:
        if not self._detail_info:
            self._detail_info = self._client.below_the_fold(self._property_id)
        else:
            #for item in self._detail_info['payload']['schoolsAndDistrictsInfo']['servingThisHomeSchools']:
            #print(list(item.keys()))

            return {k: v for d in [{f"g{'_'.join(item['gradeRanges'].lower().split())}": item['greatSchoolsRating'],
                                        f"p{'_'.join(item['gradeRanges'].lower().split())}": item['parentRating']} for item in
                                       self._detail_info['payload']['schoolsAndDistrictsInfo']['servingThisHomeSchools']] for k, v in d.items()}

    """
    def data_extract(self, input_string: str, city_name: str, state_abbr: str, method_cnt: str) -> str:
        method_dict = {'0': input_string.split(',')[0] + ', ' + ' '.join([i for i in input_string.split(',')[1].split() if i.isdigit()]),
                       '1': input_string.replace(f"{city_name.replace('-', ' ')}, {state_abbr} ", ""),
                       '2': re.search(f"{self._pattern_match['address_parser']['prefix']}(.*){self._pattern_match['address_parser']['surfix']}", input_string).group(1),
                       '3': input_string.replace("Unit ", "#")
                      }

        try:
            print(f"attempt {method_cnt}: {method_dict.get(method_cnt, 'no more methods')}")
            return method_dict.get(method_cnt, "no more methods")
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
    """
    def get_tasks(self, pg_dataset: Union[pd.DataFrame, dict, str, list, tuple], parameters: dict = None) -> bool:
        try:

            if isinstance(pg_dataset, str):
                if "read_func" in parameters:
                    self._data_inputs[parameters['name']] = parameters.get("read_func")(pg_dataset)
                else:
                    with open(pg_dataset) as read_file:
                         self._data_inputs[parameters['name']] = json.load(read_file)
            elif isinstance(pg_dataset, dict):
                self._data_inputs[parameters['name']] = {'parameter': parameters,
                                                         'data': pd.DataFrame.from_dict(pg_dataset, orient='index')}
            elif isinstance(pg_dataset, pd.DataFrame):
                self._data_inputs[parameters['name']] = {'parameter': parameters, 'data': pg_dataset}
            elif isinstance(pg_dataset, (list, tuple)):
                self._data_inputs[parameters['name']] = pg_dataset

            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _process(self, pg_dataset: str, pg_parameters: dict = None, *args: object, **kwargs: object) -> bool:
        """
            1.  Transform input addresses into a format of "<Address>, <Zipcode>"
                For example, "940 Tiverton Ln, 30022"
            2.  Invoke redfin API via 3rd party library to obtain full transaction details of property
            3.  The extracted data will be saved into self._data as a dictionary
                self._data = {"<prefix>_<unique id>": [{'<attribute 1>': '<value 1>',
                                                        '<attribute 2>': '<value 2>',
                                                        ...
                                                        },
                                                        ...
                                                        {'<attribute 1>': '<value 1>',
                                                        '<attribute 2>': '<value 2>',
                                                        ...
                                                        }]
            4.  Prefix will be used as a table name if not specified otherwise

        Args:
            pg_dataset: A iterable of addresses.
            For example: ["940 Tiverton Ln, Johns Creek GA 30022", "10607 Bent Tree Vw, Johns Creek GA 30097", ...]

        Returns:
            Returns True if the input iterable is processed successful otherwise False.

        """
        def method_validator(input_string: str) -> bool:
            self.create_client(full_addr=input_string)
            return True if self.property_id and self.listing_id else False

        try:
            _city_name, _state_abbr = self.city_state_parser(pg_dataset)
            method_cnt = 0
            """
            while True:

                _address = self.data_extract(input_string=pg_dataset,
                                             city_name=_city_name,
                                             state_abbr=_state_abbr,
                                             method_cnt=str(method_cnt).strip())
                
                self.create_client(full_addr=_address)
                print(f"property id: {self.property_id}")
                print(f"listing id: {self.listing_id}")
                if (self.property_id and self.listing_id) or _address == "no more methods":
                    break

                method_cnt += 1
            """
            method_dict = {'0': pg_dataset.split(',')[0] + ', ' + ' '.join([i for i in pg_dataset.split(',')[1].split() if i.isdigit()]),
                           '1': pg_dataset.replace(f"{_city_name.replace('-', ' ')}, {_state_abbr} ", ""),
                           '2': re.search(f"{self._pattern_match['address_parser']['prefix']}(.*){self._pattern_match['address_parser']['surfix']}", pg_dataset).group(1),
                           '3': pg_dataset.replace("Unit ", "#")
                           }

            _ = self._operation.pg_operation_run(method_dict, str(method_cnt).strip(), method_validator)

            if not self.property_id:
                if "exception_dir" in self._config.parameters["config_file"]['default']:
                    if not pgdirectory.createdirectory(self._config.parameters['config_file']['default']['exception_dir']):
                        sys.exit(99)
                    with open(f"{self._config.parameters['config_file']['default']['exception_dir']}/{_city_name.replace(' ', '-')}_exception_4_get_detail_info.txt", 'a') as except_file:
                        except_file.write(f"{str(pg_dataset)}\n")
            else:
                self._data[f"re_house_{self.property_id}"] = [{'property_id': self.property_id, 'address': str(pg_dataset)}]
                self._data[f"re_house_txn_detail_{self.property_id}"] = [{**{'property_id': self.property_id}, **x} for x in self.get_property_txn_history()]
                self._data[f"re_house_attr_detail_{self.property_id}"] = [{**{'property_id': self.property_id}, **self.get_prop_attr()}]
                self._data[f"re_house_school_detail_{self.property_id}"] = [{**{'property_id': self.property_id}, **self.get_school_info()}]

            self._property_id = None
            self._listing_id = None
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False
        #addr_cnt += 1
        #file_stats[filename] = addr_cnt

    def process(self, pg_dataset: Union[Iterable, str], *args: object, **kwargs: object) -> bool:
        """Returns True if the input iterable is processed successful otherwise False.

        Args:
            iterable: A iterable of addresses.
            For example: ["940 Tiverton Ln, Johns Creek GA 30022", "10607 Bent Tree Vw, Johns Creek GA 30097", ...]

        Returns:
            The return value. True for success, False otherwise.

        """
        if isinstance(pg_dataset, str):
            return self._process(pg_dataset, *args, **kwargs)
        elif isinstance(pg_dataset, Iterable):
            for item in pg_dataset:
                self._process(item, *args, **kwargs)
            return True
        else:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "Something is wrong")
            return False

    def city_state_parser(self, address: str) -> Tuple[str, str]:
        """
        10125 Barston Ct, Johns Creek, GA 30022

        """

        if address.count(',') == 1:
            _interested_part = [i for i in address.split(',')[1].split() if i.isalpha()]
            return ' '.join(_interested_part[:-1]).strip(), _interested_part[-1].strip()

        elif address.count(',') == 2:
            return address.split(',')[1].strip(), address.split(',')[2].split()[0].strip()
        else:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "address format is not recognizable")

    #def __repr__(self) -> str:
    #    return f"{type(self).__name__}({self._project_name})"


class PGRedfinExt(PGRedfin):
    def __init__(self, project_name: str = "redfinext", logging_enable: str = False):
        super(PGRedfinExt, self).__init__(project_name=project_name, logging_enable=logging_enable)


class PGRedfinSingleton(PGRedfin):

    __instance = None

    @staticmethod
    def get_instance():
        if PGRedfinSingleton.__instance == None:
            PGRedfinSingleton()
        else:
            return PGRedfinSingleton.__instance

    def __init__(self, project_name: str = "redfinsingleton", logging_enable: str = False):
        super(PGRedfinSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGRedfinSingleton.__instance = self




"""
def house_detail(property_id: dict, detail_data: list):
    # print(type(property_id))
    # print(type(detail_data[0]))
    if property_id and detail_data:
        temp = []
        for detail in detail_data:
            temp.append({**property_id, **detail})
        # print(temp)
        save_to_db("re_house_detail", temp)
"""


if __name__ == '__main__':
    test = PGRedfinExt()
    #print(PGRedfinExt().city_state_parser("10125 Barston Ct, Johns Creek, GA 30022")[0])


    #if "exception_dir" in test._config.parameters["config_file"]['default']:
    #    print(True)
    #print(test._config.parameters["config_file"]['default']['exception_dir'])


    #test.create_client("1303 Red Deer Way, 30022")
    #test.create_client("200 Woodscape Ct, 30022")
    test.create_client("11000 Glenbarr Dr, 30097")

    print(test.get_property_txn_history())
    print(test.get_prop_attr())
    print(test.get_school_info())




