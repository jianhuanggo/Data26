import json
import inspect
from Meta import pggenericfunc
from abc import ABCMeta, abstractmethod

__version__ = "1.8"


class PGValidateBase(metaclass=ABCMeta):
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
                # _content = json_file.read()
                return json.loads(json_file.read().replace("][", ","))
                # else:
                #     return json.load(json_file)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None
