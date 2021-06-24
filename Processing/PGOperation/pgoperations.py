import types
import pickle
import inspect
import itertools
import asyncio
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures
from typing import Callable, Tuple
from Meta import pgclassdefault
from Meta import pggenericfunc
from Processing import pgprocessingbase
from Data.Utils import pgfile

__version__ = "1.8"


class PGOperation(pgprocessingbase.PGProcessingBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str = "pgoperation", logging_enable: bool = False):
        """
        Args:
            project_name (int): The first parameter.
            logging_enable (bool): The second parameter.

        """
        super(PGOperation, self).__init__(project_name=project_name,
                                          object_short_name="PG_OP",
                                          config_file_pathname=__file__.split('.')[0] + ".ini",
                                          logging_enable=logging_enable,
                                          config_file_type="ini")

        self._pg_private = {}
        self._setting_doc = """   
            Available Setting:
                num_workers (int):  the number of workers allocated
                chunksize (int): if info is available: len(tasks) // num_workers, 
                                 if expensive to size of data, set it to 1
                            """
        self.set_profile("default")

    def pg_operation_run(self, method_dict: dict,
                         method_cnt: str,
                         method_validator: Callable,
                         method_parameters: dict = {}) -> Tuple[str, dict]:

        try:
            print(f"method_dict: {method_dict}")
            print(f"method_cnt: {method_cnt}")
            print(f"method_validator: {method_validator}")
            print(f"method_parameter: {method_parameters}")

            if "entity_name" in method_parameters and pgfile.isfileexist(f"{self._parameter['saved_dir']}/{method_parameters['entity_name']}.pickle"):
                with open(f"{self._parameter['saved_dir']}/{method_parameters['entity_name']}.pickle", 'rb') as method_handle:
                    method_dict = pickle.load(method_handle)

            while int(method_cnt) < len(method_dict):
                print(f"attempt {method_cnt}: {method_dict.get(method_cnt, 'no more methods')}")
                if method_validator(method_dict.get(method_cnt, "no more methods")):
                    if method_cnt != '0':
                        method_dict['0'], method_dict[method_cnt] = method_dict[method_cnt], method_dict['0']
                    with open(f"{self._parameter['saved_dir']}/{method_parameters['entity_name']}.pickle",
                              'wb') as method_handle:
                        pickle.dump(method_dict, method_handle)
                    return method_dict.get(method_cnt)
                else:
                    method_cnt = str(int(method_cnt) + 1)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

    def run(self):
        pass


if __name__ == '__main__':
    test = PGOperation()
    print(test._parameter)



