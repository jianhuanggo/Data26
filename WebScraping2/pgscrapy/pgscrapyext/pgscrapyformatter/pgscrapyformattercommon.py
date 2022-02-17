import json
import inspect
import joblib
import tarfile
import pandas as pd
from typing import Callable, Iterator, TypeVar, Optional, Type, Union
# from pgmeta import pggenericfunc, pgclassdefault
# from pgmeta.pggenericfunc import check_args
# from pgutils import pgoperation, pgfile
from Meta import pggenericfunc, pgclassdefault
from Meta.pggenericfunc import  check_args
from Data.Utils import pgoperation, pgfile


class PGScrapingFormatterCommon(pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str,
                 object_short_name: str,
                 config_file_pathname: str,
                 logging_enable: str,
                 config_file_type: str):

        super().__init__(project_name=project_name,
                         object_short_name=object_short_name,
                         config_file_pathname=config_file_pathname,
                         logging_enable=logging_enable,
                         config_file_type=config_file_type)


    def get_func(self, ):
        return

    #@pgoperation.pg_retry(2)
    def _json_dump(self, data: any, filepath: str) -> bool:
        """Returns True if data is json serialized and persisted

        Args:
            data: Any object which can be serialized into json
            filepath: Absolute file path where object should be persisted

        Returns:
            The return value. True for success, False otherwise.

        """
        try:
            check_args(inspect.currentframe().f_code.co_name,
                       {'data': data, 'filepath': filepath}, False)

            with open(filepath, 'w') as file_write:
                json.dump(data, file_write)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    @pgoperation.pg_retry(2)
    def _df_dump(self, data: pd.DataFrame, filepath: str) -> bool:
        """Returns True if dataframe are successfully persisted.

        Args:
            data: A dataframe object
            filepath: Absolute file path where object should be persisted

        Returns:
            The return value. True for success, False otherwise.

        """
        try:
            check_args(inspect.currentframe().f_code.co_name,
                       {'data': data, 'filepath': filepath}, False)

            data.to_csv(filepath, index=False)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    @pgoperation.pg_retry(2)
    def _joblib_load(self, filepath: str, reserved: any = None) -> any:
        """Returns an object from joblib

        Args:
            filepath: Absolute file path

        Returns:
            The return value. object for success, None otherwise.

        """
        try:
            check_args(inspect.currentframe().f_code.co_name,
                       {'filepath': filepath}, False)

            return joblib.load(filepath)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    #@pgoperation.pg_retry(2)
    def _json_load(self, filepath: str) -> any:
        """Returns an object the filepath is read and json deserialized appropriately

        Args:
            filepath: Absolute file path

        Returns:
            The return value. True for success, False otherwise.

        """
        try:
            check_args(inspect.currentframe().f_code.co_name,
                       {'filepath': filepath}, False)

            with open(filepath, 'r') as file_read:
                return json.load(file_read)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    @pgoperation.pg_retry(2)
    def _df_load(self, filepath: str, reserved: any = None):
        """Returns a dataframe if the filepath is read and parsed appropriately

        Args:
            filepath: Absolute file path

        Returns:
            The return value. True for success, False otherwise.

        """

        try:
            check_args(inspect.currentframe().f_code.co_name,
                       {'filepath': filepath}, False)

            a = pd.read_csv(filepath, header=0)
            return a.to_dict('records')

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None





