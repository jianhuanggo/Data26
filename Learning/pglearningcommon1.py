import sys
import json
import inspect
import joblib
import tarfile
import pandas as pd
from typing import Callable, Iterator, TypeVar, Optional, Type, Union
from Meta import pggenericfunc, pgclassdefault
from Meta.pggenericfunc import check_args
from Data.Utils import pgoperation, pgfile
from Data.Storage import pgstorage
from Data.Utils import pgfile


class PGLearningCommon(pgclassdefault.PGClassDefault):
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

        self._storage = pgstorage.pgstorage("localdisk")
        self._storage_format = None
        self._save_method = {'joblib': joblib.dump,
                             'json': self._json_dump,
                             'dataframe': self._df_dump,
                             'tarfile': self._tarfile_create,
                             's3': self._s3_file_upload,
                             }

        self._load_method = {'joblib': self._joblib_load, #joblib.load(filepath)
                             'json': self._json_load, #_json_load(filepath)
                             'dataframe': self._df_load,
                             'tarfile': self._tarfile_extract,
                             's3': self._s3_file_download,
                             }

    def get_func(self, ):
        return

    @pgoperation.pg_retry(2)
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

    def _tarfile_create(self, filepath: str, files: Union[list, tuple, set], purge_flg=True) -> bool:
        """Returns True if file(s) are successfully tar in the specified path

        Args:
            filepath: Absolute path for tarfile
            files: A list of files to be included in the tarfile
            purge_flg: When defaults to True, purge the files which are included in the tarfile

        Returns:
            The return value. True for success, False otherwise.

        """
        try:
            with tarfile.open(filepath, 'w:gz') as tar:
                for items in files:
                    tar.add(items, arcname=f"{pgfile.get_file_from_dirpath(items)}")
            if purge_flg:
                for items in files:
                    pgfile.remove_file(items)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    @pgoperation.pg_retry(2)
    def _s3_file_upload(self, storage, parameters: dict) -> bool:
        """Returns True if file(s) are successfully persisted in appropriate s3 location.

        Args:
            storage: A PGS3 Storage instance
            parameters: A set of parameters which needed to copy files to the appropriate S3 location
                        Required entries:
                           data: absolute filepath on localdisk
                           mode: "file"
                           object_key: filepath in S3 bucket
                           aws_access_key_id: aws access key
                           aws_secret_access_key: aws secret access key

        Returns:
            The return value. True for success, False otherwise.

        """
        try:

            check_args(inspect.currentframe().f_code.co_name,
                       {'storage': storage,
                        'data': parameters['data'] if "data" in parameters else None,
                        'mode': parameters['mode'] if "mode" in parameters else None,
                        'object_key': parameters['object_key'] if "object_key" in parameters else None,
                        'aws_access_key_id': parameters['aws_access_key_id'] if "aws_access_key_id" in parameters else None,
                        'aws_secret_access_key': parameters[
                            'aws_secret_access_key'] if "aws_secret_access_key" in parameters else None,
                        }, False)
            parameters['object_key'] = '/'.join(parameters['object_key'].replace("s3://", "").split('/')[1:]) if parameters['object_key'].startswith("s3://") else parameters['object_key']
            storage.save(data=parameters["data"],
                         storage_format=None,
                         storage_parameter=parameters)

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

    @pgoperation.pg_retry(2)
    def _tarfile_extract(self, filepath: str, reserved: any = None, purge_flg=True) -> bool:
        """Returns True if tar file is successfully extracted

        Args:
            filepath: Absolute path for tarfile
            purge_flg: When defaults to True, purge the tarfile after extraction

        Returns:
            The return value. True for success, False otherwise.

        """

        try:
            with tarfile.open(filepath, 'r:gz') as tar_read:
                tar_read.extractall('/'.join(filepath.split('/')[:-1]))

            if purge_flg:
                pgfile.remove_file(filepath)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    #@pgoperation.pg_retry(2)
    def _s3_file_download(self, storage, parameters: dict) -> bool:
        """Returns True if file(s) are successfully downloaded from appropriate s3 location.

        Args:
            storage: A PGS3 Storage instance
            parameters: A set of parameters which needed to copy files to the appropriate S3 location
                        Required entries:
                           data: absolute filepath on localdisk
                           mode: "file"
                           object_key: filepath in S3 bucket
                           aws_access_key_id: aws access key
                           aws_secret_access_key: aws secret access key

        Returns:
            The return value. True for success, False otherwise.

        """
        try:

            check_args(inspect.currentframe().f_code.co_name,
                       {'storage': storage,
                        'directory': parameters['directory'] if "directory" in parameters else None,
                        'mode': parameters['mode'] if "mode" in parameters else None,
                        'object_key': parameters['object_key'] if "object_key" in parameters else None,
                        'aws_access_key_id': parameters['aws_access_key_id'] if "aws_access_key_id" in parameters else None,
                        'aws_secret_access_key': parameters[
                            'aws_secret_access_key'] if "aws_secret_access_key" in parameters else None,
                        }, False)
            #parameters['object_key'] = '/'.join(parameters['object_key'].replace("s3://", "").split('/')[1:]) if parameters['object_key'].startswith("s3://") else parameters['object_key']
            return storage.load(location=parameters["object_key"],
                                storage_format=None,
                                storage_parameter=parameters)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _model_save(self, pgfiles: dict, _pg_action=None) -> bool:
        """Returns True if save is successful otherwise False.

        Args:
            pgfiles: A list of files need to be saved.
                     Example of pgfiles = {'<method>': [<content>, <location>]
                                           '<method>': [<content>, <location>]
                                           ...
                                           }
            _pg_action: A storage instance determine the location where the files are persisted.

        Returns:
            The return value. True for success, False otherwise.

        """
        try:
            #print(pgfiles)
            #print(_pg_action)

            check_args(inspect.currentframe().f_code.co_name,
                       {'pg_action': _pg_action, 'pgfiles': pgfiles}, False)

            for _method, _content in pgfiles.items():
                for _, _data in _content.items():
                    if isinstance(_data[0], str):
                        _save_method = _pg_action[_data[0].replace("##storage##", "")] if _data[0].startswith("##storage##") else _data[0]
                    else:
                        _save_method = _data[0]
                    if not self._save_method.get(_method)(_save_method, _data[1]):
                        sys.exit(99)

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _model_load(self, pgfiles: dict, _pg_action=None) -> Union[dict, None]:
        """Returns True if save is successful otherwise False.

        Args:
            pgfiles: A list of files need to be saved.
                     Example of pgfiles = {'<method>': [<content>, <location>]
                                           '<method>': [<content>, <location>]
                                           ...
                                           }
            _pg_action: A storage instance determine the location where the files are persisted.

        Returns:
            The return value. True for success, False otherwise.

        """
        _model_status = _pgconfig_status = True

        check_args(inspect.currentframe().f_code.co_name,
                   {'pgfiles': pgfiles, 'pg_action': _pg_action, }, False)

        try:
            _obj_list = {}

            for _method, _content in pgfiles.items():
                for _label, _data in _content.items():
                    if isinstance(_data[0], str):
                        _load_method = _pg_action[_data[0].replace("##storage##", "")] if _data[0].startswith("##storage##") else _data[0]
                    else:
                        _load_method = _data[0]
                    _extract_obj = self._load_method.get(_method)(_load_method, _data[1])
                    print(_extract_obj)

                    if not _extract_obj:
                        return None

                    if isinstance(_data[1], str) and _data[1] == "save":
                        _obj_list[_label] = _extract_obj

            return _obj_list

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None



