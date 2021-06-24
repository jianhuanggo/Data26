import os
import shutil
import inspect
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from Data.Storage import pgstoragebase
from Data.Utils import pgdirectory
from Data.Utils import pgfile
from Meta import pggenericfunc
from Data.Storage import pgstoragecommon

__version__ = "1.7"

"""
import io
buffer = io.BytesIO()
with pd.ExcelWriter(buffer) as writer:
    df.to_excel(writer)

from io import BytesIO
bytesIO = BytesIO()
bytesIO.write('whee')
bytesIO.seek(0)
s3_file.set_contents_from_file(bytesIO)

b = io.BytesIO(b"Hello World") ## Some random BytesIO Object
print(type(b))                 ## For sanity's sake
with open("test.xlsx") as f: ## Excel File
    print(type(f))           ## Open file is TextIOWrapper
    bw=io.TextIOWrapper(b)   ## Conversion to TextIOWrapper
    print(type(bw)) 

myBytesIOObj.seek(0)
with open('myfile.ext', 'wb') as f:
    shutil.copyfileobj(myBytesIOObj, f, length=131072)
"""


class PGLocaldisk(pgstoragebase.PGStorageBase, pgstoragecommon.PGStorageCommon):
    def __init__(self, project_name: str = "localdisk", logging_enable: str = False):
        super(PGLocaldisk, self).__init__(project_name=project_name,
                                          object_short_name="PG_LD",
                                          config_file_pathname=__file__.split('.')[0] + ".ini",
                                          logging_enable=logging_enable,
                                          config_file_type="ini")

        self._storage_type = "localdisk"
        self._storage_prefix = ""

        """
        _storage_location 

        {'directory': '/x/home/data/'
         'filename':  'test*.txt'
         }
        * - wildcard, it will be replaced by random string, 
            good for multipleprocessing/multithreading where each process writes its own file
        """

    @property
    def storage_type(self):
        return self._storage_type

    @property
    def storage_parameter(self):
        return self._storage_parameter

    @property
    def data(self):
        return self._data

    def inst(self):
        return self

    def get_storage_name_and_path(self, dirpath: str) -> Tuple[Union[str, None], Union[str, None]]:
        return None, None

    def save(self, data: object, storage_format: str = None, storage_parameter: dict = None, *args, **kwargs) -> bool:
        """
        self._storage_location: dict
        key:

        directory (str): required parameter to indicate the absolute directory path
        filename (str) : required parameter, can contain * in which it will be replaced by 8 random string to unfixed filename
        write_mode: optional parameter, same as file open library, 'a' for append, 'w' for write, 'r' for read, 'rb', 'wb'
                    if this paramter is not specified, then save function will append to an already existing file
        For example:
        {'directory':  '/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/Localdisk/Test_Localdisk/temp',
         'filename':   'test_*.txt',
         'write_mode': 'a'}

        direct write (dict): contains a dictonary of formats which direct write without intermediate to bytesio
                             is possible in which case we will just invoke the storage format method directly
        """
        if not self._storage_parameter:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "Storage location is not set")
            return False

        if self._storage_format_instance:
            if self._storage_format_instance.storage_format in self._direct_write_storage_format:
                try:
                    if self._storage_format_instance.encode(data):
                        return True
                    else:
                        return False
                except Exception as err:
                    pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
                    return False

        try:
            _dirpathfilename = pgdirectory.add_splash_2_dir(self._storage_parameter['directory']) +\
                               self._storage_parameter['filename'].replace("*", pgfile.get_random_string())

            if "access_mode" in self._storage_parameter:
                _write_mode = self._storage_parameter['access_mode']
            elif self._storage_format_instance and self._storage_format_instance.storage_format == "bytesio":
                data.seek(0)
                if pgfile.isfileexist(_dirpathfilename):
                    _write_mode = 'ab'
                else:
                    _write_mode = 'wb'
            else:
                if pgfile.isfileexist(_dirpathfilename):
                    _write_mode = 'a'
                else:
                    _write_mode = 'w'

            #print(_write_mode)
            with open(_dirpathfilename, _write_mode) as file:
                if self._storage_format_instance and self._storage_format_instance.storage_format == "bytesio":
                    shutil.copyfileobj(data, file, length=131072)
                else:
                    file.write(data)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def load(self, location: str, storage_format: str = None, storage_parameter: dict = None, *args, **kwargs) -> bool:

        #print(self._storage_format_instance)

        if not location:
            if self._logger:
                self._logger.Critical("Storage location is not set")
            else:
                print("Storage location is not set")
            return False

        """
        check whether the specified location is a directory, then load all the files in the directory

        """

        if os.path.isdir(location):
            _isdir = True
        elif os.path.isfile(location):
            _isdir = False
        else:
            if self._logger:
                self._logger.Critical(f"{location} is not valid")
            else:
                print(f"{location} is not valid")
            return False

        #print(_isdir)
        # if there is storage format attached
        if self._storage_format_instance:
            # is direct load enabled
            if self._storage_format_instance.storage_format in self._direct_write_storage_format:

                try:
                    # is directory
                    if _isdir:
                        for filename in pgfile.get_all_file_in_dir(location + "/"):
                            #print(filename)
                            if not self._storage_format_instance.load_file(pgdirectory.add_splash_2_dir(location) + filename):
                                return False
                    else:
                        if not self._storage_format_instance.load_file(location):
                            return False
                    return True
                except Exception as err:
                    pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
                    return False
        else:
            try:
                if _isdir:
                    for filename in pgfile.get_all_file_in_dir(location):
                        with open(pgdirectory.add_splash_2_dir(location) + filename, 'r') as file:
                            self._data.append(file.read())
                elif os.path.isfile(location):

                    with open(location, 'r') as file:
                        self._data.append(file.read())

                else:
                    return False
                return True
            except Exception as err:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
                return False


if __name__ == '__main__':
    test = PGLocaldisk()
    test.set_param(storage_location={'directory': '/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/Storage/Localdisk/Test_Localdisk',
                                     'filename':  'test_*.txt'})
    input_text = "let us test"
    test.save(input_text)
