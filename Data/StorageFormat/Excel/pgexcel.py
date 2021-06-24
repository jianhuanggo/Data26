import io
import inspect
import pandas as pd
from pprint import pprint
from Meta import pgclassdefault
from Data.StorageFormat import pgstorageformatbase
from Data.Utils import pgdirectory
from Data.Utils import pgfile
from Meta import pggenericfunc
from typing import BinaryIO

__version__ = "1.5"

"""
requires openpyxl
conda install -c anaconda openpyxl

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

>>> import numpy as np
>>> import cv2
>>> xbash = np.fromfile('/bin/bash', dtype='uint8')
>>> xbash.shape
(1086744,)
>>> cv2.imwrite('bash1.png', xbash[:10000].reshape(100,100))

"""


class PGExcel(pgstorageformatbase.PGStorageFormatBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str ="pgexcel", logging_enable: str = False):
        super(PGExcel, self).__init__(project_name=project_name,
                                      object_short_name="PG_EXC",
                                      config_file_pathname=__file__.split('.')[0] + ".ini",
                                      logging_enable=logging_enable,
                                      config_file_type="ini")
        self._name = None
        self._data = None
        self._storage_format = "excel"
        self._storage_instance = {}
        self._storage_parameter = {}
        """
        storage_parameter: {"header": true}
        """
        self._writer = None

        """

        try:
            if str(self._config.parameters["config_file"]['default']['unpicklable']):
                self._unpicklable = self._config.parameters["config_file"]['default']['unpicklable']
        except Exception as err:
            print("unpicklable setting is not found in the default section of ini file, set it to True")
            self._unpicklable = True
        """

    @property
    def data(self):
        return self._data

    @property
    def storage_format(self):
        return self._storage_format

    @property
    def storage_parameter(self):
        return self._storage_parameter

    def inst(self):
        return self

    def encode(self, data: object = None, storage_parameter=None, *args, **kwargs):
        ### 1) Convert string to dataframe
        ### 2) Write dataframe to BytesIO

        _data = data or self.data
        if _data is None:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "data parameter is empty")
            return None

        """
        else:
            if not isinstance(_data, pd.DataFrame):
                _data = self.decode(_data)
                print(f"data is: {_data}")


        if self._storage_instance storage_type == "localdisk":
            self._data = pgdirectory.add_splash_2_dir(self._storage_instance.storage_parameter['directory']) + \
                       self._storage_instance.storage_parameter['filename'].replace("*", pgfile.get_random_string())
            #print(pgbuffer)

        else:
        """

        if isinstance(_data, str):
            _storage_parameter = {**self._storage_parameter, **storage_parameter}

            print(f"storage_parameter: {_storage_parameter}")
            _storage_parameter['header'] = True
            if _storage_parameter and _storage_parameter['header']:
                print("okok111")
                _pd = pd.read_csv(io.StringIO(_data), sep=",", header=None)
                print(f"_pd: {_pd}")
                #exit(0)
            else:
                _pd = pd.read_csv(io.StringIO(_data), sep=",")
        elif isinstance(_data, pd.DataFrame):
            _pd = _data
        else:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          "data type for data must be string or dataframe")
            return None

        _buf = io.BytesIO()
        try:
            #print(f"here is the data: {_data}")
            with pd.ExcelWriter(_buf) as writer:
                _pd.to_excel(writer)
            _buf.seek(0)
            print(_buf.getvalue())
            #print(f"here is a string: {_buf.read().decode()}")
            #exit(0)
            return _buf
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          err)
            return None

    def to_excel(self, df: pd.DataFrame, sheet_name: str) -> bool:
        if not self._writer:
            self.encode()
        try:
            df.to_excel(self._writer, sheet_name)
            return True
        except Exception as err:
            if self._logger:
                self._logger(err)
            else:
                print(err)
            return False

    def decode(self, data: object, *args, **kwargs):
        if isinstance(data, str):
            print(f"in decode: {data}")
            ##return pd.read_csv(io.StringIO(data), sep=",")

            #pprint(self._storage_parameter)
            if self._storage_parameter and "header" in  self._storage_parameter and self._storage_parameter['header']:
                return pd.read_csv(io.StringIO(data), sep=",")
            else:
                return pd.read_csv(io.StringIO(data), sep=",", header=None)
        elif isinstance(data, io.BytesIO):
            return pd.read_csv(data)
        elif isinstance(data, bytes):
            return pd.read_csv(io.BytesIO(data))
        else:
            pggenericfunc.notimplemented()

    def load_file(self, filename: str, *args, **kwargs) -> bool:
        try:
            #print(f"df is {self._df}")
            if self._data is not None:
                self._data = pd.concat([self._data, pd.read_excel(filename, engine='openpyxl', header=None)])
            else:
                self._data = pd.read_excel(filename, engine='openpyxl', header=None)
            return True
        except Exception as err:
            if self._logger:
                self._logger(err)
            else:
                print(err)
            return False
        #df.head()

    def load_direct(self, data: object, *args, **kwargs) -> bool:
        try:
            if data is not None:
                self._data = pd.concat([self._data, pd.read_excel(filename, engine='openpyxl', header=None)])
            else:
                self._data = pd.read_excel(filename, engine='openpyxl', header=None)
            return True
        except Exception as err:
            if self._logger:
                self._logger(err)
            else:
                print(err)
            return False

    def save_localfile(self):
        pass
        # Create a Pandas Excel writer using XlsxWriter as the engine.
        #writer = pd.ExcelWriter('pandas_simple.xlsx', engine='xlsxwriter')

        # Convert the dataframe to an XlsxWriter Excel object.
        #df.to_excel(writer, sheet_name='Sheet1')

        # Close the Pandas Excel writer and output the Excel file.
        #writer.save()


    def chart(self):
        pass




