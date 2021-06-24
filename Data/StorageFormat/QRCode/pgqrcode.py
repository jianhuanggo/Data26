
import io
import inspect
import pandas as pd
from types import SimpleNamespace
import qrcode
from pyzbar.pyzbar import decode as decod
from PIL import Image
from pprint import pprint
from Meta import pgclassdefault
from Data.StorageFormat import pgstorageformatbase
from Data.Utils import pgdirectory
from Data.Utils import pgfile
from Meta import pggenericfunc
from typing import BinaryIO

__version__ = "1.6"



class PGQRCode(pgstorageformatbase.PGStorageFormatBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str ="pgqrcode", logging_enable: str = False):
        super(PGQRCode, self).__init__(project_name=project_name,
                                       object_short_name="PG_QR",
                                       config_file_pathname=__file__.split('.')[0] + ".ini",
                                       logging_enable=logging_enable,
                                       config_file_type="ini")
        self._client = None
        self._data = None
        self._storage_format = "qrcode"
        self._storage_instance = {}
        self._storage_parameter = {}
        self._external_lib = ["qrcode", "pyzbar", "zbar", "pillow"]

        #storage_parameter: {"header": true}

        self._writer = None

        #specific variable
        self._version = None
        self._box_size = None
        self._border = None
        self._color = None
        self._background_color = None

        self._client = qrcode.QRCode(version=1, box_size=10, border=5)




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

    def add_data(self, data: object) -> bool:
        try:
            if isinstance(data, (list, dict, tuple)):
                map(lambda x: self._client.add_data(x), data)
            else:
                self._client.add_data(data)
            self._client.add_data(data)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          err)

            return False


    def encode(self, data: object = None, storage_parameter=None, *args, **kwargs) -> {}:

        _data = data or self.data
        if _data is None:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "data parameter is empty")
            return None

        try:
            if self.add_data(data):
                self._client.make(fit=True)
            else:
                return {}
            color = self._color or "black"
            background_color = self._background_color or "white"
            _imag = self._client.make_image(color=color, background_color=background_color)
            return {self._storage_format: _imag,
                    "save": {"func": _imag.save, "parameters": "___path"}
                    }


        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          err)
            return {}

    def decode(self, data: object, *args, **kwargs) -> str:
        try:
            if isinstance(data, str):

                with Image.open(data) as img_file:
                    return list(map(lambda x: SimpleNamespace(**dict(x._asdict())).data, decod(img_file) ))
            else:
                return NotImplemented

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          err)

            return ""


if __name__ == '__main__':

    test = PGQRCode()
    qr_object = test.encode(["This is a test", "hello"])
    mypathfile = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Data/StorageFormat/QRCode/Test/test.png"
    print(qr_object)
    if "save" in qr_object:
        if "parameters" in qr_object["save"] and qr_object["save"]["parameters"] == "___path":
            qr_object["save"]["func"](mypathfile)
        else:
            qr_object["save"]["func"](qr_object["save"]["parameters"])
    print(test.decode(mypathfile))



