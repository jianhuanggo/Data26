import json
from collections.abc import Iterable
from typing import Union
from Meta import pgmetaclass

#self._data_inputs[parameters['name']] = parameters.get("read_func")(pgdataset)


class PGScrapFormatter(metaclass=pgmetaclass.PGMetaClass):

    @staticmethod
    def redfin_address_format(pg_address: Union[str, Iterable]) -> Iterable:
        if isinstance(pg_address, str):
            with open(pg_address) as read_file:
                _pg_file_content = json.load(read_file)
            #for item in list(_pg_file_content):
                #yield item["_address"]
            return [item["_address"] for item in _pg_file_content]
        else:
            return pg_address


if __name__ == '__main__':
    test = PGScrapFormatter()
    for addr in test.redfin_address_format("/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/WebScraping/PGScrapy/output.json"):
        print(addr)

