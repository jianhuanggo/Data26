import inspect
from types import SimpleNamespace
from redfin import Redfin
from Data.Utils import pgyaml
from Data.Utils import pgdirectory
from Meta import pggenericfunc
from Data.Utils import pgoperation
import json
from pprint import pprint


def get_item(mydict: dict, mykey: str) -> str:
    if mykey in mydict:
        return mydict[mykey]
    else:
        return "NA"


class PGRedfin:
    def __init__(self, prop_address: str, logger=None):
        try:
            self._logger = logger
            yaml_content = pgyaml.yaml_load(yaml_filename=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml")
            if yaml_content:
                config_yaml = SimpleNamespace(**yaml_content)
            self._client = Redfin()
            self._detail_info = None
            self._property_id = None
            self._listing_id = None


        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          err)
        try:
            _data = self._client.initial_info(self._client.search(prop_address)['payload']['exactMatch']['url'])['payload']
            if _data:
                self._property_id = _data['propertyId']
                self._listing_id = _data['listingId']
                self.get_detail()
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger,
                                          inspect.currentframe().f_code.co_name,
                                          err, "Please make sure address is correct")

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

    def get_detail_info(self):
        return self._client.below_the_fold(self._property_id)

    @pgoperation.pg_retry(3)
    def get_detail(self):
        self._detail_info = self.get_detail_info()
        return self._detail_info

    def get_property_txn_history(self):
        data = []
        if not self._detail_info:
            self._detail_info = self.get_detail_info()
        else:
            try:
                for item in self._detail_info['payload']['propertyHistoryInfo']['events']:
                    col1 = get_item(item, 'price')
                    col2 = get_item(item, 'eventDescription')
                    col3 = get_item(item, 'mlsDescription')
                    col4 = get_item(item, 'eventDate')
                    col5 = get_item(item, 'eventDateString')
                    data_item = "{" + f"\"price\": \"{col1}\", \"eventDescription\": \"{col2}\", \"mlsDescription\": \"{col3}\", \"eventDate\": \"{col4}\", \"eventDateString\": \"{col5}\"" + "}"
                    print(data_item)
                    data.append(json.loads(data_item))
                return data
            except Exception as err:
                if not self._logger:
                    raise Exception(err)
                else:
                    self._logger(err)

    def get_school_info(self):
        if not self._detail_info:
            self._detail_info = self.get_detail_info()
        else:
            try:
                for item in self._detail_info['payload']['schoolsAndDistrictsInfo']['servingThisHomeSchools']:
                    print(list(item.keys()))
                    break
            except Exception as err:
                if not self._logger:
                    raise Exception(err)
                else:
                    self._logger(err)


if __name__ == '__main__':
    #client = PGRedfin(prop_address="2429 Parcview Run Cv, Duluth Georgia")
    #client = PGRedfin(prop_address="2581 Doral Dr, Duluth, GA ")
    client = PGRedfin(prop_address="2581 Doral Dr, Duluth, GA ")

    #2581 Doral Dr, Duluth, GA 30096

    #429 - Parcview - Run - Cv - NW - 30096
    #address = '2429 Parcview Run Cv, Duluth Georgia'

    #response = client.search(address)

    #url = response['payload']['exactMatch']['url']
    #initial_info = client.initial_info(url)
    #print(initial_info)
    #property_id = initial_info['payload']['propertyId']
    #listing_id = initial_info['payload']['listingId']
    print(client.property_id)
    print(client.listing_id)

    #mls_data = client.below_the_fold(property_id)
    #print(mls_data['payload']['propertyHistoryInfo']['events'])
    #for item in mls_data['payload']['propertyHistoryInfo']['events']:
    #    col1 = get_item(item, 'price')
    #    col2 = get_item(item, 'eventDescription')
    #    col3 = get_item(item, 'mlsDescription')
    #    col4 = get_item(item, 'eventDate')
    #    col5 = get_item(item, 'eventDateString')
    #    print(f"{{'price': '{col1}', 'eventDescription': '{col2}', 'mlsDescription': '{col3}', "
    #          f"'eventDate': '{col4}', 'eventDateString': '{col5}'")

    client.get_property_txn_history()

    client.get_school_info()
    #print(list(mls_data['payload']['propertyHistoryInfo']['priceEstimates'].keys()))
    #print(client.get_detail_info()['payload']['propertyHistoryInfo']['priceEstimates'])
    print(client.get_detail_info()['payload']['propertyHistoryInfo'])

    #pprint(client.get_detail_info()['payload']['propertyHistoryInfo'])

    client.get_property_txn_history()

    #print(client.owner_estimate(client.property_id))
    #print(list(mls_data['payload']['schoolsAndDistrictsInfo'].keys()))
    #for item in mls_data['payload']['schoolsAndDistrictsInfo']['servingThisHomeSchools']:
    #    print(list(item.keys()))
    #    break

    #print(mls_data)
