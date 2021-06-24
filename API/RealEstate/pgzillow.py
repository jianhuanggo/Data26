from types import SimpleNamespace
import Data.Utils.pgyaml as pgyaml
import requests
import json
from types import SimpleNamespace
from Data.Utils import pgdirectory


CONFIG_FILE = '/Config_yaml/PGzillow.yaml'
CONFIG_FILE_PATH = '/Users/jianhuang/test100/New/Config_yaml/'

"""
PGzillow uses rapidapi "default-application_5005300" to retrieve data from zillow

"""


def pg_blank_filler(string: str) -> str:
    return string.replace(" ", "%20")


class PGZillow:
    def __init__(self, region_name: str = None):
        try:
            config_yaml = SimpleNamespace(**pgyaml.yaml_load(
                yaml_filename=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml"))
            self._url = config_yaml.default_application_5005300_url
            self._headers = {
                'content-type': config_yaml.content_type,
                'x-rapidapi-key': config_yaml.x_rapidapi_key,
                'x-rapidapi-host': config_yaml.x_rapidapi_host}
        except Exception as err:
            raise Exception(f"Something wrong with : {err}")

    @property
    def url(self) -> str:
        return self._url

    @property
    def headers(self) -> dict:
        return self._headers

    def get_info(self, city: str, state_cd: str, page_num: int) -> dict:
        try:
            response = requests.request("POST",
                                        self._url,
                                        data=pg_blank_filler(f"city={city}&state_code={state_cd}&page={page_num}"),
                                        headers=self._headers)
            if response.status_code == 200:
                return json.loads(response.text)

        except Exception as err:
            raise Exception(err)

    def save_date(self, data_store=None):
        print("ok")


if __name__ == '__main__':
    test = PGZillow()
    #my_dict = test.get_info("san francisco", "ca", 1)
    my_dict = test.get_info("Johns Creek", "ga", 1)
    print(my_dict)
    #my_dict1 = SimpleNamespace(**my_dict)
    #print(my_dict1.properties)

