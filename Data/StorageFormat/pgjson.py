import json
from Data.Utils import pgsecret
from Data.StorageFormat import pgstorageformatbase


class PGJson():
    def __init__(self, logger=None):
        """

        """
        print(__class__.__name__)
        self._logger = logger

    @property
    def inst(self):
        return self

    def load_config(self):
        pass

    def save(self, *, filename: str, data: dict) -> bool:
        if not data:
            if not self._logger:
                print("Nothing to save")
            return False
        try:
            with open(filename, 'w') as json_file:
                json.dump(data, json_file)
                return True
        except Exception as err:
            if not self._logger:
                print(err)
            else:
                self._logger(err)
            return False

    def save_object(self, *, data: dict) -> object:
        data = json.dumps(data, indent=4)
        print(type(data))
        return data

    def load(self, filename: str) -> dict:
        try:
            with open(filename, 'r') as json_file:
                return json.load(json_file)
        except Exception as err:
            if not self._logger:
                print(err)
            else:
                self._logger(err)
            return {}

    def load_object(self, *, string: str) -> dict:
        try:
            return json.loads(string)
        except Exception as err:
            if not self._logger:
                print(err)
            else:
                self._logger(err)
            return {}
