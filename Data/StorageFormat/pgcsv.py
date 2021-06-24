import csv
import pandas as pd
from Data.Utils import pgsecret
from Data.StorageFormat import pgstorageformatbase
from Data.Utils import pgfile
from types import SimpleNamespace
#import logging


class PGExcel():
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

    def save_pd(self, *, filename: str, data: dict) -> bool:
        """
        Not working for complex dataset
        """
        try:
            pd.DataFrame.from_dict(data, orient='index').to_csv(filename, header=False)
        except Exception as err:
            if not self._logger:
                print(err)
            else:
                self._logger(err)
            return False
        return True

    def save(self, *, filename: str, data: dict, mode: str = "dict") -> bool:
        if not data:
            if not self._logger:
                print("Nothing to save")
            return False
        try:
            if pgfile.isfileexist(filename):
                write_mode = 'a'
            else:
                write_mode = 'w'
            with open(filename, write_mode) as csv_file:
                writer = csv.writer(csv_file)
                if mode == "dict":
                    for key, value in data.items():
                        writer.writerow([key, value])
                elif mode == "list":
                    wr = csv.writer(csv_file, dialect='excel')
                    wr.writerow(data)

        except Exception as err:
            if not self._logger:
                print(err)
            else:
                self._logger(err)
            return False
        return True

    def save_row(self, *, filename: str, data: dict) -> bool:
        """
        Get input as a list of dictionaries
        Save each dictionary as a single record with the key of the dictionary act as the column name
        """

        try:
            if data:
                _column = list(data[0].keys())
            else:
                if not self._logger:
                    print("Data is Empty")
                    return True
                else:
                    self._logger("Data is Empty")
                    return True

            if pgfile.isfileexist(filename):
                write_mode = 'a'
            else:
                write_mode = 'w'
            with open(filename, write_mode) as csv_file:
                write = csv.DictWriter(csv_file, fieldnames=_column)
                if write_mode == 'w':
                    write.writeheader()
                for item in data:
                    write.writerow(item)
        except Exception as err:
            if not self._logger:
                print(err)
            else:
                self._logger(err)
            return False
        return True

    def load_pd(self, *, filename: str) -> dict:
        """
        Not working for complex dataset
        """
        try:
            return pd.read_csv(filename)
        except Exception as err:
            if not self._logger:
                print(err)
            else:
                self._logger(err)
            return {}

    def load(self, *, filename: str, mode: str = "dict") -> object:
        if mode not in ('dict', 'list'):
            if not self._logger:
                print(f"mode {mode} is not supported")
            else:
                self._logger(f"mode {mode} is not supported")
            return None

        try:
            with open(filename, 'r') as csv_file:
                if mode == "dict":
                    return dict(csv.reader(csv_file))
                elif mode == "list":
                    return list(csv.reader(csv_file))[0]
        except Exception as err:
            if not self._logger:
                print(err)
            else:
                self._logger(err)
            return None


if __name__ == '__main__':
    #TEST_FILE1 = '/Users/jianhuang/test100/New/temp/pg_excel_test1'
    TEST_FILE2 = '/Users/jianhuang/test100/New/temp/pg_excel_test2'
    #test_dict2 = {'name': {'test': 'ok'}, 'test': {'ok': pgsecret.generate_secret(16)}}
    test_dict2 = {'name': 'ok', 'test': pgsecret.generate_secret(16)}
    my_excel = PGExcel().inst
    my_excel.save(filename=TEST_FILE2, data=test_dict2)
    print(my_excel.load(filename=TEST_FILE2))



