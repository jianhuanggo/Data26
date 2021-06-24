from Utils import pgsecret
from types import SimpleNamespace
#import logging
import pandas as pd
import csv


class PGexcel:
    def __init__(self, data=None):
        """

        """
        print(__class__.__name__)
        if data:
            self._data = data
        else:
            self._data = None

    @property
    def data(self):
        return self._data

    def load_config(self):
        return

    def write_file_bp(self, filename):
        """
        Not working for complex dataset
        """
        try:
            pd.DataFrame.from_dict(self._data, orient='index').to_csv(filename, header=False)
        except Exception as e:
            #logging.error(e)
            print(e)
            return False
        return True

    def write_file(self, *, filename, data=None, mode="dict"):
        try:
            if data is None:
                data = self._data
            with open(filename, 'w') as csv_file:
                writer = csv.writer(csv_file)
                if mode == "dict":
                    for key, value in data.items():
                        writer.writerow([key, value])
                elif mode == "list":
                    wr = csv.writer(csv_file, dialect='excel')
                    wr.writerow(data)

        except Exception as err:
            #logging.error(e)
            print(err)
            return False
        return True

    def load_file_pd(self, filename):
        """
        Not working for complex dataset
        """
        try:
            self._data = pd.read_csv(filename)
        except Exception as err:
            print(err)
            return False
        return True

    def load_file(self, *, filename, mode="dict"):
        print("okokok1111")
        print(mode)
        try:
            with open(filename, 'r') as csv_file:
                if mode == dict:
                    print("okokok")
                    self._data = dict(csv.reader(csv_file))
                    print(self._data)
                elif mode == "list":
                    self._data = list(csv.reader(csv_file))[0]
            print("okokok22222")
        except Exception as err:
            print(err)
            return False
        return True


if __name__ == '__main__':
    #TEST_FILE1 = '/Users/jianhuang/test100/New/temp/pg_excel_test1'
    TEST_FILE2 = '/Users/jianhuang/test100/New/temp/pg_excel_test2'
    #test_dict2 = {'name': {'test': 'ok'}, 'test': {'ok': pgsecret.generate_secret(16)}}
    test_dict2 = {'name': 'ok', 'test': pgsecret.generate_secret(16)}
    my_excel = PGexcel(test_dict2)
    print(my_excel.data)
    my_excel.write_file(TEST_FILE2)

    my_excel._data = None
    my_excel.load_file(TEST_FILE2)
    print(my_excel.data)


