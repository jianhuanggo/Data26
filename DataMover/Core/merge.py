from os import listdir
from os.path import isfile, join
import pandas as pd
import glob
from Data.Utils import pgfile, pgdirectory


class Merge:

    def __init__(self):
        pass

    def merge_file(self, input_data_path, max_file_len, output_data_path):

        try:
            if not pgdirectory.isdirectoryexist(output_data_path):
                pgdirectory.createdirectory(output_data_path)
            else:
                new_dir_name = pgdirectory.get_random_directory(output_data_path)
                pgdirectory.rename_directory(output_data_path, new_dir_name)
                pgdirectory.createdirectory(output_data_path)
        except Exception as e:
            raise ("Can not create the directory for storing data")

        results = pd.DataFrame([])
        file_list = [input_data_path + '/' + f for f in listdir(input_data_path) if isfile(join(input_data_path, f)) and f.split('.')[-1] == 'match']
        print(file_list)

        record_count = 0

        #for counter, file in enumerate(glob.glob(input_data_path + "*match")):
        for counter, file in enumerate(file_list):
            namedfile = pd.read_csv(file, skiprows=0)
            print(namedfile.shape[1])
            print(namedfile.shape[0])
            print(file)
            results = results.append(namedfile, sort=True, verify_integrity=False)

        results.to_csv(output_data_path + "/mergedfile.txt")

"""
        print(file_list)



        os.chdir("C:/Folder")
        results = pd.DataFrame([])

        for counter, file in enumerate(glob.glob("datayear*")):
            namedf = pd.read_csv(file, skiprows=0, usecols=[1, 2, 3])
            results = results.append(namedf)

        results.to_csv('C:/combinedfile.csv')
"""

if __name__ == '__main__':
    test = Merge()
    test.merge_file("/Users/jianhuang/PycharmProjects/Data/DataMover/Core/data/test_table", 50000, "/Users/jianhuang/PycharmProjects/Data/DataMover/Core/data/test_table/merge_data")