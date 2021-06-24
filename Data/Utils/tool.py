from os import listdir
from os.path import isfile, join

def print_datafile(path):
    #[f for f in listdir(path) if isfile(join(path, f)) and f.split('.')[-1] == 'notmatch']
    file_list = [f for f in listdir(path) if isfile(join(path, f))]
    for item in file_list:
       print(item.split('_')[-3])


if __name__ == '__main__':
    dir_path = "/Users/jianhuang/PycharmProjects/Data/DataMover/Core/data/admins"
    print_datafile(dir_path)