from DataValidator.Core import base
from os import listdir
from os.path import isfile, join


class Node:
    def __init__(self, start, end, data):
        self.start = int(start)
        self.end = int(end)
        self.data = data

    def print(self):
        print(f"start is {self.start}, end is {self.end}, data is {self.data}")


class StagingValidate(base.Validate):
    def __init__(self):
        self.store = []

    def fetch_evidence(self, path):
        file_list = [f for f in listdir(path) if isfile(join(path, f))]
        for item in file_list:
            start, end = item.split('_')[-3:-1]
            self.store.append(Node(start, end, item))

    def sort(self):
        self.store.sort(key=lambda x: x.start)

    def check_continuity(self):
        if not self.store:
            return True
        last = int(self.store.pop(0).end)
        for item in self.store:
            if item.start != last + 1:
                return False
            last = item.end
        return True


    def print(self):
        for item in self.store:
            item.print()


if __name__ == '__main__':
    dir_path = "/Users/jianhuang/PycharmProjects/Data/DataMover/Core/data/admins"
    sol = StagingValidate()
    sol.fetch_evidence(dir_path)
    sol.sort()
    sol.print()
    print(sol.check_continuity())