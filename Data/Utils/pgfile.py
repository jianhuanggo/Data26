import uuid
from os import listdir
from os.path import isfile, join
from pathlib import Path
import os
import shutil


def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


def create_temp_file(size, file_name, file_content):
    random_file_name = '_'.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name


def get_random_filename(file_name: str) -> str:
    return '_'.join([str(uuid.uuid4().hex[:8]), file_name])

def get_random_string():
    return str(uuid.uuid4().hex[:8])


def get_file_from_dirpath(dirpath: str) -> str:
    return dirpath.split('/')[-1]


def file_len(filename):
    with open(filename) as file:
        for index, line in enumerate(file, 1):
            pass
    return index


def get_notmatch_file(path):
    return [f for f in listdir(path) if isfile(join(path, f)) and f.split('.')[-1] == 'notmatch']


def get_all_file_in_dir(dirpath):
    return [f for f in listdir(dirpath) if isfile(join(dirpath, f))]


def remove_file(filepath):
    if os.path.exists(filepath):
        os.remove(filepath)


def isfileexist(filepath):
    file = Path(filepath)
    if file.is_file():
        return True
    else:
        return False


def file_move(old_filepath, new_filepath):
    try:
        shutil.move(old_filepath, new_filepath)
    except Exception as err:
        raise err


def pick_one_config_file(config_dir: str, target_dir: str):

    file_list = get_all_file_in_dir(config_dir)
    for item in file_list:
        file_move(item, target_dir)
        if isfileexist(item):
            return item


def filename_remove_space(filename: str):
    """
    check whitespace in filename and replace whitespace between words with dash
    """
    return '-'.join(filename.split())


if __name__ == '__main__':

    for item in get_all_file_in_dir("/Users/jianhuang"):
        if "datahub-framework" in item:
            os.remove("/Users/jianhuang/" + item)
