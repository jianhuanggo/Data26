#!/usr/bin/python
import sys
import os
import uuid
import errno
import shutil
import inspect
from os import walk
from pathlib import Path
from tempfile import mkdtemp
from contextlib import contextmanager
from Data.Utils import pgfile, pgoperation
from typing import Generator, Union, Tuple
from Meta import pggenericfunc
from Processing import pgprocessing


def filedirectory(abspathfilename):
    return '/'.join(abspathfilename.split('/')[:-1])


def add_splash_2_dir(directoryname):
    return directoryname + '/' if directoryname[:-1] != '/' else directoryname


def remove_end_splash_from_dir(directoryname):
    return directoryname[:-1] if directoryname[-1] == '/' else directoryname


def homedirectory():
    return str(Path.home())


def workingdirectory():
    return os.getcwd()


def currentdirectory():
    return os.path.dirname(os.path.realpath(__file__))


def isdirectoryexist(path: str, ignore_flg: bool = True) -> bool:
    return pgoperation.pg_ignore_fail(os.path.isdir(path), "isdirectoryexist", ignore_flg, f"{path} does not exist")


def createdirectory(dirpath: str, logger=None) -> bool:
    """
    Creates the directory specified by path, creating intermediate directories
    as necessary. If directory already exists, this is a no-op.
    :param dirpath: The directory to create

    :param logger: The directory to create
    :type : str
    """
    try:
        o_umask = os.umask(0)
        os.makedirs(dirpath)
    except FileExistsError:
        return True
    except OSError:
        if not os.path.isdir(dirpath):
            pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, f"Creation of the directory {dirpath} failed")
            return False
    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)
        return False
    else:
        print(f"Successfully created the directory {dirpath}")
        return True
    finally:
        os.umask(o_umask)

    return True


def get_random_directory(directory):
    dir_name = directory.split('/')[-1]
    dir_path = '/'.join(directory.split('/')[:-1])
    random_directory = ''.join([str(uuid.uuid4().hex[:6]), dir_name])
    return dir_path + '/' + random_directory


def rename_directory(src_dir: str, dest_dir: str) -> bool:
    try:
        shutil.move(src_dir, dest_dir)
        return True
    except Exception as err:
        print(err)
        return False


def remove_all_file_in_dir(dirpath: str) -> bool:
    try:
        for filename in pgfile.get_all_file_in_dir(dirpath):
            pgfile.remove_file(add_splash_2_dir(dirpath) + filename)
        return True
    except Exception as err:
        print(err)
        return False


@contextmanager
def TemporaryDirectory(suffix='', prefix=None, dir=None):
    name = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    try:
        yield name
    finally:
        try:
            shutil.rmtree(name)
        except OSError as e:
            # ENOENT - no such file or directory
            if e.errno != errno.ENOENT:
                raise e


def files_in_dir(path: str, logger=None) :
    try:
        _, _, filenames = next(walk(path))
        return filenames

    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)


def scantree(path: str):
    """Recursively yield DirEntry objects for given directory."""
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield from scantree(entry.path)
        else:
            yield entry


def get_filename_from_dirpath(dirpath: str, logger=None) -> str:
    try:
        return '/'.join(dirpath.split('/')[:-1])

    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)
        return ""


def get_dir_filename_from_dirpath(dirpath: str, logger=None) -> Tuple[str, str]:
    try:
        temp = dirpath.split('/')
        return '/'.join(dirpath.split('/')[:-1]), temp[-1]

    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)
        return ""


def file_or_file_in_dir(filename_or_path: str, logger=None) :
    """
    return either generator of a file or files in the directory, otherwise return an empty list
    """
    if not filename_or_path:
        return None
    try:
        if pgfile.isfile(filename_or_path):
            yield filename_or_path
        elif isdirectoryexist(filename_or_path):
            print("ok")
            yield from files_in_dir(filename_or_path)
    except Exception as err:
        pggenericfunc.pg_error_logger(logger, inspect.currentframe().f_code.co_name, err)


if __name__ == '__main__':
    #files_in_dir('/Users/jianhuang/opt/anaconda3/envs/Data20/lib/python3.7/site-packages/chatterbot_corpus/data/english/')

    path = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Real_estate_research/data/Alpharetta"
    #string = "/users/jianhuang/admin"
    #print(get_random_directory(string))

    #for filename in files_in_dir(path):
    #    print(filename)
    #exit(0)

    #print(sys.argv)
    #exit(0)
    #for entry in scantree(sys.argv[1] if len(sys.argv) > 1 else '.'):
    for entry in scantree(path):

        print(entry.path)

    exit(0)
    for item in file_or_file_in_dir(path):
        print(item)

    exit(0)
