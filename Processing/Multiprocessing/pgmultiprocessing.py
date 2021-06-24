import itertools
from functools import partial
import types
import multiprocessing.pool
from multiprocessing import Pool
from multiprocessing import cpu_count
from concurrent import futures
from Meta import pgclassdefault2
from Meta import pggenericfunc
from Processing import pgprocessingbase
from Data.Utils import pgdirectory

__version__ = "1.5"


class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess


class NestablePool(multiprocessing.pool.Pool):
    """
    We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
    because the latter is only a wrapper function, not a proper class.
    """
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(NestablePool, self).__init__(*args, **kwargs)


class PGMultiProcessing(pgprocessingbase.PGProcessingBase, pgclassdefault2.PGClassDefault):

    def __init__(self, project_name: str = "multiprocessing", logging_enable: bool = False):
        """
        Args:
            project_name (int): The first parameter.
            logging_enable (bool): The second parameter.

        """
        super(PGMultiProcessing, self).__init__(project_name=project_name,
                                                object_short_name="PG_MP",
                                                config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml",
                                                logging_enable=logging_enable)

        self._pg_private = {}
        self._is_daemon = True
        self._chunksize = "AUTO"
        self._test = 5
        self._num_workers = cpu_count()
        self._setting_doc = """   
            Available Setting:
                num_workers (int):  the number of workers allocated
                is_daemon (bool): True -> daemon Pool, False -> non daemon Pool
                chunksize (int): if info is available: len(tasks) // num_workers, 
                                 if expensive to size of data, set it to 1
        """

    @property
    def num_workers(self):
        return self._num_workers

    def set_param(self, *args, **kwargs):
        """
        Args:
            args (list): The first parameter.
            kwargs (dict): The second parameter.

        Returns:
            None

        """
        if args:
            raise Exception(f"ambiguous argument(s) {args}")
        #print(vars(self))
        #print(self.__init__.__code__.co_varnames)
        #print(f"here is MP: {kwargs}")

        for pkey, pval in kwargs.items():
            if f"_{pkey}" in vars(self) and f"_{pkey}" != "_pg_private":
                setattr(self, f"_{pkey}", pval)
            else:
                print(self._setting_doc)
                raise Exception(f"parameter {pkey} does not exist")

    def run(self, func: types.FunctionType, *args, **kwargs) -> None:
        """
        Args:
            func (function): The first parameter.
            args (list): The second parameter.
            kwargs (dict): The third parameter.

        Returns:
            None

        args_list Format to pass into Executor: [(arg1, arg1, arg1...),
                                                 (arg2, arg2, arg2...),
                                                 (arg3, arg3, arg3...),
                                                 (arg4, arg4, arg4...)]
        """

        if self._num_workers == "AUTO":
            _num_workers = cpu_count()
        else:
            _num_workers = self._num_workers

        if self._chunksize == "AUTO":
            _chunksize = len(*args) // _num_workers
        else:
            _chunksize = self._chunksize

        print(f"current number of workers: {_num_workers}")
        print(f"chunksize : {_chunksize}")

        [unpacking_arg] = itertools.chain(args)
        args_list = list(map(tuple, itertools.zip_longest(*unpacking_arg, fillvalue=None)))

        with futures.ProcessPoolExecutor(_num_workers) as executor:
            if func.__code__.co_kwonlyargcount > 0:
                pggenericfunc.notimplemented()
            else:
                result = executor.map(func, *args_list, chunksize=_chunksize)
                executor.shutdown(wait=True)
            return result

    def run_map(self, func: types.FunctionType, *args, **kwargs) -> None:
        """
        Args:
            func (function): The first parameter.
            args (list): The second parameter.
            kwargs (dict): The third parameter.

        Returns:
            None

        args Format to pass into Executor: ([(arg1, arg2, arg3...),
                                             (arg1, arg2, arg3...),
                                             (arg1, arg2, arg3...),
                                             (arg1, arg2, arg3...)])

        Link: https://towardsdatascience.com/parallelism-with-python-part-1-196f0458ca14

        """
        print(args)
        print(kwargs)

        is_pool_daemon = {"True": NestablePool, "False": Pool}

        if self._num_workers == "AUTO":
            _num_workers = cpu_count()
        else:
            _num_workers = self._num_workers

        if self._chunksize == "AUTO":
            _chunksize = len(args) // _num_workers
        else:
            _chunksize = self._chunksize

        print(f"is daemon pool: {self._is_daemon}")
        print(f"current number of workers: {_num_workers}")
        print(f"chunksize : {_chunksize}")

        with is_pool_daemon.get(str(self._is_daemon), "True")(_num_workers) as executor:
            #print(func, *args, kwargs)
            if func.__code__.co_kwonlyargcount > 0:
                pggenericfunc.notimplemented()
                #result = pool.starmap(func, iterable=args_iter, chunksize=_chunksize)
            else:
                result = executor.starmap(func, *args, chunksize=_chunksize)
            return result

    def run_async(self, func: types.FunctionType, *args, **kwargs) -> None:
        """
        Args:
            func (function): The first parameter.
            args (list): The second parameter.
            kwargs (dict): The third parameter.

        Returns:
            None

        args Format to pass into Executor: ([(arg1, arg2, arg3...),
                                             (arg1, arg2, arg3...),
                                             (arg1, arg2, arg3...),
                                             (arg1, arg2, arg3...)])
        """
        is_pool_daemon = {"True": NestablePool, "False": Pool}

        if self._num_workers == "AUTO":
            _num_workers = cpu_count()
        else:
            _num_workers = self._num_workers

        if self._chunksize == "AUTO":
            _chunksize = len(*args) // _num_workers
        else:
            _chunksize = self._chunksize

        print(f"is daemon pool: {self._is_daemon}")
        print(f"current number of workers: {_num_workers}")
        print(f"chunksize : {_chunksize}")

        with is_pool_daemon.get(str(self._is_daemon), "False")(_num_workers) as executor:
            #print(func, *args, kwargs)
            if func.__code__.co_kwonlyargcount > 0:
                pggenericfunc.notimplemented()
            else:
                result = executor.starmap_async(func, *args,
                                            chunksize=_chunksize,
                                            callback=self.complete,
                                            error_callback=self.exception)

            if not "total_processes" in self._pg_private:
                self._pg_private["total_processes"] = 1
            else:
                self._pg_private["total_processes"] += 1

            result.get()
            return result


    def complete(self, result):
        if not "results" in self._pg_private:
            self._pg_private["results"] = []
        self._pg_private["results"].extend(result)
        if not "completed_processes" in self._pg_private:
            self._pg_private["completed_processes"] = 0
        self._pg_private["completed_processes"] += 1

        print('Progress: {:.2f}%'.format((self._pg_private["completed_processes"] /
                                          self._pg_private["total_processes"]) * 100))

    def exception(self, exception=None):
        raise Exception(exception)


if __name__ == '__main__':
    test = PGMultiProcessing()
    test.set_param()
