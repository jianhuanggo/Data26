import types
import itertools
import asyncio
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures
from Meta import pgclassdefault2
from Meta import pggenericfunc
from Processing import pgprocessingbase
from Data.Utils import pgdirectory

__version__ = "1.5"


class PGMultiThreading(pgprocessingbase.PGProcessingBase, pgclassdefault2.PGClassDefault):
    def __init__(self, project_name: str = "multithreading", logging_enable: bool = False):
        """
        Args:
            project_name (int): The first parameter.
            logging_enable (bool): The second parameter.

        """
        super(PGMultiThreading, self).__init__(project_name=project_name,
                                               object_short_name="PG_MP",
                                               config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml",
                                               logging_enable=logging_enable)

        #len(tasks) // num_threads
        self._pg_private = {}
        self._num_workers = 1
        self._chunksize = 1
        self._setting_doc = """   
            Available Setting:
                num_workers (int):  the number of workers allocated
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
                #print(f"key {pkey} exists")
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
        ### Remove two outside structure
        ### first one done thru unpacking below in [unpacking_arg]
        [unpacking_arg] = itertools.chain(args)
        args_list = list(map(tuple, itertools.zip_longest(*unpacking_arg, fillvalue=None)))

        print(f"current number of workers: {self._num_workers}")
        print(f"chunksize : {self._chunksize}")
        with ThreadPoolExecutor(max_workers=self._num_workers) as executor:
            if func.__code__.co_kwonlyargcount > 0:
                pggenericfunc.notimplemented()
            else:
                ### pass in as position arguments
                result = executor.map(func, *args_list, chunksize=self._chunksize)
                print(result)
                executor.shutdown(wait=True)
                #result = await asyncio.get_running_loop().run_in_executor(executor, func, *args_list)
                #print('custom process pool', result)
                return result

    def run_native_ordered(self, func: types.FunctionType, *args) -> None:
        """
        Args:
            func (function): The first parameter.
            args (list): The second parameter.
            args is expected to be in the format ([arg1, arg1, ...], [arg2, arg2, ...], [arg3, arg3, ...])

        Returns:
            None

        Maps guarantee the order of inputs to output as FIFO

        args_list Format to pass into Executor: [(arg1, arg1, arg1...),
                                                 (arg2, arg2, arg2...),
                                                 (arg3, arg3, arg3...),
                                                 (arg4, arg4, arg4...)]
        """
        [args_list] = itertools.chain(args)
        print(f"current number of workers: {self._num_workers}")
        print(f"chunksize : {self._chunksize}")
        with ThreadPoolExecutor(max_workers=self._num_workers) as executor:
            if func.__code__.co_kwonlyargcount > 0:
                pggenericfunc.notimplemented()
            else:
                result = executor.map(func, *args_list, chunksize=self._chunksize)
                print(result)
                executor.shutdown(wait=True)
            return result

    def run_native_unordered(self, func: types.FunctionType, *args) -> None:
        """
        Args:
            func (function): The first parameter.
            args (list): The second parameter.
            args is expected to be in the format ([arg1, arg1, ...], [arg2, arg2, ...], [arg3, arg3, ...])

        Returns:
            None

        submit DOES NOT guarantee the order of output which results in some cases faster performance

        args_list Format to pass into Executor: [(arg1, arg2, arg3...),
                                                 (arg1, arg2, arg3...),
                                                 (arg1, arg2, arg3...),
                                                 (arg1, arg2, arg3...)]
        """
        [args_list] = itertools.chain(args)
        print(f"current number of workers: {self._num_workers}")
        print("chunksize : NA")
        with ThreadPoolExecutor(max_workers=self._num_workers) as executor:
            if func.__code__.co_kwonlyargcount > 0:
                pggenericfunc.notimplemented()
            else:
                result_futures = list(map(lambda x: executor.submit(func, *x), args_list))
                results = [f.result() for f in futures.as_completed(result_futures)]
            return results

    """
    def run_asyncio(self, func, *args, **kwargs) -> None:

        ### Remove two outside structure
        ### first one done thru unpacking below in [unpacking_arg]
        [unpacking_arg] = itertools.chain(args)
        args_list = list(map(tuple, itertools.zip_longest(*unpacking_arg, fillvalue=None)))

        print(f"current number of workers: {self._num_workers}")
        print(f"chunksize : {self._chunksize}")
        with ThreadPoolExecutor(max_workers=self._num_workers) as executor:
            ### pass in as position arguments
            result = await asyncio.get_running_loop().run_in_executor(executor, func, *args_list)
            print('custom process pool', result)
    """
