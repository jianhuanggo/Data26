import types
import itertools
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from Meta import pgclassdefault2
from Processing import pgprocessingbase
from Data.Utils import pgdirectory

__version__ = "1.5"


class PGCoroutines(pgprocessingbase.PGProcessingBase, pgclassdefault2.PGClassDefault):
    def __init__(self, project_name: str = "multithreading", logging_enable: bool = False):
        """
        Args:
            project_name (int): The first parameter.
            logging_enable (bool): The second parameter.

        """
        super(PGCoroutines, self).__init__(project_name=project_name,
                                           object_short_name="PG_CT",
                                           config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml",
                                           logging_enable=logging_enable)

        self._pg_private = {}
        self._num_workers = None
        self._pool_executor = None
        self._setting_doc = """   
            Available Setting:


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

    async def run(self, func: types.FunctionType, *args, **kwargs):
        """
        Args:
            func (function): The first parameter.
            args (list): The second parameter.
            kwargs (dict): The third parameter.

        Returns:
            None

        args Format to pass into Executor: [(arg1, arg2, arg3...),
                                            (arg2, arg2, arg2...),
                                            (arg3, arg3, arg3...),
                                            (arg4, arg4, arg4...)]

        func must be a async function
        """

        if isinstance(self._pool_executor, (ThreadPoolExecutor, ProcessPoolExecutor)):
            _pool_executor = self._pool_executor
        else:
            _pool_executor = None

        #print(args)
        ### unpack_args:  change ((arg1, arg2), (arg1, arg2), (arg1, arg2)...) => [(arg1, arg1, arg1...), (arg2, arg2, arg2...)]

        loop = asyncio.get_event_loop()
        if any(isinstance(i, (list, tuple, dict)) for i in args):
            # func((arg1,)) or func((arg1, arg2))
            _unpack_args = iter(zip(*args))
            blocking_tasks = [map(lambda x: loop.run_in_executor(*x),
                                  iter(zip(itertools.repeat(_pool_executor), itertools.repeat(func), *_unpack_args)))]

        else:
            # func(arg1)
            #_unpack_args = tuple(list(args))
            blocking_tasks = [map(lambda x: loop.run_in_executor(*x),
                                  iter(zip(itertools.repeat(_pool_executor), itertools.repeat(func), args)))]


        #comb_single_valuable = list(zip(itertools.repeat(_pool_executor), itertools.repeat(func), args))
        #comb_multiple_valuables = list(zip(itertools.repeat(_pool_executor), itertools.repeat(func), *unpack_args))

        if self._logger:
            self._logger.info('waiting for executor tasks')

        completed, pending = await asyncio.wait(*blocking_tasks)
        results = [t.result() for t in completed]
        if self._logger:
            self._logger.info('results: {!r}'.format(results))
            self._logger.info('exiting')
        else:
            print(results)
        return results

    async def run_async(self, func: types.FunctionType, *args, **kwargs):
        """
        Note** func must be a async function

        Args:
            func (function): The first parameter.
            args (list): The second parameter.
            kwargs (dict): The third parameter.

        Returns:
            None

        args Format to pass into Executor: [(arg1, arg2, arg3...),
                                            (arg2, arg2, arg2...),
                                            (arg3, arg3, arg3...),
                                            (arg4, arg4, arg4...)]


        """
        #await asyncio.gather(*args)

        await asyncio.gather(*list(map(lambda x: func(*x), args)))







    """
            def async_to_sync(loop, this_func):
            def func_(*arguments, **kwds):
                return asyncio.run_coroutine_threadsafe(this_func(*arguments, **kwds), loop).result()
            return func_

        def sync_code(func):
            for i in range(10):
                func(i)

        async def async_cb(a):
            print("async callback:", a)

        async def main():
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, sync_code, async_to_sync(loop, async_cb))
    
    """