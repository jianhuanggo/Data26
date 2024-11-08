import multiprocessing
import time
import random
import os
from multiprocessing import Queue
from Data.Logging import pglogging as log
from multiprocessing import Pool
from Data.Config import pgconfig
import logging
from DataMover.Core import meta
from Data.Utils.pgfile import get_random_filename
from subprocess import Popen
import itertools

global_queue = Queue()

def get_task(post_url, tablename, func="split_key"):
    global global_queue

    post_url = "postgres://u3jp0qoj4h99rj:p15d5eh2gahoo6ck2gn6qiocavp@oregon-read-only.ccfrcisbix7u.us-west-1.rds.amazonaws.com:5432/ddh3j8703l2puv"

    db_client_dbshell = f"/usr/local/bin/psql"
    print(db_client_dbshell)
    limit = ''
    quote = ''

    i = 0

    while i < 100:
        an = [i, i + 10]
        global_queue.put(an)
        i += 10


    while not global_queue.empty():
        b = global_queue.get()
        query = f"COPY (select * from {tablename} where id between {b[0]} and {b[1]}) {limit} TO stdout DELIMITER ',' CSV {quote}"
        print(query)

        loadConf = [db_client_dbshell, post_url, "-c", query]


        try:
            file_name_save = "save" + get_random_filename(tablename) + ".csv"
            f = open(file_name_save, "w")
            p2 = Popen(loadConf, stdout=f)
            p2.wait()
        except Exception as e:
            raise(f"something is wrong {e}\n")
            exit(1)




    #query = f"COPY {tablename} {limit} where id between {b[0]} and {b[1]} TO stdout DELIMITER ',' CSV {quote}"

    #print(query)
    #loadConf = [db_client_dbshell, post_url, "-c", query]
    #print(loadConf)

    #file_name_save = "save" + get_random_filename(tablename) + ".csv"
    #f = open(file_name_save, "w")
    #p2 = Popen(loadConf, stdout=f)

    #p2.wait()



class Multipro(log.Logging):
    def __init__(self, conf):
        super().__init__(conf, logging.INFO, "Multiprocessing")
        self.task_queue = Queue()
        self.process_queue = Queue()
        self.logger = self.getLogger("Multiprocessing.log")

    def setup(self, func, num_process):
        processes = []
        for i in range(num_process):
            t = multiprocessing.Process(target=func, args=(i,))
            processes.append(t)
            self.logger.info(f"Starting Processing {t}")
            t.start()

        for one_process in processes:
            one_process.join()

        mylist = []

        while not self.queue.empty():
            mylist.append(self.queue.get())

        print("Done!")
        print(len(mylist))
        print(mylist)

    def __call__(self, type, number, *args, **kwargs):
        if type == 'process':
            def wrapper(func):
                print("Trying to modify this function")
                processes = []
                #queue = Queue()
                while not self.queue.empty():
                    for i in range(number):
                        t = multiprocessing.Process(target=func, args=(i,))
                    processes.append(t)
                    self.logger.info(f"Starting Processing {t}")
                    t.start()

                mylist = []

                while not queue.empty():
                    mylist.append(queue.get())
        elif type == 'pool':
            def wrapper(func):
                print("Trying to modify this function 2")
                processes = []
                queue = Queue()


                for i in range(2):
                    t = multiprocessing.Process(target=func, args=(i,))
                    processes.append(t)
                    self.logger.info(f"Starting Processing {t}")
                    t.start()

                mylist = []

                while not queue.empty():
                    mylist.append(queue.get())



class multi(log.Logging, metaclass=meta.Scoot_Meta):

    # multiprocessing.pool.ThreadPool
    # CPU bound jobs -> multiprocessing.Pool

    def __init__(self, type, number):
        self.type = type
        self.number = number
        self.task_queue = Queue()
        self.process_queue = Queue()

    def __call__(self, func, *args, **kwargs):
        name = kwargs
        print(f"okokokok{name}")
        if self.type == 'process':

            def wrapper(*args, **kwargs):
                name = kwargs
                print(f"okokokok{name}")
                print("Trying to modify this function")
                processes = []
                for i in range(self.number):
                    t = multiprocessing.Process(target=func, args=(*args,), **kwargs)
                    processes.append(t)
                    #self.logger.info(f"Starting Processing {t}")
                    t.start()

                mylist = []

                #while not queue1.empty():
                #    mylist.append(queue1.get())
                #print(mylist)
            return wrapper
        elif self.type == 'pool':
            def wrapper(*args, **kwargs):
                print("Trying to modify this function")
                processes = []

                P = Pool()
                result = [P.apply_async(func, (*args,), **kwargs)]

                P.close()
                P.join()
                for item in result.pop():
                    print(item)

                #for i in range(2):
                #    t = multiprocessing.Process(target=func, args=(*args,), **kwargs)
                #    processes.append(t)
                    #self.logger.info(f"Starting Processing {t}")
                #    t.start()

                mylist = []

                #while not queue1.empty():
                #    mylist.append(queue1.get())
                #print(mylist)
            return wrapper

    """

    @classmethod
    def multip(cls, func, *args, **kwargs):

        def wrapper(*args, **kwargs):
            print("Trying to modify this function")
            processes = []
            P = Pool()
            for i in range(2):
                t = multiprocessing.Process(target=func, args=(*args,), **kwargs)
                processes.append(t)
                # self.logger.info(f"Starting Processing {t}")
                t.start()

                args, kw = (1, 2, 3), {'cat': 'dog'}

                print
                "# Normal call"
                f(0, *args, **kw)

                print
                "# Multicall"
                P = Pool()
                sol = [P.apply_async(f, (x,) + args, kw) for x in range(2)]
                P.close()
                P.join()

        

        mylist = []

        while not cls.queue.empty():
            mylist.append(cls.queue.get())

            print(mylist)
    return wrapper
    """


@multi('process', 8)
def helper(number):
    print(number)
#   cls.queue.put("1111")

    print(f"This is working {number}!!")

#@multi('process', 2)





if __name__ == '__main__':
    get_task("sss", "admins")
#    conf = config.Config()
#    mul = Multipro(conf)
#    mul.logger.info("I'm a logger")

    num, a, b = 11, 22, 33
#    helper(num)



