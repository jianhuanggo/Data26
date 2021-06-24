from Processing import pgprocessing
from API.RealEstate import pgrealestate
from Data.Utils import pgdirectory
import inspect
import itertools


rl_proc = pgrealestate.api_real_estate(object_type="redfin", object_name="myredfin", subscription_level=2)(pgprocessing.multi_filedir_processing)

if __name__ == '__main__':
    #a = list(zip(itertools.repeat(1), ["894 Olmsted Ln, Johns Creek, GA 30097", "5770 Hershinger Close, Johns Creek, GA 30097"], itertools.repeat({})))
    #print(a)

    print(inspect.signature(rl_proc))
    print(inspect.signature(pgprocessing.multi_filedir_processing))
    path = "/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Projects/Real_estate_research/data/test"

    for filename in pgdirectory.scantree(path):
        with open(filename.path) as file:

            #a = list(zip(itertools.repeat(1),
            #             ["894 Olmsted Ln, Johns Creek, GA 30097", "5770 Hershinger Close, Johns Creek, GA 30097"],
            #             itertools.repeat({})))
            #a = ((1, 1), ("894 Olmsted Ln, Johns Creek, GA 30097", "5770 Hershinger Close, Johns Creek, GA 30097"), ({}, {}))
            #print(list(zip(*[(1, line, {}) for line in file.readlines()])))
            #exit(0)
            #print(a)
            rl_proc(*list(zip(*[(1, line, {}) for line in file.readlines()])))

            #rl_proc(*[(1, line, {}) for line in file.readlines()])




    #rl_proc([str(x) for x in range(5)])
    #print(rl_proc._pg_action.redfin.myredfin)
    #print(rl_proc._pgprocessing)


