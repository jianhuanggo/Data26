from Data.StorageFormat.Jsonpickle import pgjsonpickle
from Data.StorageFormat.Blockchain import pgblockchaingeneral
import jsonpickle


if __name__ == '__main__':

    Obj = pgblockchaingeneral.PGBlockChainGeneral("Genesis Block", ["hahah"])

    test = pgjsonpickle.PGJsonPickle()
    print(test.unpicklable)
    #oneway = test.encode(Obj, unpicklable=False)
    twoways = jsonpickle.encode(Obj._bc_tree)
    print(twoways)
    result = jsonpickle.decode(twoways)
    #assert obj.name == result['name'] ==