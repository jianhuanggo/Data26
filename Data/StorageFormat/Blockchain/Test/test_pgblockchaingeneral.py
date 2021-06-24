import unittest
import jsonpickle
import hashlib
from Data.StorageFormat.Blockchain import pgblockchaingeneral

__version__ = "1.5"


def pg_hash_object(data, level=1):
    if not data:
        return
    elif not isinstance(data, (dict,list,tuple)):
        yield data
    elif len(data) == 1:
        if isinstance(data, dict):
            for z in [[x, y] for x, y in data.items()][0]:
                yield z
        elif isinstance(data, (list, tuple)):
            yield data[0]
        else:
            yield data
    else:
        if isinstance(data, dict):
            a = data[1]
        else:
            a = data
        for c in a:
            for b in pg_hash_object(c):
                yield b

### Good TXNS
t1 = "I sends 2 gifts to you"
t2 = "You send 2.5 coin to them"
t3 = "I watch 35 netflix movies"
t4 = "XXX stores 44.45 here"
t5 = "YYY eats 5 apples"
t6 = {"jjj": 5555}


def setup():
    result1 = []
    result2 = []
    chain1 = pgblockchaingeneral.PGBlockChainGeneral()
    chain1.add([t1, t2], chain1.current_chain_hash)
    result1.append([t1, t2])
    chain1.add((t3, t4), chain1.current_chain_hash)
    result1.append((t3, t4))
    chain1.add(t5, chain1.current_chain_hash)
    result1.append(t5)
    chain1.add(t6, chain1.current_chain_hash)
    result1.append(t6)
    list(map(lambda x: result2.append(jsonpickle.decode(x[1]._bc_data)), chain1._bc_tree.items()))
    return result1, result2


class TestAddTxn(unittest.TestCase):
    def test_add_txn(self):
        """
        Test that it can sum a list of integers
        """
        list1, list2 = setup()

        self.assertEqual(hashlib.sha256('-'.join(list(map(str, pg_hash_object(list1)))).encode()).hexdigest(),
                         hashlib.sha256('-'.join(list(map(str, pg_hash_object(list2)))).encode()).hexdigest())


if __name__ == '__main__':
    unittest.main()




