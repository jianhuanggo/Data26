import logging
import time
import asyncio
import random
import unittest
import matplotlib.pyplot as plt
from Data.Utils import pgtimeit
from pprint import pprint
from Processing import pgprocessing
from Data.StorageFormat.Blockchain import pgblockchaingeneral
from Visualization import pgvisualizationdecorator
from Meta import pggenericfunc

__version__ = "1.5"


chain = pgblockchaingeneral.PGBlockChainGeneral()

MAX_NODES = 100
NUM_CLIENT = 8

SCORE = [0] * NUM_CLIENT


@pgvisualizationdecorator.visualize("pyplot")
def client(n, visualize=None):
    node_cnt = 0
    while True:
        log = logging.getLogger('blocks({})'.format(n))
        log.info('running')
        time.sleep(random.randint(0, 5))
        if chain.add(f"adding transation from client {n}", chain.current_chain_hash):
            log.info(f"Client {n}: Successfully added transaction the chain")
            node_cnt += 1
            SCORE[n] += 1
            b = ([x + 1 for x in range(NUM_CLIENT)], SCORE)
            plt.bar([x + 1 for x in range(NUM_CLIENT)], SCORE, color=("blue"))
            plt.pause(1)
        else:
            log.info(f"Client {n}: I'm too late")
        log.info('done')
        if len(chain._bc_tree) > MAX_NODES:
            break
        else:
            chain_length = len(chain._bc_tree)
            log.info(f"the length of chain: {chain_length}")
    return {f"client {n}": f"{node_cnt}"}


async def run_blocking_tasks(client_num: int, executor=None):

    variable_list = []
    for i in range(client_num):
        variable_list.append(i)
        
    result = await executor.run(client, *variable_list)
    print(SCORE)
    return result


@pgtimeit.timer(1, 1)
def func_test(func_name: str, client_num: int):
    func_map = {'io_mt': io_mt,
                'mt_io': mt_io,
                'mp_io': mp_io,
                'io_mp': io_mp,
                'mt_mp_io': mt_mp_io,
                'io_mp_mt': io_mp_mt,
                'others': pggenericfunc.notimplemented

    }
    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(
            func_map.get(func_name, "others")(client_num)


        )

    finally:
        event_loop.close()



io_mt = pgprocessing.pg_asyncio()(pgprocessing.pg_multithreading(num_workers=NUM_CLIENT)(run_blocking_tasks))
mt_io = pgprocessing.pg_multithreading(num_workers=6)(pgprocessing.pg_asyncio()(run_blocking_tasks))
io_mp = pgprocessing.pg_asyncio()(pgprocessing.pg_multiprocessing(num_workers=6)(run_blocking_tasks))
mp_io = pgprocessing.pg_multiprocessing(num_workers=6)(pgprocessing.pg_asyncio()(run_blocking_tasks))
mt_mp_io = pgprocessing.pg_multithreading(num_workers=1)(pgprocessing.pg_multiprocessing(num_workers=6)(pgprocessing.pg_asyncio()(run_blocking_tasks)))
io_mp_mt = pgprocessing.pg_asyncio()(pgprocessing.pg_multiprocessing()(pgprocessing.pg_multithreading(num_workers=1)(run_blocking_tasks)))


class TestClient(unittest.TestCase):
    def test_simulate_multiclient(self):
        """
        Test multiple clients
        """
        func_test("io_mt", NUM_CLIENT)
        plt.show()

        self.assertEqual(len(chain._bc_tree), MAX_NODES + NUM_CLIENT)


if __name__ == '__main__':
    unittest.main()




