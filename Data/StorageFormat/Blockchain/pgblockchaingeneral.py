import hashlib
import jsonpickle
from Meta import pgclassdefault
from Data.StorageFormat import pgstorageformatbase
from Data.Utils import pgdirectory

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


class PGBlockChainGeneral(pgstorageformatbase.PGStorageFormatBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str ="blockchaingeneral", logging_enable: str = False):
        super(PGBlockChainGeneral, self).__init__(project_name=project_name,
                                                  object_short_name="PG_BCG",
                                                  config_file_pathname=__file__.split('.')[0] + ".ini",
                                                  logging_enable=logging_enable,
                                                  config_file_type="ini")
        self._name = None
        self._data = None
        self._storage_format = "blockchain"
        self._storage_instance = {}
        self._storage_parameter = {}

        self._bc_tree = {}
        self._current_hash = None
        self._setting_doc = """   
            Available Setting:
                bc_prev_hash (string):  previous hash for previous blockchain node
                bc_data (list): holds data
                bc_current_hash (string): blockchain hash for current blockchain node
        """

    """
    def set_param(self, *args, **kwargs):
        '''
        Args:
            args (list): The first parameter.
            kwargs (dict): The second parameter.

        Returns:
            None

        '''
        if args:
            raise Exception(f"ambiguous argument(s) {args}")

        for pkey, pval in kwargs.items():
            if f"_{pkey}" in vars(self) and f"_{pkey}" != "_pg_private":
                setattr(self, f"_{pkey}", pval)
            else:
                print(self._setting_doc)
                raise Exception(f"parameter {pkey} does not exist")
    """

    @property
    def current_chain_hash(self) -> str:
        return self._current_hash

    def inst(self):
        return self

    def add(self, data: object, latest_hash: str = None) -> bool:
        if not self._current_hash and latest_hash != self._current_hash:
            print("Add Node failed: Not the latest hash")
            return False
        try:
            _new_node = PGBlockChainGeneralNode(self._current_hash or "Genesis Block", data)
            self._bc_tree[_new_node.bc_current_hash] = _new_node
            self._current_hash = _new_node.bc_current_hash
            return True
        except Exception as err:
            raise Exception(f"Add node failed {err}")

    def encode(self, data: object = None, *args, **kwargs):
        return jsonpickle.encode(data) if not data else jsonpickle.encode(self._bc_tree)

    def decode(self, data: object, *args, **kwargs) -> bool:
        try:
            self._bc_tree = jsonpickle.decode(data)
            self._current_hash = list(self._bc_tree.keys())[-1]
            return True
        except Exception as err:
            raise Exception(f"Not able to upload jsonpickle object {err}")

    def print_data(self):
        for index, node in self._bc_tree.items():
            _bc_data = jsonpickle.decode(node.bc_data)
            print(f"data: {_bc_data}")


class PGBlockChainGeneralNode:
    def __init__(self, bc_prev_hash: str, bc_data: object):
        self._bc_prev_hash = bc_prev_hash
        self._bc_data = jsonpickle.encode(bc_data)

        _block_data = "-".join(self._bc_data) + "-" + self._bc_prev_hash
        self._bc_current_hash = hashlib.sha256(_block_data.encode()).hexdigest()

    @property
    def bc_current_hash(self):
        return self._bc_current_hash

    @property
    def bc_data(self):
        return self._bc_data




    """
    def report(self, block_hash):
        if self._prev_block_hash != "Inital_Block":
            print(self._input_data)
            return self.report(self._prev_block_hash)
    """



