import inspect
from Meta import pggenericfunc, pgclassdefault


class PGStorageCommon(pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str,
                 object_short_name: str,
                 config_file_pathname: str,
                 logging_enable: str,
                 config_file_type: str):

        super().__init__(project_name=project_name,
                         object_short_name=object_short_name,
                         config_file_pathname=config_file_pathname,
                         logging_enable=logging_enable,
                         config_file_type=config_file_type)

        self._name = {}
        self._storage_parameter = {}  ### storage parameters
        self._storage_format_instance = {}  ### instances of storage formats
        self._data = []  ### self._data must be declared as empty array. load function will load the data into this variable
        self._direct_write_storage_format = {'excel': "reserved"}  ###  storage format where it has functions dealing with storage directly



