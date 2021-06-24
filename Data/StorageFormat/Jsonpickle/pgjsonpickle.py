import jsonpickle
from Meta import pgclassdefault
from Data.StorageFormat import pgstorageformatbase
from Data.Utils import pgdirectory


class PGJsonPickle(pgstorageformatbase.PGStorageFormatBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str ="jsonpickle", logging_enable: str = False):
        super(PGJsonPickle, self).__init__(project_name=project_name,
                                           object_short_name="PG_SF_JP",
                                           config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".ini",
                                           logging_enable=logging_enable,
                                           config_file_type="ini")

        try:
            if str(self._config.parameters["config_file"]['default']['unpicklable']):
                self._unpicklable = self._config.parameters["config_file"]['default']['unpicklable']
        except Exception as err:
            print("unpicklable setting is not found in the default section of ini file, set it to True")
            self._unpicklable = True


    @property
    def unpicklable(self):
        return self._unpicklable

    def inst(self):
            return self

    def encode(self, *args, **kwargs):
        return jsonpickle.encode(self, unpicklable=self._unpicklable)

    def decode(self, data: object, *args, **kwargs):
        return jsonpickle.decode(data)


