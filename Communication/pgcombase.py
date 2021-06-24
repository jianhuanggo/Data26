from types import SimpleNamespace
from Data.Config import pgconfig
import Meta.pgclassdefault as pgclassdefault


class PGCommBase(pgclassdefault.PGClassDefault):
    def __init__(self, project_name, object_short_name, config_file_pathname, logging_enable):
        super(PGCommBase, self).__init__(project_name=project_name,
                                         object_short_name=object_short_name,
                                         config_file_pathname=config_file_pathname,
                                         logging_enable=logging_enable)
        #self._config = pgconfig.Config("PGCOMM_")


    def __repr__(self):
        return f""