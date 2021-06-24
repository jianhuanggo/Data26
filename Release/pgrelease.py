import pgreleasebase
from Meta import pgclassdefault
from Data.Utils import pgdirectory

__version__ = "1.5"


class PGRelease(pgreleasebase.PGReleaseBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str = "release", logging_enable: bool = False):
        super(PGRelease, self).__init__(project_name=project_name,
                                        object_short_name="PG_REL",
                                        config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".ini",
                                        logging_enable=logging_enable,
                                        config_file_type="ini")

        self._release = {"1.5": ["Inheritance from meta default class and specific base class",
                                 "Standarization on method calling",
                                 "Unit test added for each module"]}

    def release(self):
        pass


if __name__ == '__main__':
    test = PGRelease()
