from types import SimpleNamespace
from Data.Config import pgconfig


class PGUtilsBase:
    def __init__(self):
        self._config = pgconfig.Config("PGUtils_")


    def __repr__(self):
        return f""