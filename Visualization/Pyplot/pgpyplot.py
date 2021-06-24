from Visualization import pgvisualizationbase
from Meta import pgclassdefault
from Data.Utils import pgdirectory
import matplotlib.pyplot as plt


class PGPyplot(pgvisualizationbase.PGVisualizationBase, pgclassdefault.PGClassDefault):
    __instance = None

    def __init__(self, project_name: str = "pyplot", logging_enable: bool = False):
        super(PGPyplot, self).__init__(project_name=project_name,
                                       object_short_name="PG_PLT",
                                       config_file_pathname=pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".ini",
                                       logging_enable=logging_enable,
                                       config_file_type="ini")

        PGPyplot.__instance = self

    @staticmethod
    def inst():
        if PGPyplot.__instance is None:
            PGPyplot()
        else:
            return PGPyplot.__instance

    def visual(self, *args, **kwargs):
        plt.bar(kwargs['x-axis'], [0] * len(kwargs['x-axis']), color=("blue"))
        plt.pause(0.05)


if __name__ == '__main__':
    test = PGPyplot()
