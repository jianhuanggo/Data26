import inspect
import warnings
import pandas as pd
from scipy.stats.stats import pearsonr
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from Learning import pglearningcommon2, pglearningbase, pglearning
from Meta import pggenericfunc
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, cohen_kappa_score, roc_auc_score, \
    confusion_matrix
from sklearn.metrics import mean_squared_error
from typing import List
from typing import Generic, TypeVar, Optional, List
from pydantic import BaseModel, validator, ValidationError
from pydantic.generics import GenericModel
warnings.simplefilter(action='ignore', category=FutureWarning)



__version__ = "1.7"

DataFrame = TypeVar('DataFrame')


###https://www.kaggle.com/bunny01/predict-stock-price-of-apple-inc-with-lstm
###https://www.kaggle.com/amberhahn/using-f-score-to-evaluate-the-lstm-model


class Data(GenericModel, Generic[DataFrame]):
    symbol: str
    data_frame: Optional[DataFrame]
    size: int = None
    """
    class Config:
        json_encoders = {
            Optional[DataFrame]: lambda x: x.to_json(),
        }
    """


def pgdp_scaler(capacity: float, val: [pd.DataFrame, list]) -> Tuple[int, list]:
    _scale_factor = 1000
    _min = min(val)
    while _min * _scale_factor < 1000:
        _scale_factor *= 10
    return int(capacity * _scale_factor), [int(x * _scale_factor) for x in val]


def pgdp2_withassignment(capacity: float, items: pd.DataFrame, _logger=None) -> Union[Tuple[int, list], None]:

    try:
        _capacity, _cost = pgdp_scaler(capacity, items.iloc[:, 1])
        _value = items.iloc[:, 2]

        _pg = [[0 for x in range(_capacity + 1)] for x in range(len(items) + 1)]
        _explained = [[[] for x in range(_capacity + 1)] for x in range(len(items) + 1)]

        for i in range(len(items) + 1):
            for w in range(_capacity + 1):
                if i == 0 or w == 0:
                    _pg[i][w] = 0
                elif _cost[i-1] <= w:
                    _current_max = _value[i - 1] + _pg[i - 1][w - _cost[i - 1]]
                    _previous_max = _pg[i - 1][w]
                    if _current_max > _previous_max:
                        _pg[i][w] = _current_max
                        _explained[i][w] = [i - 1] + _explained[i - 1][w - _cost[i - 1]]
                    else:
                        _pg[i][w] = _previous_max
                        _explained[i][w] = _explained[i - 1][w]

                else:
                    _pg[i][w] = _pg[i - 1][w]
                    _explained[i][w] = _explained[i - 1][w]

        return _pg[len(items)][_capacity], _explained[len(items)][_capacity]

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, err)
        return None


def pgdp2(capacity: float, items: pd.DataFrame, _logger=None) -> Union[int, None]:
    try:
        _capacity, _cost = pgdp_scaler(capacity, items.iloc[:, 1])
        _value = items.iloc[:, 2]
        pg = [[0 for x in range(_capacity + 1)] for x in range(len(items) + 1)]

        for i in range(len(items) + 1):
            for w in range(_capacity + 1):
                if i == 0 or w == 0:
                    pg[i][w] = 0
                elif _cost[i - 1] <= w:
                    pg[i][w] = max(_value[i - 1] + pg[i - 1][w - _cost[i - 1]],  pg[i - 1][w])
                else:
                    pg[i][w] = pg[i - 1][w]
        return pg[len(items)][_capacity]

    except Exception as err:
        pggenericfunc.pg_error_logger(_logger, inspect.currentframe().f_code.co_name, err)
        return None


class PGLearningDP(pglearningbase.PGLearningBase, pglearningcommon2.PGLearningCommon):
    def __init__(self, project_name: str = "learningdp", logging_enable: str = False):
        super(PGLearningDP, self).__init__(project_name=project_name,
                                           object_short_name="PG_LRN_DP",
                                           config_file_pathname=__file__.split('.')[0] + ".ini",
                                           logging_enable=logging_enable,
                                           config_file_type="ini")

        ### Common Variables
        self._name = "learningdp"
        self._data = {}
        self._pattern_match = {}
        self._parameter = {}
        self._data_inputs = {}

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    ### provide entity_name, dataset itself and parameters associated with the dataset
    # @pgoperation.pg_retry(3)
    def get_tasks(self, dataset: Union[pd.DataFrame, dict, str], parameter: dict = None) -> bool:
        try:
            if isinstance(dataset, str):
                self._data_inputs[parameter['name']] = {'parameter': {'capacity': parameter['capacity']}, 'data': pd.read_csv(dataset, header=0)}
            elif isinstance(dataset, dict):
                self._data_inputs[parameter['name']] = {'parameter': {'capacity': parameter['capacity']}, 'data': pd.DataFrame.from_dict(dataset, orient='index').rename(columns={0: 'item_name', 1: 'cost', 2: 'value'})}
            elif isinstance(dataset, pd.DataFrame):
                self._data_inputs[parameter['name']] = {'parameter': {'capacity': parameter['capacity']}, 'data': dataset}

            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _process(self, capacity: float, items: pd.DataFrame, *args, **kwargs) -> Union[Tuple[int, list], None]:
        try:
            return pgdp2_withassignment(capacity, items, self._logger)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def process(self, name: str = None, *args: object, **kwargs: object) -> bool:
        try:
            if name:
                _item = self._data_inputs[name]
                self._data[name] = self._process(_item['parameter']['capacity'], _item['data'])
            else:
                for _index, _data in self._data_inputs.items():
                    _item = self._data_inputs[_index]
                    self._data[_index] = self._process(_item['parameter']['capacity'], _item['data'])
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


class PGLearningDPExt(PGLearningDP):
    def __init__(self, project_name: str = "learningdpext", logging_enable: str = False):
        super(PGLearningDPExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGLearningDPSingleton(PGLearningDP):
    __instance = None

    @staticmethod
    def get_instance():
        if PGLearningDPSingleton.__instance == None:
            PGLearningDPSingleton()
        else:
            return PGLearningDPSingleton.__instance

    def __init__(self, project_name: str = "learningdpsingleton", logging_enable: str = False):
        super(PGLearningDPSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGLearningDPSingleton.__instance = self


if __name__ == '__main__':
    #test = pd.read_csv('test.csv', header=0)
    test = PGLearningDP()
    ### name and capacity is required number
    test.get_tasks('test.csv', {'capacity': 2, 'name': 'test'})
    test.get_tasks('test.csv', {'capacity': 2, 'name': 'test1'})
    test.process()
    print(test.data)









