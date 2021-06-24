import sys
import inspect
from scipy.stats.stats import pearsonr
from pprint import pprint
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List, Union
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, MinMaxScaler
from sklearn.datasets import load_sample_image
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.compose import ColumnTransformer
from pydantic import BaseModel, validator, ValidationError
from pydantic.generics import GenericModel
from Learning import pglearningcommon1, pglearningbase, pglearning
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
from Data.Storage import pgstorage
from Data.Utils import pgfile, pgdirectory
from sklearn.cluster import MiniBatchKMeans
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

__version__ = "1.7"

DataFrame = TypeVar('DataFrame')
_NUM_SAMPLES = 5


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


def plot_pixels(data, title, colors=None, N=10000):
    if colors is None:
        colors = data

    rng = np.random.RandomState(0)
    i = rng.permutation(data.shape[0])[:N]
    R, G, B = data[i].T

    fig, ax = plt.subplots(1, 2, figsize=(16,6))
    ax[0].scatter(R, G, color=colors, marker='.')
    ax[0].set(xlabel='Red', ylabel='Green', xlim=(0, 1), ylim=(0, 1))


    ax[1].scatter(R, B, color=colors, marker='.')
    ax[1].set(xlabel='Red', ylabel='Blue', xlim=(0, 1), ylim=(0, 1))

    fig.suptitle(title, size=20)


class PGLearningKMeans(pglearningbase.PGLearningBase, pglearningcommon1.PGLearningCommon):
    def __init__(self, project_name: str = "learninglr", logging_enable: str = False):
        super(PGLearningKMeans, self).__init__(project_name=project_name,
                                               object_short_name="PG_LRN_KM",
                                               config_file_pathname=__file__.split('.')[0] + ".ini",
                                               logging_enable=logging_enable,
                                               config_file_type="ini")

        ### Common Variables
        self._name = "learninglr"
        self._learning_type = "unsupervised"
        self._model_type = "kmeans"
        self._model = None
        self._min_record_cnt_4_pred = 1
        self._data = {}
        self._pattern_match = {}
        self._parameter = {'storage_type': 'localdisk',  # default storage to save and load models
                           'storage_name': 'lr',  # name of storage
                           'dirpath': pgdirectory.get_filename_from_dirpath(__file__) + "/model",
                           'test_size': 0.2}  # if test_flag set to true, percentage of data will be used as testing dataset

        ### Specific Variables
        if not pgdirectory.createdirectory(self._parameter['dirpath']):
            sys.exit(99)

        self._model_distribution_strategy = "centralStoragestrategy"
        self._data_inputs = {}
        # self.get_config(profile="default")

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    def model_save(self, model, entity_name: str, data: dict = {}, parameters: dict = {}) -> bool:
        """Returns True if file(s) are successfully persisted in appropriate s3 location.

        Args:
            model: Machine Learning Model instance
            entity_name: An unique Identifier used to form the model name
            data: any object which needed to be saved along with model itself
            parameters: parameters


        Returns:
            The return value. True for success, False otherwise.

            For linear regression model, we save following:
            1) trained model
            2) some sample data to express the cardinality of the non integer columns
            3) x_scaler - used to scale the training dataset
            4) y_scaler - used to scale the result dataset

            1 - 4 are packaged into a tar file
            5) A signature file

        """
        try:
            _save_model = pgstorage.pgstorage(object_type=self._parameter['storage_type'],
                                              object_name=self._parameter['storage_name'])(self._model_save)

            _parameters = {**self._parameter, **parameters}
            _entity = self.get_model_filename({'entity_name': entity_name,
                                               'model_type': self._model_type,
                                               'dataset_sn': '-'.join(
                                                   (item.replace('"', '').replace(" ", "") for item in
                                                    data['sample_data'].columns))
                                               }
                                              )

            _pg_file = {'joblib': {'1': [model, f"{_parameters['dirpath']}/{entity_name}.pgmodelsave"],
                                   '2': [data['x_scaler'], f"{_parameters['dirpath']}/{entity_name}.x_scaler"],
                                   '3': [data['y_scaler'], f"{_parameters['dirpath']}/{entity_name}.y_scaler"]},
                        'dataframe': {'1': [data['sample_data'], f"{_parameters['dirpath']}/{entity_name}.samples"]},
                        'tarfile': {'1': [f"{_parameters['dirpath']}/{entity_name}.pg_model",
                                          [f"{_parameters['dirpath']}/{entity_name}.pgmodelsave",
                                           f"{_parameters['dirpath']}/{entity_name}.x_scaler",
                                           f"{_parameters['dirpath']}/{entity_name}.y_scaler",
                                           f"{_parameters['dirpath']}/{entity_name}.samples"]]},
                        }

            _filepath = f"{_parameters['dirpath']}/{entity_name}.pg_model" if self._parameter[
                                                                                  'storage_type'] == "localdisk" else f"{_parameters['targetpath']}/{entity_name}.pg_model"

            ### Add signature file

            _pg_file['json'] = {'1': [{'entity': _entity, 'filepath': _filepath},
                                      f"{_parameters['dirpath']}/{entity_name}.pgconfig"]}

            ### if attached storage is not localdisk
            if self._parameter['storage_type'] != "localdisk":
                _pg_file[self._parameter['storage_type']] = {
                    '1': [f"##storage##storage_{self._parameter['storage_type']}_{self._parameter['storage_name']}",
                          {'mode': "file",
                           'data': f"{_parameters['dirpath']}/{entity_name}.pg_model",
                           'object_key': f"{_parameters['targetpath']}/{entity_name}.pg_model",
                           'aws_access_key_id': _parameters['aws_access_key_id'],
                           'aws_secret_access_key': _parameters['aws_secret_access_key']}],
                    '2': [f"##storage##storage_{self._parameter['storage_type']}_{self._parameter['storage_name']}",
                          {'mode': "file",
                           'data': f"{_parameters['dirpath']}/{entity_name}.pgconfig",
                           'object_key': f"{_parameters['targetpath']}/{entity_name}.pgconfig",
                           'aws_access_key_id': _parameters['aws_access_key_id'],
                           'aws_secret_access_key': _parameters['aws_secret_access_key']}],
                    }

            return _save_model(pgfiles=_pg_file)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    # @pgoperation.pg_retry(3)
    def model_load(self, entity_name: str, parameters: dict):
        try:
            _load_model = pgstorage.pgstorage(object_type=self._parameter['storage_type'],
                                              object_name=self._parameter['storage_name'])(self._model_load)

            _parameters = {**self._parameter, **parameters}
            _pg_file = {}

            if _parameters['storage_type'] != "localdisk":
                _pg_file = {_parameters['storage_type']: {
                    '1': [f"##storage##storage_{_parameters['storage_type']}_{_parameters['storage_name']}",
                          {'mode': "file",
                           'directory': f"{_parameters['dirpath']}",
                           'object_key': f"{_parameters['targetpath']}/{entity_name}.pg_model",
                           'aws_access_key_id': _parameters['aws_access_key_id'],
                           'aws_secret_access_key': _parameters['aws_secret_access_key']}],
                    '2': [f"##storage##storage_{_parameters['storage_type']}_{_parameters['storage_name']}",
                          {'mode': "file",
                           'directory': f"{_parameters['dirpath']}",
                           'object_key': f"{_parameters['targetpath']}/{entity_name}.pgconfig",
                           'aws_access_key_id': _parameters['aws_access_key_id'],
                           'aws_secret_access_key': _parameters['aws_secret_access_key']}],
                    }
                            }

            _pg_file['tarfile'] = {'1': [f"{_parameters['dirpath']}/{entity_name}.pg_model", None]}
            _pg_file['joblib'] = {'model': [f"{_parameters['dirpath']}/{entity_name}.pgmodelsave", "save"],
                                  'x_scaler': [f"{_parameters['dirpath']}/{entity_name}.x_scaler", "save"],
                                  'y_scaler': [f"{_parameters['dirpath']}/{entity_name}.y_scaler", "save"]
                                  }
            _pg_file['dataframe'] = {'samples': [f"{_parameters['dirpath']}/{entity_name}.samples", "save"]}

            """
            'joblib': {'1': [model, f"{_parameters['dirpath']}/{entity_name}.pgmodelsave"],
                                   '2': [data['x_scaler'], f"{_parameters['dirpath']}/{entity_name}.x_scaler"],
                                   '3': [data['y_scaler'], f"{_parameters['dirpath']}/{entity_name}.y_scaler"]},
                        'dataframe': {'1': [data['sample_data'], f"{_parameters['dirpath']}/{entity_name}.samples"]},

            """

            return _load_model(pgfiles=_pg_file)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def data_preprocessing(self, pgdataset: Union[pd.DataFrame, np.ndarray], pgscaler = None, parameter: dict = None) -> Union[np.ndarray, None]:
        try:
            _data = pgdataset.to_numpy() if isinstance(pgdataset, pd.DataFrame) else pgdataset
            #_scaler = MinMaxScaler(feature_range=(0, 1))
            #_data = _data.transpose((2, 0, 1)).reshape(3,-1)

            _data = _data / 255.0
            #print(np.subtract(_data1, _data))
            #exit(0)

            _data = _data.reshape(_data.shape[0] * _data.shape[1], _data.shape[2])
            print(_data.shape)
            return _data
            # _processed_data = pg_convert_categorical_data(pgdataset, "auto")
            # return pgscaler.fit_transform(pd.get_dummies(pgdataset, drop_first=True).to_numpy())

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def model_train(self, pgdataset: Union[list, np.ndarray], data_scaler=None, parameter: dict = None):

        try:
            self._model = MiniBatchKMeans(16)
            self._model.fit(pgdataset)
            return self._model

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def model_test(self, model, x_test: list, y_test: list, data_scaler=None, parameter: dict = None) -> Union[
        float, None]:
        try:
            y_pred = model.predict(x_test)
            _predicted = data_scaler.inverse_transform(y_pred) if data_scaler else y_pred
            return self.pg_model_r2_score(y_test, _predicted)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def get_model_filename(self, parameters: dict) -> str:
        try:
            _filename = ""
            for _key, _val in parameters.items():
                _filename += f"{str(_key)}-{str(_val)}_"
            return '_'.join(_filename.split('_')[:-1])

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return ""

    def get_tasks(self, dataset: Union[pd.DataFrame, dict, str], parameter: dict = None) -> bool:
        try:
            if isinstance(dataset, str):
                ### header is required
                self._data_inputs[parameter['name']] = {'parameter': parameter, 'data': pd.read_csv(dataset, header=0)}
            elif isinstance(dataset, dict):
                self._data_inputs[parameter['name']] = {'parameter': parameter,
                                                        'data': pd.DataFrame.from_dict(dataset, orient='index')}
            elif isinstance(dataset, pd.DataFrame):
                self._data_inputs[parameter['name']] = {'parameter': parameter, 'data': dataset}

            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    @staticmethod
    def _val_dimension(pgdataset_1: np.ndarray, pgdataset_2: np.ndarray) -> bool:
        _pgdataset_1 = np.asarray(pgdataset_1) if isinstance(pgdataset_1, list) else pgdataset_1
        _pgdataset_2 = np.asarray(pgdataset_2) if isinstance(pgdataset_2, list) else pgdataset_2
        return True if _pgdataset_1.shape[1] == _pgdataset_2.shape[1] else False

    @staticmethod
    def _val_min_record(pgprediction_dataset: np.ndarray, min_record_cnt: int) -> bool:
        return True if len(pgprediction_dataset) >= min_record_cnt else False

    @staticmethod
    def display_side_by_side(original, new):
        fig, ax = plt.subplots(1, 2, figsize=(16, 6), subplot_kw=dict(xticks=[], yticks=[]))
        fig.subplots_adjust(wspace=0.05)
        ax[0].imshow(original)
        ax[0].set_title('Original Image', size=16)
        ax[1].imshow(new)
        ax[1].set_title('16-color Image', size=16)
        plt.show()


    def _process(self, pgdataset, parameters: dict, *args, **kwargs) -> Union[float, int, None]:

        #_x_scaler = MinMaxScaler(feature_range=(0, 1))
        #_y_scaler = None

        try:
            _load_model_config = self.model_load(parameters['name'], {**self._parameter, **parameters})
            _model = _load_model_config['model'] if _load_model_config and "model" in _load_model_config else None
            if _model:
                #_x_scaler = self.model_load(parameters['name'], {**self._parameter, **parameters})['x_scaler']
                #_y_scaler = self.model_load(parameters['name'], {**self._parameter, **parameters})['y_scaler']
                _data = pd.DataFrame.from_dict(
                    self.model_load(parameters['name'], {**self._parameter, **parameters})['samples'])
            else:
                print(f"model not found, start training a model for the dataset")
                _data = self.data_preprocessing(pgdataset)
                _model = self.model_train(_data)

                #self.model_save(model=_model, entity_name=parameters['name'], data={'sample_data': pgdataset,
                #                                                                    'x_scaler': _x_scaler,
                #                                                                    'y_scaler': _y_scaler})

            new_colors = _model.cluster_centers_[_model.predict(_data)]
            print(new_colors.shape)
            china_recolored = new_colors.reshape(pgdataset.shape)

            self.display_side_by_side(pgdataset, china_recolored)


        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def process(self, name: str = None, *args: object, **kwargs: object) -> bool:
        try:
            if name:
                _item = self._data_inputs[name]
                self._data[name] = self._process(_item['data'], _item['parameter'], )
            else:
                for _index, _data in self._data_inputs.items():
                    _item = self._data_inputs[_index]
                    self._data[_index] = self._process(_item['data'], _item['parameter'])
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    @staticmethod
    def pg_model_r2_score(actual, predicted) -> Union[float, None]:
        return r2_score(actual, predicted)


class PGLearningKMeansExt(PGLearningKMeans):
    def __init__(self, project_name: str = "learningmeansmext", logging_enable: str = False):
        super(PGLearningKMeansExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGLearningKMeansImage(PGLearningKMeans):
    def __init__(self, project_name: str = "learningmeansimage", logging_enable: str = False):
        super(PGLearningKMeansImage, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def data_preprocessing(self, pgdataset: Union[pd.DataFrame, np.ndarray], pgscaler = None, parameter: dict = None) -> Union[np.ndarray, None]:
        try:
            _data = pgdataset.to_numpy() if isinstance(pgdataset, pd.DataFrame) else pgdataset
            #_scaler = MinMaxScaler(feature_range=(0, 1))
            _data = _data.transpose((2, 0, 1)).reshape(3, -1) / 255.0
            print(_data.shape)
            return _data.reshape(-1, 3)

            #return _data.reshape(_data[0] * _data[1], -1)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None
    """
    def model_train(self, pgdataset: Union[list, np.ndarray], data_scaler=None, parameter: dict = None):

        #_data = self.data_preprocessing(pgdataset, {})
        try:

            self._model = MiniBatchKMeans(16)
            self._model.fit(pgdataset)
            return self._model

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise
    """

    #def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
    #    pass


class PGLearningKMeansSingleton(PGLearningKMeans):
    __instance = None

    @staticmethod
    def get_instance():
        if PGLearningKMeansSingleton.__instance == None:
            PGLearningKMeansSingleton()
        else:
            return PGLearningKMeansSingleton.__instance

    def __init__(self, project_name: str = "learninglstmsingleton", logging_enable: str = False):
        super(PGLearningKMeansSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGLearningKMeansSingleton.__instance = self


if __name__ == '__main__':
    test = PGLearningKMeansImage()
    test.set_profile("cloud_storage")
    china = load_sample_image("flower.jpg")
    print(type(china))

    #plot_pixels(china, title='Input color space: 16 million possible colors')
    #ax = plt.axes(xticks=[], yticks=[])
    #ax.imshow(china)
    #plt.show()
    test._process(china, {'name': "test1"})
    exit(0)


    # print(test.parameter)
    test.get_tasks('train10.csv', {'name': "city7", 'prediction': pd.read_csv('prediction10.csv')})
    # print(test._data_inputs)
    test.process()
    print(test.data)








