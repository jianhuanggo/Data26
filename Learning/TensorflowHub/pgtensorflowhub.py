import sys
import inspect
import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_datasets as tfds
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, MinMaxScaler
from sklearn.metrics import r2_score
from sklearn.compose import ColumnTransformer
from sklearn.datasets import make_classification
from sklearn.datasets import make_regression
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from matplotlib import pyplot
from pydantic.generics import GenericModel
from Learning import pglearningcommon1, pglearningbase, pglearning
from Meta.pggenericfunc import check_args
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
from Data.Storage import pgstorage
from Data.Utils import pgfile, pgdirectory


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


class PGLearningTFHub(pglearningbase.PGLearningBase, pglearningcommon1.PGLearningCommon):
    def __init__(self, project_name: str = "learningtfh", logging_enable: str = False):
        super(PGLearningTFHub, self).__init__(project_name=project_name,
                                              object_short_name="PG_LRN_TFH",
                                              config_file_pathname=__file__.split('.')[0] + ".ini",
                                              logging_enable=logging_enable,
                                              config_file_type="ini")

        ### Common Variables
        self._name = "learningtfh"
        self._model_type = "random_forest_regression"
        self._model = None
        self._min_record_cnt_4_pred = 1
        self._data = {}
        self._pattern_match = {}
        self._parameter = {'storage_type': 'localdisk',  # default storage to save and load models
                           'storage_name': 'rf',  # name of storage
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

    def _model_selection(self, data: Union[pd.DataFrame, np.ndarray]):
        try:
            #layers = model.layers
            #first_layer = layers[0]  # usually the first layer is the input layer
            #print(first_layer.input_shape)
            #model.signatures['serving_default'].variables
            self._model_type = hub.keras_layer('https://tfhub.dev/google/cropnet/feature_vector/concat/1')

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def data_preprocessing(self, pgdataset: pd.DataFrame, pgscaler, parameter: dict = None) -> Union[np.ndarray, None]:
        pass


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

            _pg_file = {'joblib': {'1': [model, f"{_parameters['dirpath']}/{entity_name}.pg_model"]}}
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

            _pg_file['joblib'] = {'model': [f"{_parameters['dirpath']}/{entity_name}.pg_model", "save"]}

            return _load_model(pgfiles=_pg_file)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def model_train(self, X_train: Union[list, np.ndarray], y_train: Union[list, np.ndarray], parameter: dict = None):

        self._model_selection(y_train)
        _model = tf.keras.Sequential([self._model_type,
                                     tf.keras.layers.Dense(64, activation='relu'),
                                     tf.keras.layers.Dense(parameter['num_classes'])
                                     ])

        try:
            _model.compile(
                loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                optimizer=tf.keras.optimizers.Adam(),
                metrics=['accuracy'])
            _model.fit(X_train, epochs= 10, validation_data=y_train)
            return _model

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def model_test(self, model, x_test: list, y_test: list, data_scaler=None, parameter: dict = None) -> Union[float, None]:

        try:
            y_pred = model.predict(x_test)
            _predicted = data_scaler.inverse_transform(y_pred) if data_scaler else y_pred
            return self.pg_model_r2_score(y_test, _predicted)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def get_model_filename(self, parameters: dict) -> str:

        check_args(inspect.currentframe().f_code.co_name,{'parameters': parameters, }, False)
        try:
            _filename = ""
            for _key, _val in parameters.items():
                _filename += f"{str(_key)}-{str(_val)}_"
            return '_'.join(_filename.split('_')[:-1])

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return ""

    def get_tasks(self, pgdataset: Union[pd.DataFrame, dict, str], parameters: dict = None) -> bool:

        check_args(inspect.currentframe().f_code.co_name,
                   {'pgdataset': pgdataset, 'parameters': parameters, }, False)

        try:
            if isinstance(pgdataset, str):
                ### header is required
                self._data_inputs[parameters['name']] = {'parameter': parameters, 'data': pd.read_csv(pgdataset, header=0)}
            elif isinstance(pgdataset, dict):
                self._data_inputs[parameters['name']] = {'parameter': parameters,
                                                        'data': pd.DataFrame.from_dict(pgdataset, orient='index')}
            elif isinstance(pgdataset, pd.DataFrame):
                self._data_inputs[parameters['name']] = {'parameter': parameters, 'data': pgdataset}

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

    def _process(self, pgdataset, parameters: dict, *args, **kwargs) -> Union[float, int, None]:

        check_args(inspect.currentframe().f_code.co_name,
                   {'pgdataset': pgdataset, 'parameters': parameters, }, False)

        try:
            if "prediction" in parameters:
                if not self._val_dimension(pgdataset.to_numpy()[:, : -1], parameters['prediction'].to_numpy()):
                    pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                  "error!! dimension mismatch between training dataset and prediction dataset ")

                if not self._val_min_record(parameters['prediction'], self._min_record_cnt_4_pred):
                    pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                  f"error!! minimum record is less than {self._min_record_cnt_4_pred}")

            _load_model = self.model_load(parameters['name'], {**self._parameter, **parameters})
            _model = _load_model['model'] if _load_model and "model" in _load_model else None

            if not _model:
                print(f"model not found, start training a model for the dataset")
                _model = self.model_train(pgdataset, parameters['validation'], parameters['num_classes'])
                exit(0)
                self.model_save(model=_model, entity_name=parameters['name'], data={'sample_data': pgdataset})

            if "prediction" in parameters:
                return _model.predict(parameters['prediction'])

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

    @staticmethod
    def data_gen_dataset(batch_num: int) -> Tuple[any, any]:
        datasets, info = tfds.load(name='beans', with_info=True, as_supervised=True)
        num_classes = info.features['label'].num_classes

        def preprocess_fn(image, label):
            # Normalize [0, 255] to [0, 1]
            image = tf.case(image, tf.float32) / 255.
            # Resize the images to 224 x 224
            image = tf.image.resize(image, (224, 224))
            return image, label

        return datasets['train'].map(preprocess_fn).batch(batch_num), \
               datasets['train'].map(preprocess_fn).batch(batch_num), num_classes


class PGLearningRandomForestExt(PGLearningTFHub):
    def __init__(self, project_name: str = "learninglrmext", logging_enable: str = False):
        super(PGLearningRandomForestExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGLearningLRSingleton(PGLearningTFHub):
    __instance = None

    @staticmethod
    def get_instance():
        if PGLearningLRSingleton.__instance == None:
            PGLearningLRSingleton()
        else:
            return PGLearningLRSingleton.__instance

    def __init__(self, project_name: str = "learninglstmsingleton", logging_enable: str = False):
        super(PGLearningLRSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGLearningLRSingleton.__instance = self


if __name__ == '__main__':

    test = PGLearningTFHub()
    test.set_profile("cloud_storage")
    train_dataset, eval_dataset, num_classes = training_dataset_classification = test.data_gen_dataset(10)
    test.get_tasks(train_dataset, {'name': "city20", 'validation': eval_dataset, 'num_classes': num_classes})
    test.process()









