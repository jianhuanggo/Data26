
from scipy.stats.stats import pearsonr
from pprint import pprint
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from Learning import pglearningcommon2, pglearningbase, pglearning
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
from Data.Storage import pgstorage
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from tensorflow import keras
from tensorflow.keras.layers import Dense, Dropout, LSTM
from tensorflow.keras.models import Sequential, load_model, save_model
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, cohen_kappa_score, roc_auc_score, confusion_matrix
from sklearn.metrics import mean_squared_error
from typing import List
from typing import Generic, TypeVar, Optional, List
from pydantic import BaseModel, validator, ValidationError
from pydantic.generics import GenericModel
from Data.Utils import pgfile, pgdirectory
import warnings

import h5py
import math
import hashlib
import inspect
import json
import datetime as dt
import numpy as np
import pandas as pd
import pandas_datareader as web
import tensorflow as tf
import tensorflow_addons as tfa
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


class PGLearningLSTM(pglearningbase.PGLearningBase, pglearningcommon2.PGLearningCommon):
    def __init__(self, project_name: str = "learninglstm", logging_enable: str = False):
        super(PGLearningLSTM, self).__init__(project_name=project_name,
                                             object_short_name="PG_LRN_LSTM",
                                             config_file_pathname=__file__.split('.')[0] + ".ini",
                                             logging_enable=logging_enable,
                                             config_file_type="ini")

        ### Common Variables
        self._name = "learninglstm"
        self._data = {}
        self._pattern_match = {}
        self._model_distribution_strategy_map = {'mirroredstrategy': tf.distribute.MirroredStrategy(),
                                                 #'collectivecommunication': CollectiveCommunication(),
                                                 'centralStoragestrategy': tf.distribute.experimental.CentralStorageStrategy()
                                                 }
        self._model_distribution_strategy = "mirroredstrategy"
        self._parameter = {'num_consecutive_inputs': 60,  # consecutive data points for training
                           'position_offset_for_output': 0,  # predict number of data point(s) ahead
                           'storage_type': 'localdisk',  # default storage to save and load models
                           'storage_name': 'lstm', # name of storage
                           'default_dirpath': pgdirectory.get_filename_from_dirpath(__file__) + "/model",
                           'intermediate_dirpath': pgdirectory.get_filename_from_dirpath(__file__) + "/model_intermediate",
                           'batch_size': 32,
                           'test_flag': True,  # if False, all input data will be treated as training dataset if model is not train and this flag is not used if model is already exist
                           'test_size': 0.2}   # if test_flag set to true, percentage of data will be used as testing dataset


        ### Specific Variables
        if not pgdirectory.createdirectory(self._parameter['default_dirpath']):
            exit(1)
        """
        _date_inputs = {'BTC-USD': {'parameters': dict,
                                    'data': list
                                    }
                        }
        
        
        """
        self._model_distribution_strategy = "centralStoragestrategy"
        self._data_inputs = {}
        #self.get_config(profile="default")

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    @property
    def process(self) -> Callable:
        return self._process

    @pgoperation.pg_retry(3)
    def model_save(self, model, entity_name: str, parameter: dict) -> bool:
        try:
            _save_model = pgstorage.pgstorage(object_type=self._parameter['storage_type'],
                                              object_name=self._parameter['storage_name'])(self._model_save)

            return _save_model(model=model, entity_name=entity_name, parameter=parameter)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    #@pgoperation.pg_retry(3)
    def model_load(self, entity_name: str, parameter: dict):
        try:
            _load_model = pgstorage.pgstorage(object_type=self._parameter['storage_type'],
                                              object_name=self._parameter['storage_name'])(self._model_load)
            print(self._parameter['storage_name'])
            print(self._parameter['storage_type'])

            return _load_model(entity_name=f"{entity_name}", parameter=parameter)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def model_train(self, entity_name: str,
                    premise_dataset: list,
                    premise_dataset_scaler,
                    result_dataset: list,
                    result_dataset_scaler,
                    parameter: dict,
                    batch_size: int = 32):

        _z_score = _a_score = _f_score = _correlation_score = -1

        try:
            if len(premise_dataset) < int(parameter['num_consecutive_inputs']) * 3:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "not enough data to train")
                return None

            if self._parameter['test_flag']:
                #_training_data = premise_dataset[:len(premise_dataset) * self._parameter['training_dataset_ratio'] // 100]
                #_result_dataset = result_dataset[:len(premise_dataset) * self._parameter['training_dataset_ratio'] // 100]
                _training_data, _testing_data, _result_dataset, _testing_result = train_test_split(premise_dataset,
                                                                                                   result_dataset,
                                                                                                   test_size=
                                                                                                   self._parameter[
                                                                                                           'test_size'],
                                                                                                   shuffle=False)
            else:
                _training_data = premise_dataset
                _result_dataset = result_dataset

            #_training_data1, _testing_data1, _result_dataset1, _testing_result1 = train_test_split(premise_dataset, result_dataset, test_size=self._parameter['test_size'], shuffle=False)
            #print(set(_training_data[:, 0]) ^ set(_training_data1[:, 0]))



            x_train, y_train = [], []

            for x in range(parameter['num_consecutive_inputs'], len(_training_data) - parameter['position_offset_for_output']):
                x_train.append(_training_data[x - parameter['num_consecutive_inputs']: x, 0])
                y_train.append(_result_dataset[x + parameter['position_offset_for_output'], 0])

            x_train, y_train = np.array(x_train), np.array(y_train)

            # formatting to 3 dimension array
            x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))
            _distribution = self._model_distribution_strategy_map.get(self._model_distribution_strategy)
            print(f"distribution: {_distribution}")
            _callback = tf.keras.callbacks.EarlyStopping(monitor='loss', min_delta=0, patience=3)

            with _distribution.scope():
                model = Sequential()
                model.add(LSTM(units=50, return_sequences=True, input_shape=(x_train.shape[1], 1)))
                model.add(Dropout(0.2))
                model.add(LSTM(units=50, return_sequences=True))
                model.add(Dropout(0.2))
                model.add(LSTM(units=50))
                model.add(Dropout(0.2))
                model.add(Dense(units=1))
                model.compile(optimizer='adam', loss="mean_squared_error", )

                model.fit(x_train, y_train, epochs=1, batch_size=batch_size, callbacks=[_callback])

            if self._parameter['test_flag']:
                #_testing_data = premise_dataset[len(premise_dataset) * self._parameter['training_dataset_ratio'] // 100 - parameter['num_consecutive_inputs']:]
                #_testing_result = result_dataset[len(premise_dataset) * self._parameter['training_dataset_ratio'] // 100 - parameter['num_consecutive_inputs']:]
                _z_score, _a_score, _f_score, _correlation_score = self.model_test(model=model,
                                                                                   test_dataset=_testing_data,
                                                                                   test_dataset_scaler=premise_dataset_scaler,
                                                                                   result_dataset=_testing_result,
                                                                                   result_dataset_scaler=result_dataset_scaler,
                                                                                   parameter=parameter)

            self.model_save(model, entity_name,  {**self._parameter, **{'z_score': _z_score, 'a_score': _a_score, 'f_score': _f_score, 'correlation_score':  _correlation_score}})

            print(model)

            return model

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def model_test(self, model, test_dataset: list, test_dataset_scaler, result_dataset: list, result_dataset_scaler, parameter: dict):
        try:
            x_test = []

            for x in range(parameter['num_consecutive_inputs'], len(test_dataset)):
                x_test.append(test_dataset[x - parameter['num_consecutive_inputs']: x, 0])

            x_test = np.array(x_test)
            x_test = np.reshape(x_test, (x_test.shape[0], x_test.shape[1], 1))

            _actual = result_dataset_scaler.inverse_transform(result_dataset)
            _prediction = result_dataset_scaler.inverse_transform(model.predict(x_test))

            _actual_results = _actual[parameter['num_consecutive_inputs']:].ravel().tolist()
            _predicted_results = _prediction.ravel().tolist()

            _correlation, _ = pearsonr(_actual_results, _predicted_results)
            print(self.pg_model_a_score(_predicted_results, _actual_results),
                  self.pg_model_f_score(_predicted_results, _actual_results),
                  _correlation)
            print(self.pg_model_z_score(_predicted_results, _actual_results))

            return self.pg_model_z_score(_predicted_results, _actual_results), \
                   self.pg_model_a_score(_predicted_results, _actual_results), \
                   self.pg_model_f_score(_predicted_results, _actual_results), _correlation


            """
            
            metric = tfa.metrics.F1Score(num_classes=1, threshold=0.5)

            metric.update_state(_actual[parameter['num_consecutive_inputs']:], _prediction)
            result = metric.result()
            print(result.numpy())
            
            # accuracy: (tp + tn) / (p + n)
            #accuracy = accuracy_score(__prediction, __actual)
            #print('Accuracy: %f' % accuracy)

            # precision tp / (tp + fp)
            precision = precision_score(__prediction, __actual)
            print('Precision: %f' % precision)

            
            # recall: tp / (tp + fn)
            recall = recall_score(_predicted_results[0], _actual_results[0])
            print('Recall: %f' % recall)
            # f1: 2 tp / (2 tp + fp + fn)
            f1 = f1_score(_predicted_results[0], _actual_results[0])
            print('F1 score: %f' % f1)
            
            # kappa
            kappa = cohen_kappa_score(testy, yhat_classes)
            print('Cohens kappa: %f' % kappa)
            # ROC AUC
            auc = roc_auc_score(testy, yhat_probs)
            print('ROC AUC: %f' % auc)
            # confusion matrix
            matrix = confusion_matrix(testy, yhat_classes)
            print(matrix)
            
            """

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def get_model_filename(self, parameters: dict) -> str:
        try:
            _filename = ""
            for _key, _val in parameters.items():
                _filename += f"{str(_key)}-{str(_val)}_"
            return '_'.join(_filename.split('_')[:-1])

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return ""

    ### provide entity_name, dataset itself and parameters associated with the dataset
    #@pgoperation.pg_retry(3)
    def get_tasks(self, dataset: dict, parameter: dict = {}) -> list:
        try:
            _num_consecutive_inputs = parameter['num_consecutive_inputs'] if "num_consecutive_inputs" in parameter else self._parameter['num_consecutive_inputs']
            _position_offset_for_output = parameter['position_offset_for_output'] if "position_offset_for_output" in parameter else self._parameter['position_offset_for_output']

            if not dataset:
                print("nothing to process")
                return []
            for _entity_name, _data in dataset.items():
                if _entity_name not in self._data_inputs:
                    self._data_inputs[_entity_name] = {'parameter': {'entity_name': f"{_entity_name}_{_data['premise_dataset_label']}_2_{_data['result_dataset_label']}",
                                                                     'num_consecutive_inputs': _num_consecutive_inputs,
                                                                     'position_offset_for_output': _position_offset_for_output,
                                                                     'batch_size': 32
                                                                     },
                                                        'premise_dataset': _data['premise_dataset'],
                                                        'result_dataset': _data['result_dataset']
                                                        }

            return list(self._data_inputs.keys())
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return []

    def _process(self, premise_dataset: list, result_dataset: list, parameter: dict, *args, **kwargs) -> Union[float, int, None]:
        try:
            if len(premise_dataset) != len(premise_dataset):
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "warning!! the size of premise and result is not the same")

            _premise_dataset_scaler = MinMaxScaler(feature_range=(0, 1))
            _result_dataset_scaler = MinMaxScaler(feature_range=(0, 1))

            # (-1, 1) setting to dimension/column to 1 and calculate number of rows
            _scaled_premise_dataset = _premise_dataset_scaler.fit_transform(np.array(premise_dataset).reshape(-1, 1))
            _scaled_result_dataset = _result_dataset_scaler.fit_transform(np.array(result_dataset).reshape(-1, 1))

            #print(_scaled_premise_dataset)
            #print(_scaled_result_dataset)

            _entity_name = self.get_model_filename(parameter)
            _curr_model = self.model_load(_entity_name, self._parameter)

            print(_curr_model)

            if _curr_model:
                print(f"{_entity_name} model is successfully loaded")
                _x_predict = []
                for x in range(parameter['num_consecutive_inputs'], len(_scaled_premise_dataset)):
                    _x_predict.append(_scaled_premise_dataset[x - parameter['num_consecutive_inputs']: x, 0])

                _x_predict = np.array(_x_predict)
                _x_predict = np.reshape(_x_predict, (_x_predict.shape[0], _x_predict.shape[1], 1))

                return _result_dataset_scaler.inverse_transform(_curr_model.predict(_x_predict))

            else:
                print(f"{_entity_name} model not found, start training a model for the dataset")
                _curr_model = self.model_train(entity_name=f"{_entity_name}",
                                               premise_dataset=_scaled_premise_dataset,
                                               premise_dataset_scaler=_premise_dataset_scaler,
                                               result_dataset=_scaled_result_dataset,
                                               result_dataset_scaler=_result_dataset_scaler,
                                               parameter=parameter)

                _x_predict = []
                _x_predict.append(_scaled_premise_dataset[len(_scaled_premise_dataset) - parameter['num_consecutive_inputs']:, 0])
                _x_predict = np.array(_x_predict)

                _x_predict = np.reshape(_x_predict, (_x_predict.shape[0], _x_predict.shape[1], 1))
                return _result_dataset_scaler.inverse_transform(_curr_model.predict(_x_predict))

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    # invoke self._get_func_parameter to get a list of items to be processed
    def process(self, item: str, *args: object, **kwargs: object) -> bool:
        try:
            _sub_item = self._data_inputs[item]
            print(_sub_item.keys())
            return self._process(_sub_item['premise_dataset'], _sub_item['result_dataset'], _sub_item['parameter'])

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    """
    # invoke self._get_func_parameter to get a list of items to be processed
    def process(self, item: str, *args: object, **kwargs: object) -> bool:
        try:
            _scaler = MinMaxScaler(feature_range=(0, 1))
            _sub_item = self._data_inputs[item]
            print(_sub_item.keys())

            # (-1, 1) setting to dimension/column to 1 and calculate number of rows
            _scaled_data = _scaler.fit_transform(np.array(_sub_item['data']).reshape(-1, 1))

            print(_scaled_data.shape)

            _entity_name = self.get_model_filename(_sub_item['parameters'])

            _curr_model, _parameter = self.model_load(_entity_name)
            print(_curr_model, _parameter)

            if _curr_model:
                print(f"{_entity_name} model is successfully loaded")

                _x_predict = []

                for x in range(_sub_item['parameters']['num_consecutive_inputs'], len(_scaled_data)):
                    _x_predict.append(_scaled_data[x - _sub_item['parameters']['num_consecutive_inputs']: x, 0])

                _x_predict = np.array(_x_predict)
                _x_predict = np.reshape(_x_predict, (_x_predict.shape[0], _x_predict.shape[1], 1))


                _predict = _curr_model.predict(_x_predict)
                print(_predict)
                exit(0)
                _predict = _scaler.inverse_transform(_predict)

                print(_predict)

            else:
                print(f"{_entity_name} model not found, start training a model for the dataset")
                _curr_model = self.model_train(entity_name=f"{_entity_name}", scaler=_scaler, data=_scaled_data, parameter=_sub_item['parameters'])

                _x_predict = []
                _x_predict.append(_scaled_data[len(_scaled_data) - _sub_item['parameters']['num_consecutive_inputs']: len(_scaled_data), 0])
                _x_predict = np.array(_x_predict)

                _x_predict = np.reshape(_x_predict, (_x_predict.shape[0], _x_predict.shape[1], 1))

                _predict = _curr_model.predict(_x_predict)
                _predict = _scaler.inverse_transform(_predict)
                print(_predict)

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False
    """

    def pg_model_f_score(self, predicted, actual) -> Union[float, str]:
        if isinstance(predicted, (list, tuple, set,)) and isinstance(predicted, (list, tuple, set)):
            if len(predicted) != len(actual):
                return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "the size of predicted does not match with size of actual")
            _tp, _fn, _fp = 0, 0, 0

            try:
                for i in range(len(predicted) - 1):
                    _actual_diff = actual[i + 1] - actual[i]
                    _predicted_diff = predicted[i + 1] - predicted[i]

                    if _actual_diff * _predicted_diff > 0:
                        _tp += 1
                    else:
                        if _predicted_diff < 0:
                            _fn += 1
                        else:
                            _fp += 1

                _precision = float(_tp) / float(_tp + _fp)
                _recall = float(_tp) / float(_tp + _fn)
                return 2.0 * _precision * _recall / (_precision + _recall)
            except Exception as err:
                return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

        else:
            return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                 "this calculation needs multiple numbers as input")

    def pg_model_a_score(self, predicted, actual) -> Union[float, str]:
        if isinstance(predicted, (list, tuple, set,)) and isinstance(predicted, (list, tuple, set)):
            if len(predicted) != len(actual):
                return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "the size of predicted does not match with size of actual")
            _tp, _fn, _fp = 0, 0, 0

            try:
                for i in range(len(predicted)):
                    _diff = predicted[i] / actual[i]

                    if 0.95 < _diff < 1.05:
                        _tp += 1
                    else:
                        if _diff < 0.95:
                            _fn += 1
                        else:
                            _fp += 1

                _precision = float(_tp) / float(_tp + _fp)
                _recall = float(_tp) / float(_tp + _fn)
                return 2.0 * _precision * _recall / (_precision + _recall) if (_precision + _recall) != 0 else 0
            except Exception as err:
                return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

        else:
            return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                 "this calculation needs multiple numbers as input")

    def pg_model_z_score(self, predicted, actual) -> Tuple[float, float]:
        if isinstance(predicted, (list, tuple, set,)) and isinstance(predicted, (list, tuple, set)):
            if len(predicted) != len(actual):
                return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "the size of predicted does not match with size of actual")
            _up, _down = 0, 0

            try:
                for i in range(len(predicted)):
                    if predicted[i] / actual[i] < 0.95 :
                        _down += 1
                    elif predicted[i] / actual[i] > 1.05:
                        _up += 1
                return f"{_up / len(predicted)}+{_down / len(predicted)}"
            except Exception as err:
                return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

        else:
            return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                 "this calculation needs multiple numbers as input")


class PGLearningLSTMExt(PGLearningLSTM):
    def __init__(self, project_name: str = "learninglstmext", logging_enable: str = False):
        super(PGLearningLSTMExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGLearningLSTMSingleton(PGLearningLSTM):

    __instance = None

    @staticmethod
    def get_instance():
        if PGLearningLSTMSingleton.__instance == None:
            PGLearningLSTMSingleton()
        else:
            return PGLearningLSTMSingleton.__instance

    def __init__(self, project_name: str = "learninglstmsingleton", logging_enable: str = False):
        super(PGLearningLSTMSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGLearningLSTMSingleton.__instance = self


def load():
    with open('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Learning/LSTM/Test/saved_symbol1.txt', 'r') as file:
        _saved_data = Data(**json.loads(file.readlines()[0]))
    print(pd.read_json(_saved_data.data_frame, orient='records'))

    exit(0)
    _saved_data.data_frame = pd.read_json(_saved_data.data_frame, orient='records')
    print(_saved_data.data_frame)
        #_data = pd.read_json(Data(**json.loads(file.readlines()[0])).data_frame, orient='records')
    _saved_data.data_frame.set_index('Date', inplace=True)
    return _saved_data


def load1():
    with open('/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/Learning/LSTM/Test/saved_symbol1.txt', 'r') as file:
        _saved_data = Data(**json.loads(file.read()))
        _saved_data.data_frame = pd.read_json(_saved_data.data_frame, orient='records')
        _saved_data.data_frame.set_index('Date', inplace=True)
    return _saved_data

        #_data = pd.read_json(Data(**json.loads(file.read())).data_frame, orient='records')
        #_data.set_index('Date', inplace=True)
        #return {Data(**json.loads(file.read())).symbol: _data}


if __name__ == '__main__':
    test = PGLearningLSTM()
    #test.set_profile("default")
    test.set_profile("cloud_storage")
    print(test.parameter)
    _data = load1()
    #print(_data.data_frame)
    #'premise_dataset': _data.data_frame['Volume'],
    """
    data = {_data.symbol: {'premise_dataset_label': "price",
                           'result_dataset_label': "price",
                           'premise_dataset': _data.data_frame['Close'],
                           'result_dataset': _data.data_frame['Close']
                          }
            }
    """

    data = {_data.symbol: {'premise_dataset_label': "volume",
                           'result_dataset_label': "price",
                           'premise_dataset': _data.data_frame['Volume'],
                           'result_dataset': _data.data_frame['Close']
                          }
            }
    print(test.process(test.get_tasks(data)[0]))








