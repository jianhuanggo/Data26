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
from pydantic.generics import GenericModel
from Learning import pglearningcommon1, pglearningbase, pglearning
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
from Data.Storage import pgstorage
from Data.Utils import pgfile, pgdirectory
from sklearn.cluster import MiniBatchKMeans

import gym
import random
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Flatten
from tensorflow.keras.optimizers import Adam
from rl.agents import DQNAgent
from rl.policy import BoltzmannQPolicy
from rl.memory import SequentialMemory
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


class PGLearningReinforcement(pglearningbase.PGLearningBase, pglearningcommon1.PGLearningCommon):
    def __init__(self, project_name: str = "learninglr", logging_enable: str = False):
        super(PGLearningReinforcement, self).__init__(project_name=project_name,
                                                      object_short_name="PG_LRN_LR",
                                                      config_file_pathname=__file__.split('.')[0] + ".ini",
                                                      logging_enable=logging_enable,
                                                      config_file_type="ini")

        ### Common Variables
        self._name = "learninglr"
        self._learning_type = "reinforcement"
        self._model_type = "reinforcement"
        self._model = None
        self._agent = None
        self._min_record_cnt_4_pred = 1
        self._data = {}
        self._pattern_match = {}
        self._parameter = {'storage_type': 'localdisk',  # default storage to save and load models
                           'storage_name': 'lr',  # name of storage
                           'dirpath': pgdirectory.get_filename_from_dirpath(__file__) + "/model",
                           'test_size': 0.2}  # if test_flag set to true, percentage of data will be used as testing dataset

        ### Specific Variables
        self._env = gym.make('CartPole-v0')
        self._states = self._env.observation_space.shape[0]
        self._actions = self._env.action_space.n



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

    def model_save(self, agent, entity_name: str, parameters: dict = {}) -> bool:
        """Returns True if weights are persisted

        Args:
            agent: agent instance
            entity_name: An unique Identifier used to form the model name
            parameters: parameters


        Returns:
            The return value. True for success, False otherwise.

            For linear regression model, we save following:
            1) weights - file 1
            2) weights - file 2

            1 - 4 are packaged into a tar file
            5) A signature file

        """
        try:

            return agent.save_weights(f"{entity_name}.h5f", overwrite=True)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    # @pgoperation.pg_retry(3)
    def model_load(self, entity_name: str, parameters: dict = None):
        try:
            return self._agent.load_weights(f"{entity_name}.h5f")
            """
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




            return _load_model(pgfiles=_pg_file)
            """
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def data_preprocessing(self, pgdataset: Union[pd.DataFrame, np.ndarray], pgscaler = None, parameter: dict = None) -> Union[np.ndarray, None]:
        try:
            pass

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def model_train(self, pgdataset: Union[list, np.ndarray] = None, data_scaler=None, parameter: dict = None):
        ### Training an agent
        try:
            self._model = None
            self._model = Sequential()
            self._model.add(Flatten(input_shape=(1, self._states)))
            self._model.add(Dense(24, activation='relu'))
            self._model.add(Dense(24, activation='relu'))
            self._model.add(Dense(self._actions, activation='linear'))

            self._model.summary()
            exit(0)


            policy = BoltzmannQPolicy()
            memory = SequentialMemory(limit=50000, window_length=1)
            self._agent = DQNAgent(model=self._model, memory=memory, policy=policy,
                           nb_actions=self._actions, nb_steps_warmup=10, target_model_update=1e-2)
            #return dqn
            #return model
            self._agent.compile(Adam(lr=1e-3), metrics=['mae'])
            self._agent.fit(self._env, nb_steps=50000, visualize=False, verbose=1)
            self.model_save(self._agent, "CartPole")

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def model_test(self, episodes: int = 100, parameter: dict = None) -> Union[float, None]:

        ### Testing agent
        try:
            scores = self._agent.test(self._env, nb_episodes=100, visualize=False)
            return np.mean(scores.history['episode_reward'])

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

    def _reset(self):
        del self._model
        del self._agent
        del self._env
        self._env = gym.make('CartPole-v0')
        self._states = self._env.observation_space.shape[0]
        self._actions = self._env.action_space.n

    def _process(self, pgdataset, parameters: dict, *args, **kwargs) -> Union[float, int, None]:
        try:

            _load_model_config = self.model_load(parameters['name'], {**self._parameter, **parameters})
            self._reset()
            _model_conf = _load_model_config['entity'] if _load_model_config and "entity" in _load_model_config else None
            if _model_conf:
                self.model_load(_load_model_config['entity'])

            else:
                self.model_train()

            print(self.model_test())

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

class PGLearningReinforcementExt(PGLearningReinforcement):
    def __init__(self, project_name: str = "learningmeansmext", logging_enable: str = False):
        super(PGLearningReinforcementExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGLearningKMeansSingleton(PGLearningReinforcement):
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
    test = PGLearningReinforcement()
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








