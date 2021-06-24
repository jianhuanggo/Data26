import os
import sys
import inspect
import pandas as pd
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List, Union
from Learning import pglearningcommon1, pglearningbase, pglearning
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
from Data.Storage import pgstorage
from Data.Utils import pgfile, pgdirectory
import gym
import random
import numpy as np
from subprocess import Popen
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Flatten
from tensorflow.keras.optimizers import Adam
from rl.agents import DQNAgent
from rl.policy import BoltzmannQPolicy
from rl.memory import SequentialMemory
from stable_baselines3 import PPO, A2C, DDPG, DQN, SAC, TD3
from stable_baselines3.common.vec_env import VecFrameStack, DummyVecEnv
from stable_baselines3.common.evaluation import evaluate_policy
from Data.Utils import pgfile
from stable_baselines3.common.callbacks import StopTrainingOnRewardThreshold, EvalCallback
from stable_baselines3.common.env_checker import check_env
from Learning.Reinforcement.Environment import pgshowerenv, pgstocktradingenv
from stable_baselines3.common.monitor import Monitor

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

__version__ = "1.7"


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



        #print(Flatten(input_shape=(1, self._states)))

        #exit(0)


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

    def data_preprocessing(self, pgdataset: Union[pd.DataFrame, np.ndarray] = None, pgscaler = None, parameter: dict = None) -> Union[np.ndarray, None]:
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



            policy = BoltzmannQPolicy()
            memory = SequentialMemory(limit=50000, window_length=1)
            self._agent = DQNAgent(model=self._model, memory=memory, policy=policy,
                           nb_actions=self._actions, nb_steps_warmup=100, target_model_update=1e-2)
            #return dqn
            #return model
            self._agent.compile(Adam(lr=1e-3), metrics=['mae'])
            self._agent.fit(self._env, nb_steps=50000, visualize=False, verbose=1)
            print("okokok")

            self.model_save(self._agent, "CartPole")

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def model_test(self, agent, env, episodes: int = 100, parameter: dict = None) -> Union[float, None]:

        ### Testing agent
        try:
            scores = agent.test(env, nb_episodes=100, visualize=False)
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

    def _reset(self, env_name):
        del self._model
        del self._agent
        del self._env
        self._env = gym.make(env_name)
        self._states = self._env.observation_space.shape[0]
        self._actions = self._env.action_space.n

    def _process(self, pgdataset = None, parameters: dict = {}, *args, **kwargs) -> Union[float, int, None]:
        try:

            _load_model_config = self.model_load(parameters['name'], {**self._parameter, **parameters})
            self._reset('CartPole-v0')

            _model_conf = _load_model_config['entity'] if _load_model_config and "entity" in _load_model_config else None
            if _model_conf:
                self.model_load(_load_model_config['entity'])
            else:
                self.model_train()

            print(self.model_test(self._model, self._env))

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


class PGLearningReinforcementExt(PGLearningReinforcement):
    def __init__(self, project_name: str = "learningmeansmext", logging_enable: str = False):
        super(PGLearningReinforcementExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

        ### Specific Variables

        self._model_subtype = {'A2C': A2C,
                               'PPO': PPO,
                               'DDPG': DDPG,
                               'DQN': DQN,
                               'TD3': TD3,
                               'SAC': SAC,
                               'custom': PPO
                                }
        os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'

        self._states = self._env.observation_space.shape[0]
        self._actions = self._env.action_space.n
        self._custom_env = {'shower': pgshowerenv.PGShowerEnv,
                            'stocktrading': pgstocktradingenv.PGStockTradingEnv
                            }
        self._custom_env_param = {'shower': {},
                                  'stocktrading': {}
                                  }

        #print(Flatten(input_shape=(1, self._states)))

    def _reset(self, env_name):
        if self._custom_env.get(env_name, None):
        #if env_name == "custom":
            self._env.reset()
        else:
            del self._model
            del self._agent
            del self._env
            self._model = None
            self._agent = None
            self.create_env(env_name, 1, 4)
            self._states = self._env.observation_space.shape[0]
            self._actions = self._env.action_space.n

    def set_custom_env_param(self, entity_name, entity_param):
        try:
            self._custom_env_param[entity_name] = [entity_param]
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            exit(1)

    def create_env(self, env_type: str, num_env: int, num_stacks: int = None, custom_env=None, parameters: dict = {}) -> bool:
        try:
            _env = self._custom_env.get(env_type, None)

            print(_env)
            if _env:
                #print(self._custom_env_param[env_type][0])

                self._env = _env(*self._custom_env_param[env_type]) if self._custom_env_param[env_type] else _env()

                check_env(self._env)

            elif num_env == 1:
                self._env = DummyVecEnv([lambda: gym.make(env_type)])
            else:
                self._env = VecFrameStack(custom_env(env_type, n_envs=num_env, seed=0), num_stacks)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            exit(1)

    def model_test(self, model, env, episodes: int = 100, parameter: dict = None) -> Union[float, None]:
        return evaluate_policy(model, Monitor(env), n_eval_episodes=episodes, render=False)

    def model_train(self, model=None, data_scaler=None, parameters: dict = None):

        try:

            self._model = model or self._model or self._model_subtype.get(parameters['model_name'])('MlpPolicy', self._env, verbose=1, tensorboard_log=os.path.join('Training', 'Logs'))

            self._model.learn(total_timesteps=10000, callback=EvalCallback(self._env,
                                                                           callback_on_new_best=StopTrainingOnRewardThreshold(reward_threshold=200, verbose=1),
                                                                           eval_freq=10000,
                                                                           best_model_save_path=os.path.join('Training', 'Save_models', parameters['entity_name'], parameters['model_name']),
                                                                           verbose=1))



            self.model_save(self._model, parameters['entity_name'], parameters)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            exit(1)

    def define_model(self, parameters: dict = {}):
        try:
            _policy = {'net_arch': [dict(pi=[128, 128, 128, 128], vf=[128, 128, 128, 128])]} if parameters['model_name'] == "custom" else {}
            print(f"{self._env}")

            return self._model_subtype.get(parameters['model_name'])('MlpPolicy',
                                                                 self._env,
                                                                 verbose=1,
                                                                 tensorboard_log=os.path.join('Training', 'Logs', parameters['entity_name']),
                                                                 policy_kwargs=_policy)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def model_save(self, model, entity_name: str, parameters: dict = {}) -> bool:
        try:
            return model.save(os.path.join('Training', 'Save_models', entity_name, parameters['model_name'], entity_name))

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def model_load(self, entity_name: str, parameters: dict = None):
        try:
            print("loading model...")

            self._reset(parameters['entity_name'])
            self.create_env(parameters['entity_name'], 1)
            _model_to_load = os.path.join('Training', 'Save_models', entity_name, parameters['model_name'], "best_model.zip") if pgfile.isfileexist(os.path.join('Training', 'Save_models', entity_name, parameters['model_name'], "best_model.zip")) else os.path.join('Training', 'Save_models', entity_name, parameters['model_name'], entity_name)
            print(_model_to_load)
            self._model = self._model_subtype.get(parameters['model_name']).load(_model_to_load, self._env)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def get_log(self, path: str):
        sts = Popen("tensorboard" + f" --logdir=\"{path}\"", shell=True).wait()

    def check_env(self, env):
        print(check_env(env))

    def _process(self, pgdataset = None, parameters: dict = {}, *args, **kwargs) -> Union[float, int, None]:
        try:

            _load_model_config = {**self._parameter, **parameters}
            print(_load_model_config)


            self._reset(parameters['entity_name'])
            #self.create_env("CartPole-v0", 1)


            self.create_env(parameters['entity_name'], 1)

            if _load_model_config['entity_name'] and _load_model_config['model_name']:
                self.model_load(_load_model_config['entity_name'], _load_model_config)


            if not self._model:
                ### pass in custom model, default to PPO (MlpPolicy)
                self.model_train(model=self.define_model(_load_model_config), parameters=_load_model_config)

            print(self.model_test(self._model, self._env))

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

        return None


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
    test = PGLearningReinforcementExt()
    test.set_profile("cloud_storage")
    test.get_log("/Users/jianhuang/opt/anaconda3/envs/pg_learning/Learning/Reinforcement/Example/Training/Logs/PPO_4")
    exit(0)



    #plot_pixels(china, title='Input color space: 16 million possible colors')
    #ax = plt.axes(xticks=[], yticks=[])
    #ax.imshow(china)
    #plt.show()
    test._process("", {'name': "test1"})
    exit(0)


    # print(test.parameter)
    test.get_tasks('train10.csv', {'name': "city7", 'prediction': pd.read_csv('prediction10.csv')})
    # print(test._data_inputs)
    test.process()
    print(test.data)








