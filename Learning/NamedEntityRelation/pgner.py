import os
import sys
import inspect
import pandas as pd
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List, Union
from Learning import pglearningcommon1, pglearningbase, pglearning
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
from Data.Storage import pgstorage
from itertools import repeat
import gym
import spacy
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
from Data.Utils import pgfile, pgdirectory, pgyaml

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

__version__ = "1.7"


class PGLearningNER(pglearningbase.PGLearningBase, pglearningcommon1.PGLearningCommon):
    def __init__(self, project_name: str = "learningner", logging_enable: str = False):
        super(PGLearningNER, self).__init__(project_name=project_name,
                                            object_short_name="PG_LRN_NER",
                                            config_file_pathname=__file__.split('.')[0] + ".ini",
                                            logging_enable=logging_enable,
                                            config_file_type="ini")

        ### Common Variables
        self._name = "learningner"
        self._learning_type = "supervised"
        self._model_type = "supervised"
        self._model = spacy.load("en_core_web_trf")
        self._agent = None
        self._min_record_cnt_4_pred = 1
        self._data = pd.DataFrame()
        self._data1 = {}
        self._pattern_match = {}
        self._parameter = {'storage_type': 'localdisk',  # default storage to save and load models
                           'storage_name': 'lr',  # name of storage
                           'dirpath': pgdirectory.get_filename_from_dirpath(__file__) + "/model",
                           'test_size': 0.2}  # if test_flag set to true, percentage of data will be used as testing dataset


        self._column_heading = None
        ### Specific Variables
        self._labels = ['PERSON', 'NORP', 'FAC', 'ORG', 'GPE', 'LOC', 'PRODUCT', 'EVENT', 'WORK_OF_ART', 'LAW', 'LANGUAGE', 'DATE', 'TIME', 'PERCENT', 'MONEY', 'QUANTITY', 'ORDINAL', 'CARDINAL']
        self._public_labels = ['PERSON', 'ORG', 'GPE', 'LOC', 'LAW', 'MONEY']
        self._test_labels = ['ORG']

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

    def get_tasks_v2(self, pg_data: Union[str, list], pg_parameters: dict = None) -> bool:

        """Prepares inputs into a standard format
        [(name(string), parameters(dict), data(varies), (name1(string), parameters1(dict), data1(varies), ...]

        Three type of inputs:
        1) if dataset is a string, then expects it to be a filename in yaml format
        2) if dataset is a list with no parameter, then expects to be the final format above
        3) if dataset is a list with parameter, then it will convert it to be the final format above

        Args:
            pg_data: data
            pg_parameters: parameters

        Returns:

        """

        try:

            if isinstance(pg_data, str):
                _file_ext = pg_data.split('.')[-1]

                if _file_ext in ("yml", "yaml"):
                    self._data_inputs = [(key, item) for key, item in pgyaml.yaml_load(yaml_filename=pg_data).items()]
                elif _file_ext in (".csv"):
                    _df = pd.read_csv(pg_data, sep=pg_parameters["separator"])
                    self._column_heading = _df.columns.values.tolist()
                    self._parameter = {**self._parameter, **pg_parameters}
                    #print(f"column heading: {type(self._column_heading)}")

                    #pd.read_csv(pg_data, sep="|").values.tolist())
                    #self._data_inputs = [x for x in zip(repeat(pg_parameters['name']), repeat(pg_parameters), _df.values.tolist())] if pg_parameters else _df.values.tolist()
                    self._data_inputs = [x for x in zip(repeat(pg_parameters['name']), _df.values.tolist())] if pg_parameters else _df.values.tolist()
            elif isinstance(pg_data, list) and pg_parameters:
                self._data_inputs = list(zip(pg_data, pg_parameters))
            else:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "pg_data needs to be a list or str")
                return False
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

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

    def _process(self, pgdataset: pd.DataFrame, parameters: dict = {}, *args, **kwargs) -> Union[float, int, None]:
        def _get_entity(data: str, *args, **kwarg):
            doc = self._model(data)
            org_list =[]
            # loop through the identified entities and append ORG entities to org_list

            for entity in doc.ents:
                #if entity.label_ == entity_type:
                if entity.label_ in args:
                    org_list.append(entity.text)

            # if organization is identified more than once it will appear multiple times in list
            # we use set() to remove duplicates then convert back to list
            org_list = list(set(org_list))
            return org_list

        try:
            # process the text with our SpaCy model to get named entities
            #for item in self._public_labels:
            for item in self._test_labels:
                #pgdataset[item] = pgdataset['selftext'].apply(_get_entity)
                pgdataset[item] = pgdataset['selftext'].apply(_get_entity, args=(item,))
                
            self._data = pd.concat([self._data, pgdataset], ignore_index=True)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def process_v2(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        def _get_entity(data: str, *args, **kwarg):
            doc = self._model(data)
            org_list = []
            # loop through the identified entities and append ORG entities to org_list

            for entity in doc.ents:
                # if entity.label_ == entity_type:
                if entity.label_ in args:
                    org_list.append(entity.text)

            # if organization is identified more than once it will appear multiple times in list
            # we use set() to remove duplicates then convert back to list
            org_list = list(set(org_list))
            return org_list

        try:
            print(f"here is the number of records: {pg_data}")
            print(f"here is the number of records in df: {pd.DataFrame(pg_data).transpose()}")
            exit(0)
            # process the text with our SpaCy model to get named entities
            # for item in self._public_labels:
            for item in self._test_labels:
                # pgdataset[item] = pgdataset['selftext'].apply(_get_entity)
                pg_data[item] = pg_data['selftext'].apply(_get_entity, args=(item,))

            self._data = pd.concat([self._data, pg_data], ignore_index=True)

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


class PGLearningNERExt(PGLearningNER):
    def __init__(self, project_name: str = "learningnerext", logging_enable: str = False):
        super(PGLearningNERExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

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


class PGLearningNERSingleton(PGLearningNER):
    __instance = None

    @staticmethod
    def get_instance():
        if PGLearningNERSingleton.__instance == None:
            PGLearningNERSingleton()
        else:
            return PGLearningNERSingleton.__instance

    def __init__(self, project_name: str = "learninglstmsingleton", logging_enable: str = False):
        super(PGLearningNERSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGLearningNERSingleton.__instance = self


if __name__ == '__main__':
    test = PGLearningNERExt()
    test.set_profile("cloud_storage")
    filepath = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/SocialMedia/Example/investing_new_09f8c71d_.csv"
    example = pd.read_csv(filepath, sep="|")

    print(example)

    exit(0)

    test._process(example)
    #test.get_log("/Users/jianhuang/opt/anaconda3/envs/pg_learning/Learning/Reinforcement/Example/Training/Logs/PPO_4")
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








