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
from pycaret.classification import *
from subprocess import Popen
from stable_baselines3 import PPO, A2C, DDPG, DQN, SAC, TD3
from Data.Utils import pgfile, pgdirectory, pgyaml
from sklearn.model_selection import train_test_split, cross_val_score
import keyboard

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

__version__ = "1.7"

# problem: Lightgbm OSError, Library not loaded
# solution for mac: brew install libomp
#
#
#


class PGLearningCaret(pglearningbase.PGLearningBase, pglearningcommon1.PGLearningCommon):
    def __init__(self, project_name: str = "learningcaret", logging_enable: str = False):
        super(PGLearningCaret, self).__init__(project_name=project_name,
                                              object_short_name="PG_LRN_CRT",
                                              config_file_pathname=__file__.split('.')[0] + ".ini",
                                              logging_enable=logging_enable,
                                              config_file_type="ini")

        ### Common Variables
        self._name = "learningcaret"
        self._learning_type = "supervised"
        self._model_type = "supervised"
        self._agent = None
        self._min_record_cnt_4_pred = 1
        self._data = {}
        self._pattern_match = {}
        self._parameter = {'storage_type': 'localdisk',  # default storage to save and load models
                           'storage_name': 'lr',  # name of storage
                           'dirpath': pgdirectory.get_filename_from_dirpath(__file__) + "/model",
                           'test_size': 0.2}  # if test_flag set to true, percentage of data will be used as testing dataset


        self._column_heading = None
        ### Specific Variables
        self._best_model = None
        self._max_client = 1
        if not pgdirectory.createdirectory(self._parameter['dirpath']):
            sys.exit(99)

        self._model_distribution_strategy = "centralStoragestrategy"
        self._data_inputs = {}
        # self.get_config(profile="default")
        self._X_train = self._X_test = self._y_train = self._y_test = None
        self.set_profile("default")
        self.clean_profile()

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    def model_save(self, entity_name: str, parameters: dict = {}) -> bool:
        """Returns True if weights are persisted

        Args:
            entity_name: An unique Identifier used to form the model name
            parameters: parameters

        Returns:
            The return value. True for success, False otherwise.

        """
        try:
            return save_model(self._best_model, model_name=entity_name)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    # @pgoperation.pg_retry(3)
    def model_load(self, entity_name: str, parameters: dict = None):
        try:
            return load_model(entity_name)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def preprocessing(self, pgdataset: Union[pd.DataFrame, np.ndarray] = None, pgscaler = None, parameter: dict = None) -> Union[list, None]:
        try:
            if len(pgdataset) < 50000:
                # print(pgdataset[col].unique())
                return list(set([col for col in pgdataset if len(pgdataset[col].unique()) < 7] + [col for col in pgdataset.columns if pgdataset.dtypes[col] not in ["int64", "float64"]]))
            else:
                #print(pgdataset[:50000].apply(lambda col: col.unique()))
                return list(set([col for col in pgdataset[:50000] if len(pgdataset[:50000][col].unique()) < 7] + [col for col in pgdataset.columns if pgdataset.dtypes[col] not in ["int64", "float64"]]))

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def model_train(self, pgdataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None, parameters: dict = None):
        try:
            #print(self.preprocessing(pgdataset.iloc[:, :-1]))
            #print(pgdataset.columns.tolist()[-1])
            experiment = setup(pgdataset,
                               silent=True,
                               target=pgdataset.columns.tolist()[-1],
                               categorical_features=self.preprocessing(pgdataset.iloc[:, :-1]))

            self._best_model = compare_models()

            print(self._best_model)
            self.model_save(parameters['entity_name'])
            #keyboard.press_and_release('enter')

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def model_test(self, pgdataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None, parameter: dict = None) -> Union[float, None]:

        ### Testing agent
        try:
            predict_model(self._best_model, pgdataset)
            return

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

    def get_tasks(self, pg_data: Union[str, list, pd.DataFrame], pg_parameters: dict = {}) -> bool:

        """Prepares inputs into a standard format
        [(name(string), data(varies), parameters(dict)), (name1(string), data1(varies), parameters1(dict)), ...]

        Three type of inputs:
        1) if dataset is a string, then expects it to be a filename in yaml or csv format
        2) if dataset is a list with no parameter, then expects to be the final format above
        3) if dataset is a list with parameter, then it will convert it to be the final format above

        Args:
            pg_data: data
            pg_parameters: parameters

        Returns:

        """
        try:
            if self._best_model:
                pass
            elif pgfile.isfileexist(os.path.join(self._parameter['save_dir'], f"{self._parameter['entity_name']}.pkl")):
                self._best_model = self.model_load(self._parameter['entity_name'])
                if self._best_model:
                    print(f"model {self._parameter['entity_name']} loaded")

            if isinstance(pg_data, str):
                _file_ext = pg_data.split('.')[-1]
                if _file_ext in ("yml", "yaml"):
                    self._data_inputs = [(key, item) for key, item in pgyaml.yaml_load(yaml_filename=pg_data).items()]
                elif _file_ext in (".csv"):
                    if "file_delimiter" in self._parameter:
                        _df = pd.read_csv(pg_data, sep=self._parameter["file_delimiter"])
                    else:
                        print(pg_data)
                        _df = pd.read_csv(pg_data, sep=',')

                    self._column_heading = _df.columns.values.tolist()
                    self._parameter = {**self._parameter, **pg_parameters}

                    if self._best_model:
                        #self._data_inputs = [(x[0], x[1][0].split(',')) for x in zip(repeat(self._parameter['entity_name']), _df.values.tolist())] if "entity_name" in self._parameter else [x[0].split(',') for x in _df.values.tolist()]
                        self._data_inputs = [x for x in zip(repeat(self._parameter['entity_name']), _df.values.tolist())] if "entity_name" in self._parameter else _df.values.tolist()
                    else:
                        if self._max_client % pg_parameters['num_client'] == pg_parameters['client_id']:
                            print(f"client_id: {pg_parameters['client_id']}")
                            self._X_train, self._X_test, self._y_train, self._y_test = train_test_split(_df.iloc[:, :-1], _df.iloc[:, -1], test_size=0.2,
                                shuffle=False)
                            self.model_train(pd.concat([self._X_train, self._y_train], axis=1), parameters=self._parameter)
                            pg_data = pd.concat([self._X_test, self._y_test], axis=1)
                            #self._data_inputs = [(x[0], x[1].split(',')) for x in zip(repeat(self._parameter['entity_name']),
                            #                                pg_data.values.tolist())] if "entity_name" in self._parameter else [x.split(',') for x in
                            #                                pg_data.values.tolist()]
                            self._data_inputs = [x for x in zip(repeat(self._parameter['entity_name']), pg_data.values.tolist())] if "entity_name" in self._parameter else pg_data.values.tolist()

            elif isinstance(pg_data, list) and pg_parameters:
                self._data_inputs = list(zip(pg_data, pg_parameters))
            else:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "pg_data needs to be a list or str")
                return False
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _process(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        try:
            #print(self._best_model)
            #print(pg_data)
            #print(pg_data_name)

            if self._best_model:
                pass
            elif pgfile.isfileexist(os.path.join(self._parameter['save_dir'], f"{self._parameter['entity_name']}.pkl")):
                self._best_model = self.model_load(self._parameter['entity_name'])
                print(f"model {self._parameter['entity_name']} loaded")
            #print(f"the table column {self._column_heading}")

            if isinstance(pg_data, pd.DataFrame):
                self._data[pg_data_name] = self._best_model.predict(pg_data.iloc[:, : -1])
            elif isinstance(pg_data, list):
                #print(pd.DataFrame([pg_data], columns=self._column_heading).iloc[:, :-1])
                #exit(0)
                _pg_dataset = self._best_model.predict(pd.DataFrame([pg_data], columns=self._column_heading).iloc[:, :-1])
                if isinstance(_pg_dataset, (str, int)):
                    _pg_dataset = [_pg_dataset]
                if isinstance(_pg_dataset, (list, np.ndarray)):
                    self._data[pg_data_name] = pd.concat([self._data[pg_data_name], pd.DataFrame([_pg_dataset], columns=[self._column_heading[-1]])], ignore_index=True) if pg_data_name in self._data else pd.DataFrame([_pg_dataset], columns=[self._column_heading[-1]])
                elif isinstance(_pg_dataset, pd.DataFrame):
                    self._data[pg_data_name] = pd.concat([self._data[pg_data_name], _pg_dataset], ignore_index=True) if pg_data_name in self._data else _pg_dataset

            if len(self._data[pg_data_name]) % 10000 == 0 and len(self._data[pg_data_name]) != 0:
                self.save(self._data[pg_data_name], pg_data_name)

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

    def save(self, data: pd.DataFrame, file_prefix: str = None) -> bool:
        if len(data) == 0: return True
        try:
            _data = data.replace({'|': ''}, regex=True)
            print(f"save data to {self._parameter['save_dir']}/{file_prefix}_{pgfile.get_random_filename('.csv')}")
            _filepath = f"{self._parameter['save_dir']}/{file_prefix}_{pgfile.get_random_filename('.csv')}"
            data.to_csv(_filepath, sep="|", index=False)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False


class PGLearningCaretExt(PGLearningCaret):
    def __init__(self, project_name: str = "learningnerext", logging_enable: str = False):
        super(PGLearningCaretExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

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


class PGLearningCaretSingleton(PGLearningCaret):
    __instance = None

    @staticmethod
    def get_instance():
        if PGLearningCaretSingleton.__instance == None:
            PGLearningCaretSingleton()
        else:
            return PGLearningCaretSingleton.__instance

    def __init__(self, project_name: str = "learninglstmsingleton", logging_enable: str = False):
        super(PGLearningCaretSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGLearningCaretSingleton.__instance = self


if __name__ == '__main__':
    test = PGLearningCaretExt()
    test.set_profile("default")
    filepath = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Input/heart.csv"
    #test.preprocessing(pd.read_csv(filepath))
    test.preprocessing(pd.read_csv(filepath))
    exit(0)
    test.get_tasks(filepath)
    print(test._data_inputs)
    test._process(*test._data_inputs[0])
    #test._process("heart", pd.read_csv(filepath))
    exit(0)
    test.model_train(pd.read_csv(filepath), parameters={'name': 'heart'})



    #test._process(example)
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








