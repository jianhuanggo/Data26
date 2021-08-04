import json
import os
import sys
import inspect
import pandas as pd
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List, Union
from Learning import pglearningcommon1, pglearningbase, pglearning
from Meta import pgclassdefault, pggenericfunc
from Data.Storage.S3 import pgs3
from Data.Utils import pgoperation
from Data.Storage import pgstorage
from itertools import repeat
from pycaret.classification import *
from subprocess import Popen
from Data.Utils import pgfile, pgdirectory, pgyaml
from sklearn.metrics import f1_score, recall_score, precision_score, accuracy_score, mean_squared_error, mean_absolute_error, mean_squared_log_error, r2_score


import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


__version__ = "1.7"


class PGHub(pglearningbase.PGLearningBase, pglearningcommon1.PGLearningCommon):
    def __init__(self, project_name: str = "pghub", logging_enable: str = False):
        super().__init__(project_name=project_name,
                                    object_short_name="PG_HUB",
                                    config_file_pathname=__file__.split('.')[0] + ".ini",
                                    logging_enable=logging_enable,
                                    config_file_type="ini")

        ### Common Variables
        self._name = "pghub"
        self._learning_type = "supervised"
        self._model_type = "supervised"
        self._data = {}
        self._pattern_match = {}
        self._parameters = {"aws_access_key_id": "AKIAXW42WHCP3KKG2BPJ",
                            "aws_secret_access_key": "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",
                            "region_name": "us-east-1",
                            "mode": "file"}

        self._hub_cloud_location = pgs3.PGS3Ext()
        self._hub_bucket_name = "s3://tag-data-hub-prod-0001"
        self._hub_cloud_location.create_client(self._parameters["aws_access_key_id"],
                                               self._parameters["aws_secret_access_key"],
                                               self._parameters["region_name"])

        self._hub_cloud_location.set_bucket(self._hub_bucket_name)
        self._column_heading = None
        ### Specific Variables
        self._models = {}
        self._best_model = None
        self._model_result = {}
        self._model_metrics = None
        self.set_profile("default")
        self.clean_profile()

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    def model_metrics_sel(self, pg_target_dataset: pd.DataFrame, pg_parameters: dict = {}) -> bool:
        """Automatically determine supervised learning metrics depends on whether it's Classification or Regression problem

        Args:
            pg_target_dataset: Dataframe only contains target labels
            pg_parameters: Accept overwrites

        Returns:
            The return value. True for success, False otherwise.

        """
        try:
            self._model_metrics = pg_parameters["overwrite"] if "overwrite" in pg_parameters else {'True': "f1",
                                                                                                   'False': "neg_mean_squared_error"}.get(
                str(len(pg_target_dataset.unique()) < 7))
            print(f"metrics is: {self._model_metrics}")
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def model_save(self, pg_model, pg_entity_name: str, pg_parameters: dict = {}) -> bool:
        """Returns True if weights are persisted

        Args:
            pg_model: the model needs to be saved
            pg_entity_name: An unique Identifier used to form the model name
            pg_parameters: parameters

        Returns:
            The return value. True for success, False otherwise.

        """
        try:
            with open (pg_parameters["dir"], "w") as f: json.dump(self._best_model, f)
            return save_model(pg_model, model_name=pg_entity_name)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    @pgoperation.pg_retry(3)
    def model_load(self, pg_entity_name: str, pg_parameters: dict = {}) -> bool:
        try:
            _pg_entity_name = pg_entity_name if pg_entity_name else pg_parameters['pg_entity_name']
            return self._hub_cloud_location.load(f"{self._hub_bucket_name}/{_pg_entity_name}", storage_parameter={**self._parameters, **pg_parameters}) if _pg_entity_name else False

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

        return False

    def preprocessing(self, pgdataset: Union[pd.DataFrame, np.ndarray] = None, pgscaler = None, parameter: dict = None) -> Union[list, None]:
        try:
            pass
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def model_train(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None, pg_parameters: dict = None):
        try:
            self.model_metrics_sel(pg_dataset.iloc[:, -1], pg_parameters=pg_parameters)
            self._model_train_automl(pg_dataset, pg_parameters=self._parameter)
            self._model_train_ensemble(pg_dataset, pg_parameters=self._parameter)
            self._model_train_ransomforest(pg_dataset, pg_parameters=self._parameter)

            #print(self._model_result["rf"][self._model_metrics])

            _comp_dict = {"rf": self._model_result["rf"][self._model_metrics], "automl": self._model_result["automl"][self._model_metrics], "ensemble": self._model_result["ensemble"][self._model_metrics]}
            print(_comp_dict)
            _sorted_comp_dict = {k: v for k, v in sorted(_comp_dict.items(), key=lambda item: item[1], reverse=True)}
            self._best_model = list(_sorted_comp_dict.keys())[0]
            print(f"picked model: {self._best_model}")

            print(int(self._model_result[self._best_model][self._model_metrics]) > int(self._model_save_threshold))

            self.model_save(self._models[self._best_model], pg_parameters['entity_name'])

            #if self.model_test()['f1'] > self._model_save_threshold
                #self.model_save(pg_parameters['entity_name'])

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise


    def model_test(self, pg_model_name: str, parameter: dict = None) -> Union[dict, None]:

        try:
            pass

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def pg_model_score(self, pg_actual: Union[tuple, list, set], pg_predicted: Union[tuple, list, set], pg_parameters: dict = {}) -> Union[None, dict]:

        """Returns appreciate metrics depends on whether classification or regression

        See details @ https://scikit-learn.org/stable/modules/model_evaluation.html

        Args:
            pg_actual: the model needs to be saved
            pg_predicted: An unique Identifier used to form the model name
            pg_parameters: parameters

        Returns:
            The return value. True for success, False otherwise.

        """

        _pg_actual = [int(x) for x in pg_actual]
        _pg_predicted = [int(x) for x in pg_predicted]

        if len(_pg_actual) != len(pg_actual):
            return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "the size of predicted does not match with size of actual")

        try:
            return {'accuracy': accuracy_score(_pg_actual, _pg_predicted),
                    'recall': recall_score(_pg_actual, _pg_predicted),
                    'precision': precision_score(_pg_actual, _pg_predicted),
                    'f1': f1_score(_pg_actual, _pg_predicted)
                    } if self._model_metrics == 'f1' else {'neg_mean_squared_error': mean_squared_error(_pg_actual, _pg_predicted),
                                                           'neg_mean_squared_log_error': mean_squared_log_error(_pg_actual, _pg_predicted),
                                                           'neg_mean_absolute_error': mean_absolute_error(_pg_actual, _pg_predicted),
                                                           'r2': r2_score(_pg_actual, _pg_predicted)}
        except Exception as err:
            return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)



    def get_tasks(self, pg_data: Union[str, list, pd.DataFrame, dict], pg_parameters: dict = {}) -> bool:
        pass

    def _process(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        try:
            #print(self._best_model)
            #print(f"pg_data1: {pg_data}")
            #print(f"pg_data_name1: {pg_data_name}")
            #print(f"pg_parameters1: {pg_parameters}")
            if not pg_data:
                return None

            if self._best_model in self._models and self._models[self._best_model]:
                pass
            elif pgfile.isfileexist(os.path.join(self._parameter['save_dir'], f"{self._parameter['entity_name']}.pkl")):
                with open(os.path.join(self._parameter['save_dir'], f"{self._parameter['entity_name']}.conf")) as f: self._best_model = json.load(f)["model_name"]
                self._models[self._best_model] = self.model_load(
                    os.path.join(self._parameter['save_dir'], self._parameter['entity_name']))

                if self._best_model in self._models and self._models[self._best_model]:
                    print(f"model {self._parameter['entity_name']} loaded")

            if isinstance(pg_data, pd.DataFrame):
                self._data[pg_data_name] = self._models[self._best_model].predict(pg_data.iloc[:, : -1])
            elif isinstance(pg_data, list):
                _pg_dataset = self._models[self._best_model].predict(pd.DataFrame([pg_data], columns=self._column_heading).iloc[:, :-1])
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
            if not self._data_inputs:
                return True
            if name:
                _item = self._data_inputs[name]
                #print(f"_item: {_item}")
                #exit(0)
                self._process(name, _item['data'], _item['parameter'], )
            else:
                for _index, _data in self._data_inputs.items():
                    _item = self._data_inputs[_index]
                    self._process(name, _item['data'], _item['parameter'])
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


class PGHubExt(PGHub):
    def __init__(self, project_name: str = "hubext", logging_enable: str = False):
        super().__init__(project_name=project_name, logging_enable=logging_enable)

        ### Specific Variables
        self._model_subtype = {}
        os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'


class PGHubSingleton(PGHub):
    __instance = None

    @staticmethod
    def get_instance():
        if PGHubSingleton.__instance == None:
            PGHubSingleton()
        else:
            return PGHubSingleton.__instance

    def __init__(self, project_name: str = "hubsingleton", logging_enable: str = False):
        super(PGHubSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGHubSingleton.__instance = self


if __name__ == '__main__':
    test = PGHubExt()
    #test.set_profile("default")
    print(test.model_load("output1", {"directory": "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PGHub/Test/"}))

    exit(0)

    filepath = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Input/heart.csv"
    #test.preprocessing(pd.read_csv(filepath))
    #test.preprocessing(pd.read_csv(filepath))
    #test.get_tasks(filepath)
    test.get_tasks({'output1': ['/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test2/output1_1.csv']})
    #print(f"data input: {test._data_inputs}")
    exit(0)
    test.process("heart")
    print(f"data : {test.data}")
    #test._process("heart", pd.read_csv(filepath))
    exit(0)
    test.model_train(pd.read_csv(filepath), parameters={'name': 'heart'})








