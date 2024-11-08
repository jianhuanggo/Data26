#https://pycaret.org/guide/

import re
import json
import os
import sys
import inspect
from pycaret.classification import *
import pandas as pd
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List, Union
from Learning import pglearningcommon1, pglearningbase, pglearning
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
from Data.Storage import pgstorage
from itertools import repeat
from subprocess import Popen
from Data.StorageFormat import pgstorageformat
from Data.Utils import pgfile, pgdirectory, pgyaml
from Learning.PGHub import pghub
from sklearn.metrics import f1_score, recall_score, precision_score, accuracy_score, mean_squared_error, mean_absolute_error, mean_squared_log_error, r2_score
from sklearn.model_selection import train_test_split, cross_val_score

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


__version__ = "1.7"

# problem: Lightgbm OSError, Library not loaded
# solution for mac: brew install libomp
#
# link: https://pycaret.org/group-features/
# To get better results, we may need to apply feature engineering


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
        self._data = {}
        self._pattern_match = {}
        self._parameter = {'storage_type': 'localdisk',  # default storage to save and load models
                           'storage_name': 'lr',  # name of storage
                           'default_model_dir': os.path.join(pgdirectory.get_filename_from_dirpath(__file__), "/model"),
                           'test_size': 0.2}  # if test_flag set to true, percentage of data will be used as testing dataset

        self._column_heading = None
        ### Specific Variables
        self._models = {}
        self._best_model = None
        self._model_result = {}
        self._model_metrics = None
        self._model_training_client_id = 0
        self._model_save_threshold = 0.75
        if not pgdirectory.createdirectory(self._parameter['default_model_dir']):
            sys.exit(99)

        self._model_distribution_strategy = "centralStoragestrategy"
        self._data_inputs = {}
        # self.get_config(profile="default")
        self._X_train = self._X_test = self._y_train = self._y_test = None
        self.set_profile("default")
        self.clean_profile()
        self._pg_hub = pghub.PGHubExt()

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
            self._model_metrics = pg_parameters["overwrite"] if "overwrite" in pg_parameters else {'True': "F1",
                                                                                                   'False': "MSE"}.get(
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
            _pg_save_dir = pg_parameters.get("dir", None) or self._parameter['default_model_dir']
            with open(_pg_save_dir, "w") as f:
                json.dump(self._best_model, f)
            return save_model(pg_model, model_name=pg_entity_name)

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
            # high_cardinality_features = ['native-country'])
            if len(pgdataset) < 50000:
                return list(set([col for col in pgdataset if len(pgdataset[col].unique()) < 7] + [col for col in pgdataset.columns if pgdataset.dtypes[col] not in ["int64", "float64"]]))
            else:
                return list(set([col for col in pgdataset[:50000] if len(pgdataset[:50000][col].unique()) < 7] + [col for col in pgdataset.columns if pgdataset.dtypes[col] not in ["int64", "float64"]]))

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def _model_train_automl(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None,
                           pg_parameters: dict = None) -> bool:
        try:
            if self._model_metrics == "F1":
                _pg_model_type = "classification"
                from pycaret.classification import automl, compare_models, tune_model, ensemble_model, blend_models, \
                    setup
                experiment = setup(pg_dataset,
                                   silent=True,
                                   target=pg_dataset.columns.tolist()[-1],
                                   categorical_features=self.preprocessing(pg_dataset.iloc[:, :-1]),
                                   # fix_imbalance=True,
                                   # normalize=True,
                                   # feature_selection=True,
                                   remove_multicollinearity=True,
                                   multicollinearity_threshold=0.8,
                                   # remove_outliers=True,
                                   log_experiment=True,
                                   experiment_name=self._parameter['log_dir'])
            else:
                _pg_model_type = "regression"
                from pycaret.regression import automl, compare_models, tune_model, ensemble_model, blend_models, setup
                experiment = setup(pg_dataset,
                                   silent=True,
                                   target=pg_dataset.columns.tolist()[-1],
                                   categorical_features=self.preprocessing(pg_dataset.iloc[:, :-1]),
                                   normalize=True,
                                   # feature_selection=True,
                                   remove_multicollinearity=True,
                                   multicollinearity_threshold=0.8,
                                   # remove_outliers=True,
                                   transform_target=True,
                                   log_experiment=True,
                                   experiment_name=self._parameter['log_dir'])

            print(pg_parameters)

            _pg_num_selection = pg_parameters.get('num_selection', 5)

            #_pg_best_models = compare_models(n_select=_pg_num_selection, sort=self._model_metrics) if _pg_model_type == "classification" else compare_models(n_select=_pg_num_selection)
            _pg_best_models = compare_models(n_select=_pg_num_selection, sort=self._model_metrics)

            print(f"_pg_best_models: {_pg_best_models}")
            print(_pg_num_selection)
            print(self._model_metrics)
            print(_pg_model_type)

            # tune top 5 base models
            # https://pycaret.org/tune-model/

            #_pg_tuned_best_models = [tune_model(i, optimize=self._model_metrics) for i in _pg_best_models if i] if _pg_model_type == "classification" else [tune_model(i, optimize='MSE') for i in _pg_best_models if i]
            #_pg_tuned_best_models = [tune_model(i, optimize=self._model_metrics) for i in _pg_best_models if i]

            _pg_tuned_best_models = []
            for _pg_model in _pg_best_models:
                try:
                    _pg_tuned = tune_model(_pg_model, optimize=self._model_metrics)
                    if _pg_tuned:
                        _pg_tuned_best_models.append(_pg_tuned)
                except:
                    continue



            print("aaaaaaaaa")
            print(f"_pg_tuned_best_models: {_pg_tuned_best_models}")
            print("bbbbbbbbb")
            #print(type(_pg_tuned_best_models))
            print(len(_pg_tuned_best_models))


            # ensemble top 5 tuned models
            #_pg_bagged_best_models = [ensemble_model(i) for i in _pg_tuned_best_models if i]
            _pg_bagged_best_models = []
            for _pg_model in _pg_tuned_best_models:
                try:
                    _pg_bagged = ensemble_model(_pg_model)
                    if _pg_bagged:
                        _pg_bagged_best_models.append(_pg_bagged)
                except:
                    continue


            # blend top 5 base models
            _pg_blender = blend_models(estimator_list=_pg_best_models)
            # select best model
            self._models["automl"] = automl(optimize=self._model_metrics)
            print(self._models)
            self._model_result["automl"] = self.model_test("automl")

            print(self._model_result)

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def _model_train_ensemble(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None,
                           pg_parameters: dict = None) -> bool:
        try:
            if self._model_metrics == "F1":
                _pg_model_type = "classification"
                from pycaret.classification import automl, compare_models, tune_model, ensemble_model, blend_models, \
                    setup
                experiment = setup(pg_dataset,
                                   silent=True,
                                   target=pg_dataset.columns.tolist()[-1],
                                   categorical_features=self.preprocessing(pg_dataset.iloc[:, :-1]),
                                   # fix_imbalance=True,
                                   # normalize=True,
                                   # feature_selection=True,
                                   remove_multicollinearity=True,
                                   multicollinearity_threshold=0.8,
                                   # remove_outliers=True,
                                   log_experiment=True,
                                   experiment_name=self._parameter['log_dir'])
            else:
                _pg_model_type = "regression"
                from pycaret.regression import automl, compare_models, tune_model, ensemble_model, blend_models, setup
                experiment = setup(pg_dataset,
                                   silent=True,
                                   target=pg_dataset.columns.tolist()[-1],
                                   categorical_features=self.preprocessing(pg_dataset.iloc[:, :-1]),
                                   normalize=True,
                                   # feature_selection=True,
                                   remove_multicollinearity=True,
                                   multicollinearity_threshold=0.8,
                                   # remove_outliers=True,
                                   transform_target=True,
                                   log_experiment=True,
                                   experiment_name=self._parameter['log_dir'])

            _pg_num_selection = pg_parameters.get('num_selection', 5)

            _pg_best_models = compare_models(n_select=_pg_num_selection, sort=self._model_metrics)
            #_best_models = [tune_model(i, optimize=self._model_metrics) for i in _pg_best_models if i]

            _pg_tuned_best_models = []
            for _pg_model in _pg_best_models:
                try:
                    _pg_tuned = tune_model(_pg_model, optimize=self._model_metrics)
                    if _pg_tuned:
                        _pg_tuned_best_models.append(_pg_tuned)
                except:
                    continue

            _best_models = _pg_tuned_best_models[0]
            self._models["ensemble"] = ensemble_model(_best_models, method='Bagging', optimize=self._model_metrics)

            self._model_result["ensemble"] = self.model_test("ensemble")

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def _model_train_ransomforest(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None,
                             pg_parameters: dict = None) -> bool:
        try:
            if self._model_metrics == "F1":
                from pycaret.classification import automl, compare_models, tune_model, ensemble_model, blend_models, \
                    setup
                experiment = setup(pg_dataset,
                                   silent=True,
                                   target=pg_dataset.columns.tolist()[-1],
                                   categorical_features=self.preprocessing(pg_dataset.iloc[:, :-1]),
                                   # fix_imbalance=True,
                                   # normalize=True,
                                   # feature_selection=True,
                                   remove_multicollinearity=True,
                                   multicollinearity_threshold=0.8,
                                   # remove_outliers=True,
                                   log_experiment=True,
                                   experiment_name=self._parameter['log_dir'])
            else:
                from pycaret.regression import automl, compare_models, tune_model, ensemble_model, blend_models, setup
                experiment = setup(pg_dataset,
                                   silent=True,
                                   target=pg_dataset.columns.tolist()[-1],
                                   categorical_features=self.preprocessing(pg_dataset.iloc[:, :-1]),
                                   normalize=True,
                                   # feature_selection=True,
                                   remove_multicollinearity=True,
                                   multicollinearity_threshold=0.8,
                                   # remove_outliers=True,
                                   transform_target=True,
                                   log_experiment=True,
                                   experiment_name=self._parameter['log_dir'])

            _pg_num_selection = pg_parameters.get('num_selection', 5)

            _pg_rf = create_model('rf')
            print("here here here")

            for i in range(3):
                try:
                    self._models["rf"] = tune_model(_pg_rf, optimize=self._model_metrics)
                    break
                except Exception as err:
                    print(err)
                    continue
            self._model_result["rf"] = self.model_test("rf")

            print(self._model_result)
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def model_train(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None, pg_parameters: dict = None) -> Tuple[bool, str]:
        try:
            if not self.model_metrics_sel(pg_dataset.iloc[:, -1], pg_parameters=pg_parameters):
                return False, "model_metrics_sel"
            if not self._model_train_automl(pg_dataset, pg_parameters=self._parameter):
                return False, "_model_train_automl"
            if not self._model_train_ensemble(pg_dataset, pg_parameters=self._parameter):
                return False, "_model_train_ensemble"
            if not self._model_train_ransomforest(pg_dataset, pg_parameters=self._parameter):
                return False, "_model_train_ransomforest"

            #print(self._model_result["rf"][self._model_metrics])

            _comp_dict = {"rf": self._model_result["rf"][self._model_metrics], "automl": self._model_result["automl"][self._model_metrics], "ensemble": self._model_result["ensemble"][self._model_metrics]}
            print(_comp_dict)
            _sorted_comp_dict = {k: v for k, v in sorted(_comp_dict.items(), key=lambda item: item[1], reverse=True)}
            self._best_model = list(_sorted_comp_dict.keys())[0]
            print(f"picked model: {self._best_model}")

            if int(self._model_result[self._best_model][self._model_metrics]) > int(self._model_save_threshold):
                self.model_save(self._models[self._best_model], pg_parameters['entity_name'])
            else:
                self.model_save(self._models[self._best_model], pg_parameters['entity_name'])

            #if self.model_test()['f1'] > self._model_save_threshold
                #self.model_save(pg_parameters['entity_name'])
            return True, ""

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False, "model_train"

    """
    def model_train(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None, pg_parameters: dict = None):
        try:
            self.model_metrics_sel(pg_dataset.iloc[:, -1], pg_parameters=pg_parameters)
            #print(self.preprocessing(pgdataset.iloc[:, :-1]))
            #print(pgdataset.columns.tolist()[-1])

            if self._model_metrics == "f1":
                from pycaret.classification import automl, compare_models, tune_model, ensemble_model, blend_models, setup
                experiment = setup(pg_dataset,
                                   silent=True,
                                   target=pg_dataset.columns.tolist()[-1],
                                   categorical_features=self.preprocessing(pg_dataset.iloc[:, :-1]),
                                   #fix_imbalance=True,
                                   #normalize=True,
                                   #feature_selection=True,
                                   remove_multicollinearity=True,
                                   multicollinearity_threshold=0.8,
                                   #remove_outliers=True,
                                   log_experiment=True,
                                   experiment_name=self._parameter['log_dir'])
            else:
                from pycaret.regression import automl, compare_models, tune_model, ensemble_model, blend_models, setup
                experiment = setup(pg_dataset,
                                   silent=True,
                                   target=pg_dataset.columns.tolist()[-1],
                                   categorical_features=self.preprocessing(pg_dataset.iloc[:, :-1]),
                                   normalize=True,
                                   #feature_selection=True,
                                   remove_multicollinearity=True,
                                   multicollinearity_threshold=0.8,
                                   #remove_outliers=True,
                                   transform_target=True,
                                   log_experiment=True,
                                   experiment_name=self._parameter['log_dir'])


            #_pg_num_selection = pg_parameters['num_selection'] if "num_selection" in pg_parameters else 5

            #_pg_best_models = compare_models(n_select=_pg_num_selection, sort=self._model_metrics)
            # tune top 5 base models
            #_pg_tuned_best_models = [tune_model(i, optimize=self._model_metrics) for i in _pg_best_models]
            # ensemble top 5 tuned models
            #_pg_bagged_best_models = [ensemble_model(i) for i in _pg_tuned_best_models]
            # blend top 5 base models
            #_pg_blender = blend_models(estimator_list=_pg_best_models)
            # select best model
            #self._best_model = automl(optimize=self._model_metrics)

            #print(f"bagged_top5: {_pg_bagged_best_models}")
            #print(f"blender: {_pg_blender}")
            #print(f"best: {self._best_model}")
            #best_model_results = pull()
            #print(best_model_results)
            #self.model_test()

            #if self.model_test()['f1'] > self._model_save_threshold
                #self.model_save(pg_parameters['entity_name'])

            _pg_num_selection = pg_parameters['num_selection'] if "num_selection" in pg_parameters else 5
            _pg_best_models = compare_models(n_select=_pg_num_selection, sort=self._model_metrics)
            _best_models = [tune_model(i, optimize=self._model_metrics) for i in _pg_best_models]

            self._best_model = _best_models[0]
            print(self._best_model)

            xx = ensemble_model(self._best_model, method='Bagging', optimize=self._model_metrics)
            #_boosted_dt = ensemble_model(self._best_model, method='Boosting', n_estimators=100)
            self._best_model = blend_models(estimator_list=_pg_best_models)
            _stacker = stack_models(estimator_list=_best_models[1:], meta_model=_best_models[0])

            print(self.model_test())

            #print(f"best model: {self._best_model}")
            #print(f"boosted model: {_boosted_dt}")
            #print(f"top 5 models: {_best_models}")
            #print(f"stack: {_stacker}")


        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise
    """

    def model_test(self, pg_model_name: str, parameter: dict = None) -> Union[dict, None]:

        try:
            print("start apply model to test dataset...")

            if self._model_metrics == 'F1':
                _pg_calibrated_dt = calibrate_model(self._models[pg_model_name])
                #print(_pg_calibrated_dt)

            print(self._models)

            # https://pycaret.org/predict-model/
            _pg_pred_holdout = predict_model(self._models[pg_model_name])
            print(_pg_pred_holdout)
            #return self.pg_model_score(_pg_pred_holdout['target'].values.tolist(), _pg_pred_holdout['Label'].values.tolist())
            return self.pg_model_score(_pg_pred_holdout.iloc[:, -1].values.tolist(), _pg_pred_holdout['Label'].values.tolist())

            #_pg_pred_holdout.to_csv("/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test/output.csv")
            #print([x for x in zip(_pg_pred_holdout['target'].values.tolist(), _pg_pred_holdout.iloc['Label'].values.tolist())])
            #print(self.pg_model_f_score(_pg_pred_holdout['target'].values.tolist(), _pg_pred_holdout['Label'].values.tolist()))
            #print(f1_score(_pg_pred_holdout.iloc[:, -2].values.tolist(), _pg_pred_holdout.iloc[:, -1].values.tolist()))
            #print(predict_model(self._best_model, pgdataset))
            #self._best_model.predict

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
                    'F1': f1_score(_pg_actual, _pg_predicted)
                    } if self._model_metrics == 'F1' else {'neg_mean_squared_error': mean_squared_error(_pg_actual, _pg_predicted),
                                                           'neg_mean_squared_log_error': mean_squared_log_error(_pg_actual, _pg_predicted),
                                                           'MSE': mean_absolute_error(_pg_actual, _pg_predicted),
                                                           'r2': r2_score(_pg_actual, _pg_predicted)}
        except Exception as err:
            return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

    def get_model_filename(self, parameters: dict) -> str:
        try:
            _filename = ""
            for _key, _val in parameters.items():
                _filename += f"{str(_key)}-{str(_val)}_"
            return '_'.join(_filename.split('_')[:-1])

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return ""

    def get_tasks(self, pg_data: Union[str, list, dict], pg_parameters: dict = {}) -> bool:

        """Prepares inputs into a standard format
        ######[(name(string), data(varies), parameters(dict)), (name1(string), data1(varies), parameters1(dict)), ...]

        [filename1, filename2, filename3...]
        {"entity_name": [filename1, filename2, filename3 ...]}


        Four type of inputs:
            1) if dataset is a string, then it expects to be a filename in yaml or csv format with no entity_name
            2) if dataset is a list with no parameter, then it expects to be a list of filename in yaml or csv format with no entity_name
            3) if dataset is a dict, then it expects to be a filename in yaml or csv format with corresponding entity_name

        Examples:
            1) "<filepath1>"
            2) ["<filepath1>", "<filepath2>"...]
            3) {"<entity_name": ["<filepath1>", "<filepath2>"...]}

        Args:
            pg_data: data
            pg_parameters: parameters

        Returns:

        """
        _pg_data = {}
        print(f"I'm here ........{pg_data}")
        try:
            if not pg_data:
                return True

            print("I'm here2222 ........")

                ### best model doesn't not exist, then attempt to load it
            if isinstance(pg_data, str):
                _pg_data = {re.split('_|\.', pg_data.split('/')[-1])[0]: pg_data}
            elif isinstance(pg_data, list):
                for _entity_name in [(re.split('_|\.', x.split('/')[-1])[0], x) for x in pg_data]:
                    _pg_data[_entity_name[0]] = [_pg_data[_entity_name[0]], _entity_name[1]] if _entity_name[0] in _pg_data else _entity_name[1]

            _pg_data = _pg_data or pg_data

            if isinstance(_pg_data, dict):
                for _pg_entity, _pg_tgt_filepath in _pg_data.items():

                    _pg_tgt_dir = '/'.join(_pg_tgt_filepath[0].split('/')[:-1]) if isinstance(_pg_tgt_filepath, list) else '/'.join(_pg_tgt_filepath.split('/')[:-1])
                    _pg_model_filepath = _pg_tgt_dir or self._parameter['save_dir']
                    if self._pg_hub.model_load(_pg_entity, {"directory": _pg_tgt_dir}) and pgfile.isfileexist(os.path.join(_pg_model_filepath, f"{_pg_entity}.pkl")) and pgfile.isfileexist(os.path.join(_pg_model_filepath, f"{_pg_entity}.conf")):
                        with open(os.path.join(_pg_model_filepath, f"{_pg_entity}.conf")) as f: self._best_model = json.load(f)["model_name"]
                        self._models[self._best_model] = self.model_load(os.path.join(_pg_model_filepath, f"{_pg_entity}"))
                    ### verification
                        if self._best_model in self._models and self._models[self._best_model]:
                            print(f"model {_pg_entity} loaded")
                            print(self._best_model)
                            print(self._models)
                            print(pg_data)
                    print("I'm here33333 ........")

                    ### substitute this code with storageformat
                    with pgstorageformat.pg_set_storage_format(_pg_tgt_filepath) as fileformat:
                        _df = fileformat["data"]
                        print(_df)

                        self._column_heading = _df.columns.values.tolist()
                        print(self._column_heading)
                        self._parameter = {**self._parameter, **pg_parameters}
                        if self._models.get(self._best_model, None):
                            self._data_inputs = {_pg_entity: {'data': _df, 'parameter': self._parameter}}
                        # self._data_inputs = [(x[0], x[1][0].split(',')) for x in zip(repeat(self._parameter['entity_name']), _df.values.tolist())] if "entity_name" in self._parameter else [x[0].split(',') for x in _df.values.tolist()]
                        # self._data_inputs = [x for x in zip(repeat(self._parameter['entity_name']), _df.values.tolist())] if "entity_name" in self._parameter else _df.values.tolist()

                        else:

                            if self._model_training_client_id % self._parameter.get('num_client', 1) == self._parameter.get('client_id', 0):
                                print(f"client_id: {self._parameter.get('client_id', 0)}")
                                print(self._parameter)
                                # self._X_train, self._X_test, self._y_train, self._y_test = train_test_split(_df.iloc[:, :-1], _df.iloc[:, -1], test_size=0.2,
                                #    shuffle=False)
                                # self.model_train(pd.concat([self._X_train, self._y_train], axis=1), pg_parameters=self._parameter)
                                # pg_data = pd.concat([self._X_test, self._y_test], axis=1)
                                # self._data_inputs = [(x[0], x[1].split(',')) for x in zip(repeat(self._parameter['entity_name']),
                                #                                pg_data.values.tolist())] if "entity_name" in self._parameter else [x.split(',') for x in
                                #                                pg_data.values.tolist()]
                                # self._data_inputs = [x for x in zip(repeat(self._parameter['entity_name']), pg_data.values.tolist())] if "entity_name" in self._parameter else pg_data.values.tolist()
                                _pg_result, _pg_func = self.model_train(_df, pg_parameters={**self._parameter, **{"entity_name": _pg_entity}})
                                if not _pg_result:
                                    print(f"Something wrong is in {_pg_func}")
                                    return False
                else:
                    pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "format not supported")
                return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

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
        self._model_subtype = {}
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

    """
    a = pd.read_csv("/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test/output.csv")
    print(a['target'].values.tolist())
    print(f1_score(a['target'].values.tolist(), a['Label'].values.tolist()))
    print(accuracy_score(a['target'].values.tolist(), a['Label'].values.tolist()))
    print(recall_score(a['target'].values.tolist(), a['Label'].values.tolist()))
    print(precision_score(a['target'].values.tolist(), a['Label'].values.tolist()))
    """

    #_data = pd.read_csv('/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test2/heart1.csv')
    #print(_data)
    #print(_data.iloc[: , -1].values.tolist())
    #print(_data.isnull().sum())
    #exit(0)

    test = PGLearningCaretExt()
    test.set_profile("default")
    filepath = "/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Input/heart1.csv"
    #test.preprocessing(pd.read_csv(filepath))
    #test.preprocessing(pd.read_csv(filepath))
    #test.get_tasks(filepath)
    test.get_tasks(['/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test2/heart1.csv'])
    print(f"data input: {test._data_inputs}")
    exit(0)
    test.process("heart")
    print(f"data : {test.data}")
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








