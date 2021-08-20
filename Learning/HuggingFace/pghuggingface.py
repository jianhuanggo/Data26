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
from Data.Utils import pgfile, pgdirectory, pgyaml
from sklearn.metrics import f1_score, recall_score, precision_score, accuracy_score
from sklearn.model_selection import train_test_split, cross_val_score
from transformers import AutoTokenizer, AutoModel

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

###url="https://huggingface.co/transformers/v3.0.2/main_classes/pipelines.html"

__version__ = "1.7"

# problem: Lightgbm OSError, Library not loaded
# solution for mac: brew install libomp
#
# link: https://pycaret.org/group-features/
# To get better results, we may need to apply feature engineering


class PGLearningHuggingFace(pglearningbase.PGLearningBase, pglearningcommon1.PGLearningCommon):
    def __init__(self, project_name: str = "learninghuggingface", logging_enable: str = False):
        super(PGLearningHuggingFace, self).__init__(project_name=project_name,
                                                    object_short_name="PG_LRN_HF",
                                                    config_file_pathname=__file__.split('.')[0] + ".ini",
                                                    logging_enable=logging_enable,
                                                    config_file_type="ini")

        ### Common Variables
        self._name = "learninghuggingface"
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
        self._models = {}
        self._best_model = None
        self._model_result = {}
        self._model_metrics = None
        self._model_training_client_id = 1
        self._model_save_threshold = 0.75
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
                           pg_parameters: dict = None):
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

            _pg_num_selection = pg_parameters['num_selection'] if "num_selection" in pg_parameters else 5

            _pg_best_models = compare_models(n_select=_pg_num_selection, sort=self._model_metrics)
            # tune top 5 base models
            _pg_tuned_best_models = [tune_model(i, optimize=self._model_metrics) for i in _pg_best_models]
            # ensemble top 5 tuned models
            _pg_bagged_best_models = [ensemble_model(i) for i in _pg_tuned_best_models]
            # blend top 5 base models
            _pg_blender = blend_models(estimator_list=_pg_best_models)
            # select best model
            self._models["automl"] = automl(optimize=self._model_metrics)

            self._model_result["automl"] = self.model_test("automl")


        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def _model_train_ensemble(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None,
                           pg_parameters: dict = None):
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

            _pg_num_selection = pg_parameters['num_selection'] if "num_selection" in pg_parameters else 5

            _pg_best_models = compare_models(n_select=_pg_num_selection, sort=self._model_metrics)
            _best_models = [tune_model(i, optimize=self._model_metrics) for i in _pg_best_models]

            _best_models = _best_models[0]
            self._models["ensemble"] = ensemble_model(_best_models, method='Bagging', optimize=self._model_metrics)

            self._model_result["ensemble"] = self.model_test("ensemble")

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def _model_train_ransomforest(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None,
                             pg_parameters: dict = None):
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

            _pg_num_selection = pg_parameters['num_selection'] if "num_selection" in pg_parameters else 5

            _pg_rf = create_model('rf')
            self._models["rf"] = tune_model(_pg_rf, optimize=self._model_metrics)

            self._model_result["rf"] = self.model_test("rf")

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    def model_train(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None, pg_parameters: dict = None):
        try:
            self.model_metrics_sel(pg_dataset.iloc[:, -1], pg_parameters=pg_parameters)
            self._model_train_automl(pg_dataset, pg_parameters=self._parameter)
            self._model_train_ensemble(pg_dataset, pg_parameters=self._parameter)
            self._model_train_ransomforest(pg_dataset, pg_parameters=self._parameter)

            _comp_dict = {"rf": self._model_result["rf"][self._model_metrics], "automl": self._model_result["automl"][self._model_metrics], "ensemble": self._model_result["ensemble"][self._model_metrics]}
            _sorted_comp_dict = {k: v for k, v in sorted(_comp_dict.items(), key=lambda item: item[1])}

            self._best_model = list(_sorted_comp_dict.keys())[0]
            print(f"picked model: {self._best_model}")

            if self._model_result[list(_sorted_comp_dict.keys())[0]] > self._model_save_threshold:
                self.model_save(self._best_model, pg_parameters['entity_name'])

            #if self.model_test()['f1'] > self._model_save_threshold
                #self.model_save(pg_parameters['entity_name'])

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            raise

    """
    def model_train(self, pg_dataset: Union[list, np.ndarray, pd.DataFrame] = None, data_scaler=None, pg_parameters: dict = None):
        try:
            self.model_metrics_sel(pg_dataset.iloc[:, -1], pg_parameters=pg_parameters)
            #print(self.preprocessing(pgdataset.iloc[:, :-1]))
            #print(pgdataset.columns.tolist()[-1])

            if self._model_metrics == "F1":
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
                _pg_calibrated_dt = calibrate_model(self._best_model[pg_model_name])
                #print(_pg_calibrated_dt)
            _pg_pred_holdout = predict_model(self._best_model[pg_model_name])
            #print(_pg_pred_holdout)
            return self.pg_model_score(_pg_pred_holdout['target'].values.tolist(), _pg_pred_holdout['Label'].values.tolist())

            #_pg_pred_holdout.to_csv("/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test/output.csv")
            #print([x for x in zip(_pg_pred_holdout['target'].values.tolist(), _pg_pred_holdout.iloc['Label'].values.tolist())])
            #print(self.pg_model_f_score(_pg_pred_holdout['target'].values.tolist(), _pg_pred_holdout['Label'].values.tolist()))
            #print(f1_score(_pg_pred_holdout.iloc[:, -2].values.tolist(), _pg_pred_holdout.iloc[:, -1].values.tolist()))
            #print(predict_model(self._best_model, pgdataset))
            #self._best_model.predict

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None

    def pg_model_score(self, actual: Union[tuple, list, set], predicted: Union[tuple, list, set]) -> Union[None, dict]:

        _actual = [int(x) for x in actual]
        _predicted = [int(x) for x in predicted]

        if len(_actual) != len(actual):
            return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "the size of predicted does not match with size of actual")

        try:
            return {'accuracy': accuracy_score(_actual, _predicted),
                    'recall': recall_score(_actual, _predicted),
                    'precision': precision_score(_actual, _predicted),
                    'f1': f1_score(_actual, _predicted)
                    }
        except Exception as err:
            return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

        else:
            return pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                 "this calculation needs multiple numbers as input")

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

                    if len(_df) < 100:
                        pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                                      "not enough data to train the models")
                        raise

                    self._column_heading = _df.columns.values.tolist()
                    self._parameter = {**self._parameter, **pg_parameters}

                    if self._best_model:
                        self._data_inputs = [x for x in zip(repeat(self._parameter['entity_name']), _df.values.tolist())] if "entity_name" in self._parameter else _df.values.tolist()
                    else:
                        if self._model_training_client_id % pg_parameters['num_client'] == pg_parameters['client_id']:
                            print(f"client_id: {pg_parameters['client_id']}")
                            self.model_train(_df, pg_parameters=self._parameter)
            elif isinstance(pg_data, list) and pg_parameters:
                self._data_inputs = list(zip(pg_data, pg_parameters))
            else:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "pg_data needs to be a list or str")
                return False
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _process2(self, pg_data_name: str, pg_data: Union[pd.DataFrame, str] = None, pg_parameters: dict = {}) -> bool:
        from transformers import pipeline, AutoModelForTokenClassification, AutoTokenizer

        """process one record at a time, expects only one record in pg_data

        Args:
            pg_data_name: name of the dataset
            pg_data: data
            pg_parameters: parameters

        Option:
        
        "feature-extraction": FeatureExtractionPipeline.
        "text-classification": TextClassificationPipeline.
        "sentiment-analysis": (alias of "text-classification") ~transformers.TextClassificationPipeline.
        "token-classification": TokenClassificationPipeline.
        "ner"(alias of "token-classification"): ~transformers.TokenClassificationPipeline.
        "question-answering": QuestionAnsweringPipeline.
        "fill-mask": FillMaskPipeline.
        "summarization": SummarizationPipeline.
        "translation_xx_to_yy": TranslationPipeline.
        "text2text-generation": Text2TextGenerationPipeline.
        "text-generation": TextGenerationPipeline.
        "zero-shot-classification: ZeroShotClassificationPipeline.
        "conversational": ConversationalPipeline.
        
        Returns: returns true if successful
            
        """

        try:
            _pg_dataset = None
            _pg_pipeline_name = pg_parameters.get("pg_hf_pipeline", None)
            if _pg_pipeline_name:
                self._best_model = pipeline(_pg_pipeline_name)
            elif self._best_model:
                pass
            elif pgfile.isfileexist(os.path.join(self._parameter['save_dir'], f"{self._parameter['entity_name']}.pkl")):
                self._best_model = self.model_load(self._parameter['entity_name'])
                print(f"model {self._parameter['entity_name']} loaded")

            #sequence = "Using a transformer network is simple"  # get input
            #tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")  # tokenizer = AutoTokenizer.from_pretrained("sshleifer/tiny-gpt2")
            #model_inputs = tokenizer(sequence, padding=True, truncation=True, return_tensors="pt")  # or do 3 three steps in one command
            #output = self._best_model(model_inputs)  # make prediction
            #print(output)

            if isinstance(pg_data, pd.DataFrame):
                _pg_text_column = pg_parameters.get("pg_text_column") if "pg_text_column" in pg_parameters else "text"
                _pg_dataset = [self._best_model(pg_data[_pg_text_column][0])[0]['label'], str(round(self._best_model(pg_data[_pg_text_column][0])[0]['score'], 4))]
            elif isinstance(pg_data, str):
                _pg_dataset = [self._best_model(pg_data)[0]['label'], str(round(self._best_model(pg_data)[0]['score'], 4))]
                #print(f"label: {_pg_dataset['label']}, with score: {round(_pg_dataset['score'], 4)}")

            if isinstance(_pg_dataset, (list, np.ndarray)):
                self._data[pg_data_name] = pd.concat(
                    [self._data[pg_data_name], pd.DataFrame([_pg_dataset], columns=["sentiment", "score"])],
                    ignore_index=True) if pg_data_name in self._data else pd.DataFrame([_pg_dataset], columns=["sentiment", "score"])
            elif isinstance(_pg_dataset, pd.DataFrame):
                self._data[pg_data_name] = pd.concat([self._data[pg_data_name], _pg_dataset],
                                                     ignore_index=True) if pg_data_name in self._data else _pg_dataset

            if len(self._data[pg_data_name]) % 10000 == 0 and len(self._data[pg_data_name]) != 0:
                self.save(self._data[pg_data_name], pg_data_name)

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False
        
        # Sentiment analysis pipeline




        # Question answering pipeline, specifying the checkpoint identifier
        #pipeline('question-answering', model='distilbert-base-cased-distilled-squad', tokenizer='bert-base-cased')

        # Named entity recognition pipeline, passing in a specific model and tokenizer
        #model = AutoModelForTokenClassification.from_pretrained("dbmdz/bert-large-cased-finetuned-conll03-english")
        #tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
        #pipeline('ner', model=model, tokenizer=tokenizer)

    def _process(self, pg_data_name: str, pg_data=None, pg_parameters: dict = {}) -> Union[float, int, None]:
        try:
            #print(self._best_model)
            #print(pg_data)
            #print(pg_data_name)

            self._best_model = AutoModel.from_pretrained("bert-base-cased")

            if self._best_model:
                pass
            elif pgfile.isfileexist(os.path.join(self._parameter['save_dir'], f"{self._parameter['entity_name']}.pkl")):
                self._best_model = self.model_load(self._parameter['entity_name'])
                print(f"model {self._parameter['entity_name']} loaded")

            sequence = "Using a transformer network is simple"  # get input
            tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")  # tokenizer = AutoTokenizer.from_pretrained("sshleifer/tiny-gpt2")
            model_inputs = tokenizer(sequence, padding=True, truncation=True, return_tensors="pt")  # or do 3 three steps in one command
            output = self._best_model(model_inputs)  # make prediction
            print(output)

            exit(0)


            if isinstance(pg_data, pd.DataFrame):
                self._data[pg_data_name] = self._best_model.predict(pg_data.iloc[:, : -1])
            elif isinstance(pg_data, list):
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


class PGLearningHuggingFaceExt(PGLearningHuggingFace):
    def __init__(self, project_name: str = "learninghuggingfaceext", logging_enable: str = False):
        super(PGLearningHuggingFaceExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

        ### Specific Variables
        self._model_subtype = {}
        os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'


class PGLearningHuggingFaceSingleton(PGLearningHuggingFace):
    __instance = None

    @staticmethod
    def get_instance():
        if PGLearningHuggingFaceSingleton.__instance == None:
            PGLearningHuggingFaceSingleton()
        else:
            return PGLearningHuggingFaceSingleton.__instance

    def __init__(self, project_name: str = "learninghuggingfacesingleton", logging_enable: str = False):
        super(PGLearningHuggingFaceSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGLearningHuggingFaceSingleton.__instance = self


if __name__ == '__main__':
    a = pd.read_csv("/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/Learning/PyCaret/Test/output.csv")
    test = PGLearningHuggingFaceExt()
    #test._process2("test", "The Tomorrow War is the surprise of the season, a stuffed-to-the-gills battle between evil aliens and terrified humans.", {"pg_hf_pipeline": "sentiment-analysis"})
    #test._process2("test", pd.DataFrame([["The Tomorrow War is a movie that needed a surround sound, enormous screen experience - because its only value is in the spectacle."]], columns=['text']), {"pg_hf_pipeline": "sentiment-analysis"})

    test._process2("test", "In The Tomorrow War, the world is stunned when a group of time travelers arrive from the year 2051 to deliver an urgent message: Thirty years in the future mankind is losing a global war against a deadly alien species. The only hope for survival is for soldiers and civilians from the present to be transported to the future and join the fight. Among those recruited is high school teacher and family man Dan Forester (Chris Pratt).", {"pg_hf_pipeline": "ner"})
    print(test.data)
    exit(0)
    test = PGLearningHuggingFaceExt()
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








