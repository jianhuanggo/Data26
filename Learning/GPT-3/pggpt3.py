import os
import re
import sys
import inspect
import openai
import textwrap
from time import time, sleep
from pprint import pprint
from uuid import uuid4
from random import seed, choice
import numpy as np
import pandas as pd
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List
from Meta.pggenericfunc import check_args
from Meta import pgclassdefault, pggenericfunc
from Learning import pglearningcommon1, pglearningbase, pglearning
from Data.Utils import pgfile, pgdirectory


__version__ = "1.9"

### https://www.youtube.com/watch?v=u9hQmcgh_gE

class PGLearningGPT3(pglearningbase.PGLearningBase, pglearningcommon1.PGLearningCommon):
    def __init__(self, project_name: str = "learninggpt3", logging_enable: str = False):
        super(PGLearningGPT3, self).__init__(project_name=project_name,
                                             object_short_name="PG_LRN_GPT3",
                                             config_file_pathname=__file__.split('.')[0] + ".ini",
                                             logging_enable=logging_enable,
                                             config_file_type="ini")

        ### Common Variables
        self._openai_api_key = self.get_key()
        self._name = "learninggpt3"
        self._model_type = "transformer"
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

    def data_preprocessing(self, pgdataset: pd.DataFrame, pgscaler, parameter: dict = None) -> Union[np.ndarray, None]:
        """
        The beauty of random forest is that it doesn't need preprocessing
        """
        pass

    def get_tasks(self, pgdataset: Any, parameters: dict = None) -> bool:

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

    def _process(self, pgdataset, parameters: dict, *args, **kwargs) -> Union[float, int, None]:
        """Returns True if file(s) are successfully persisted in appropriate s3 location.

        Args:
            pgdataset: Machine Learning Model instance
            parameters: An unique Identifier used to form the model name

        Returns:
            The return value. True for success, False otherwise.

        Raw text data is tokenized (converted into numerical IDs that map that word to a vector representation of the same word)
        Token IDs are fed into the sentiment model
        A set of values are output, each value represents a class, and the value represents probability of that being the correct sentiment class, from zero (definitely not) to one (definitely yes).
        The argmax of this output array is taken to give us our winning sentiment classification.

        """

        check_args(inspect.currentframe().f_code.co_name,
                   {'pgdataset': pgdataset, 'parameters': parameters, }, False)

        try:
            seed()
            pass

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

    def open_file(self, filepath: str) -> str:
        with open(filepath, 'r', encoding = "utf-8") as infile:
            return infile.read()

    def save_file(self, filepath: str, content: str) -> bool:
        try:
            with open(filepath, "w", encoding="utf-8") as outfile:
                outfile.write(content)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def gpt3_completion(self,
                        prompt,
                        engine="text-davinci-002",
                        temp=0.7,
                        top_p=1.0,
                        tokens=1000,
                        freq_pen=0.0,
                        pres_pen=0.0,
                        stop=["asdfasdf", "asdasdf"]):
        max_retry = 5
        retry = 0
        # force it to fix any unicode errors
        prompt = prompt.encode(encoding="ASCII", errors="ignore").decode()
        while True:
            try:
                response = openai.Completion.create (
                    engine=engine,
                    prompt=prompt,
                    temperature=temp,
                    max_tokens=tokens,
                    top_p=top_p,
                    frequency_penalty=freq_pen,
                    presence_penalty=pres_pen,
                    stop=stop)
                text = response["choice"][0]["text"].strip()
                #text = re.sub("\s+", " ", text)
                filename = f"{str(time())}_gpt3.txt"
                self.save_file(f"gpt3_logs/{filename}", prompt + "\n\n==============\n\n" + text)
                return text
            except Exception as err:
                retry += 1
                if retry >= max_retry:
                    pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, f"GPT3 error: {err}")
                print(f"Error communicating with OpenAI {err}")
                sleep(1)

    def improve_outline(self, request, outline, prompt_improvement="prompt_improve_outline.txt"):
        try:
            _prompt = self.open_file(prompt_improvement).replace("<<REQUEST>>", request).replace("<<OUTLINE>>", outline)
            return "1. " + self.gpt3_completion(_prompt)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def neural_recall(self, request, section, prompt_improvement="prompt_section_research.txt"):
        try:
            _prompt = self.open_file(prompt_improvement).replace("<<REQUEST>>", request).replace("<<SECTION>>", section)
            return self.gpt3_completion(_prompt)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def improve_prose(self, research, prose, prompt_improvement="prompt_improve_prose.txt"):
        try:
            _prompt = self.open_file(prompt_improvement).replace("<<RESEARCH>>", research).replace("<<PROSE>>", prose)
            return self.gpt3_completion(_prompt)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    @staticmethod
    def get_key():
        return "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"


class PGLearningGPT3Ext(PGLearningGPT3):
    def __init__(self, project_name: str = "learninggpt3ext", logging_enable: str = False):
        super(PGLearningGPT3Ext, self).__init__(project_name=project_name, logging_enable=logging_enable)


class PGLearningGPT3Singleton(PGLearningGPT3):
    __instance = None

    @staticmethod
    def get_instance():
        if PGLearningGPT3Singleton.__instance == None:
            PGLearningGPT3Singleton()
        else:
            return PGLearningGPT3Singleton.__instance

    def __init__(self, project_name: str = "learninggpt3singleton", logging_enable: str = False):
        super(PGLearningGPT3Singleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGLearningGPT3Singleton.__instance = self


if __name__ == '__main__':
    seed()
    test = PGLearningGPT3Ext()









