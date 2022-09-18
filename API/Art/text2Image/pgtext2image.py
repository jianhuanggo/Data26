import inspect
import json

import torch
import os
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, List, Dict
from API.Art import pgartcommon, pgartbase
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgdirectory
from torch import autocast
from diffusers import StableDiffusionPipeline


_version__ = "1.8"


### Issues:

#   1) Initializing libiomp5.dylib, but found libomp.dylib already initialized https://github.com/pytorch/pytorch/issues/78490


class PGtext2Image(pgartbase.PGArtBase, pgartcommon.PGArtCommon):
    def __init__(self, project_name: str = "pgtext2image", logging_enable: str = False):

        super(PGtext2Image, self).__init__(project_name=project_name,
                                           object_short_name="PG_ART_T2I",
                                           config_file_pathname=__file__.split('.')[0] + ".ini",
                                           logging_enable=logging_enable,
                                           config_file_type="ini")

        ### Common Variables
        self.get_config(profile="default")
        self._name = "pgtext2image"
        self._data = {}
        self._pattern_match = {}
        self._parameter = {}

        ### Specific Variables
        # tiingo - python

        self._model_id = "CompVis/stable-diffusion-v1-4"
        # device = "cuda"
        self._device = "mps"
        self._data_inputs = {}
        self._get_func_map = {}
        self._get_func_parameter = {}
        self._set_func_map = {}
        self._set_func_parameter = {}

        os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'


    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data


    def get_config(self, profile: str = "default") -> bool:
        try:
            if profile in self._config.parameters["config_file"]:
                for key in self._config.parameters["config_file"][profile]:
                    for element in self._config.parameters["config_file"][profile][key].split(','):
                        self._set_func_map.get(key, "other")(element.split('-')[0], element.split('-')[1])

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    # single iterable - list/tuple, each item in the iterable will be assigned to one client
    def get_tasks(self):
        ### Need to auth with huggingface

        return list(self._data_inputs.keys())

    def _process(self, pg_item: Dict, *args: object, **kwargs: object) -> bool:
        try:

            pipe = StableDiffusionPipeline.from_pretrained(self._model_id, use_auth_token=True)
            pipe = pipe.to(self._device)

            pgdirectory.createdirectory(pg_item["dirpath"])
            with autocast("cuda"):
                image = pipe(pg_item["prompt"], guidance_scale=7.5)["sample"][0]
                image.save(os.path.join(pg_item["dirpath"], f"{pg_item['name']}.png"))

            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False

    def process(self, pg_items: Union[List, Tuple], *args: object, **kwargs: object) -> bool:
        try:
            _surrogate_key = 0
            for _item in pg_items:
                _item_json = json.loads(_item)
                _set_name = _item_json['name']
                for i in range(int(_item_json.get("occurrence", 0))):
                    _item_json["name"] = f"{_set_name}_{str(i)}"
                    self._process(_item_json)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


class PGtext2ImagelExt(PGtext2Image):
    def __init__(self, project_name: str = "pgtext2imageext", logging_enable: str = False):
        super(PGtext2ImagelExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGtext2ImageSingleton(PGtext2Image):

    __instance = None

    @staticmethod
    def get_instance():
        if PGtext2ImageSingleton.__instance == None:
            PGtext2ImageSingleton()
        else:
            return PGtext2ImageSingleton.__instance

    def __init__(self, project_name: str = "pgtext2imagesingleton", logging_enable: str = False):
        super(PGtext2ImageSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGtext2ImageSingleton.__instance = self


if __name__ == '__main__':

    test = PGtext2ImageSingleton()
    test.process(["{\"prompt\": \"a porsche racing a ferrari in a drag race\", "
                  "\"dirpath\": \"/Users/jianhuang/opt/anaconda3/envs/NewData2/Art/g2\", "
                  "\"name\": \"astronaut_rides_horse\", \"occurrence\": \"5\"}"])








