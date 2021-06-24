import sys
import json
import inspect
from Meta import pggenericfunc, pgclassdefault
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow.keras.layers import Dense, Dropout, LSTM
from tensorflow.keras.models import Sequential, load_model, save_model
from Data.Utils import pgfile
from Data.Storage import pgstorage
from tensorflow import keras
from Data.StorageFormat import pgjson


class PGLearningCommon(pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str,
                       object_short_name: str,
                       config_file_pathname: str,
                       logging_enable: str,
                       config_file_type: str):

        super().__init__(project_name=project_name,
                         object_short_name=object_short_name,
                         config_file_pathname=config_file_pathname,
                         logging_enable=logging_enable,
                         config_file_type=config_file_type)

        self._storage = pgstorage.pgstorage("localdisk")
        self._storage_format = None


    def get_func(self, ):
        return

    def _model_save(self, model, entity_name: str, parameter: dict, _pg_action=None) -> bool:
        try:
            _h5_result = _pgconfig_result = True

            #print(parameter)


            #print(entity_name)

            #print(_pg_action)

            _dirpath = parameter['default_dirpath'] if parameter['storage_type'] == "localdisk" else parameter['intermediate_dirpath']

            #tf.saved_model.save(model, f"{parameters['default_dirpath']}/{entity}") <- regular format
            model.save(f"{_dirpath}/{entity_name}.h5") # <- h5 format

            ### h5 format
            with open(f"{_dirpath}/{entity_name}.pgconfig", 'w') as file_write:
                json.dump({'entity': f"{entity_name}", 'distribution_strategy': self._model_distribution_strategy,
                           'filepath': f"{parameter['default_dirpath']}/{entity_name}.h5",
                           'z_score': parameter['z_score'],
                           'a_score': str(parameter['a_score']),
                           'f_score': str(parameter['f_score']),
                           'correlation_score': str(parameter['correlation_score']),
                           }, file_write)

            if _pg_action and _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].storage_type != "localdisk":
                _storage_name, _storage_path = _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].get_storage_name_and_path(parameter['default_dirpath'])

                _h5_result = _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].save(data=f"{_dirpath}/{entity_name}.h5",
                                                                                                                 storage_format=None,
                                                                                                                 storage_parameter={**parameter, **{'mode': 'file', 'object_key': f"{_storage_path}/{entity_name}.h5"}})

                _pgconfig_result = _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].save(data=f"{_dirpath}/{entity_name}.pgconfig",
                                                                                                                       storage_format=None,
                                                                                                                       storage_parameter={**parameter, **{'mode': 'file', 'object_key': f"{_storage_path}/{entity_name}.pgconfig"}})

            if _h5_result and _pgconfig_result:
                print(f"model is successfully saved to {_dirpath}/{entity_name}")
                return True
            else:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, f"failed to save model to {parameter['storage_type']} ")
                return False
            '''
            regular format
            #   with open(f"{parameters['default_dirpath']}/{entity}.pgconfig", 'w') as file_write:
            #    json.dump({'entity': f"{entity}", 'distribution_strategy': distribution_strategy,
            #               'filepath': f"{parameters['default_dirpath']}/{entity}"}, file_write)
            
            '''

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)

        return False

    def _model_load(self, entity_name: str, parameter: dict = None, _pg_action=None):
        _h5_result = _pgconfig_result = True

        try:

            #print(entity_name)
            #print(_pg_action)
            #print(parameter)

            _dirpath = parameter['default_dirpath'] if parameter['storage_type'] == "localdisk" else parameter['intermediate_dirpath']
            if _pg_action and _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].storage_type != "localdisk":
                print(f"{parameter['default_dirpath']}/{entity_name}.h5")
                _h5_result = _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].load(location=f"{parameter['default_dirpath']}/{entity_name}.h5",
                                                                                                                 storage_format=None,
                                                                                                                 storage_parameter={**parameter, **{'mode': 'file',
                                                                                                                                                    'directory': parameter['intermediate_dirpath']}})

                _pgconfig_result = _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].load(location=f"{parameter['default_dirpath']}/{entity_name}.pgconfig",
                                                                                                                  storage_format=None,
                                                                                                                  storage_parameter={**parameter, **{'mode': 'file',
                                                                                                                                                     'directory': parameter['intermediate_dirpath']}})

                if _h5_result and _pgconfig_result:
                    with open(f"{parameter['intermediate_dirpath']}/{entity_name}.pgconfig", 'r') as read_json_file:
                        _pgconfig = json.load(read_json_file)
                    _pgconfig['filepath'] = f"{parameter['intermediate_dirpath']}/{entity_name}.h5"

                    with open(f"{parameter['intermediate_dirpath']}/{entity_name}.pgconfig", 'w') as write_json_file:
                        json.dump(_pgconfig, write_json_file)
                else:
                    return None

            else:
                _pgconfig_result = _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].load(location=f"{_dirpath}/{entity_name}.pgconfig")

                    #print(_pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].data)
            if _h5_result and _pgconfig_result:
                if _pg_action and _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].storage_type != "localdisk":
                    with open(f"{parameter['intermediate_dirpath']}/{entity_name}.pgconfig", 'r') as read_file:
                        _file_content = [read_file.read()]
                else:
                    _file_content = _pg_action[f"storage_{parameter['storage_type']}_{parameter['storage_name']}"].data

                if _file_content:
                    _parameter = json.loads(_file_content[0])
                    _distribution = self._model_distribution_strategy_map.get(_parameter['distribution_strategy'])
                    print(f"loading model from {_parameter['filepath']}")
                    print(_distribution)
                    with _distribution.scope():
                        loaded = tf.keras.models.load_model(_parameter['filepath']) # <- h5 format
                        #loaded = tf.saved_model.load(_parameter['filepath'])  <- regular format
                    return loaded

            return None

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return None






