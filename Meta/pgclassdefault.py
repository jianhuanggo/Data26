import sys
import uuid
import inspect
import configparser
from pprint import pprint
from Meta import pggenericfunc
from Data.Utils import pgyaml as pgyl
import Data.Config.pgconfig as pgconfig
import Data.Logging.pglogging as pglogging
from pydantic import BaseModel, validator, ValidationError


__version__ = "1.5"


LOGGING_LEVEL = pglogging.logging.INFO
pglogging.logging.basicConfig(level=LOGGING_LEVEL)


class PGClassDefault:
    def __init__(self, project_name="generic" + str(uuid.uuid4().hex[:6]),
                 object_short_name="GEN",
                 config_file_pathname=__file__.split('.')[0] + ".yml",
                 logging_enable=False,
                 config_file_type="yml"):

        self._project_name = project_name
        self._object_short_name = object_short_name
        try:
            self._config = pgconfig.Config(self._object_short_name)
            if config_file_type == "yml":
                self._config.parameters = pgyl.yaml_load(yaml_filename=config_file_pathname)
            else:
                self._config.parameters["config_file"] = configparser.ConfigParser()
                #print(config_file_pathname)
                try:
                    self._config.parameters["config_file"].read(config_file_pathname)
                except Exception as err:
                    print(err)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            sys.exit(99)

        try:
            if logging_enable:
                self._logger = pglogging.Logging(self._config, logging_level=LOGGING_LEVEL,
                                                 subject=f" {self._project_name} logger").getLogger(self._project_name)
            else:
                self._logger = None

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            sys.exit(99)

        self._parameter = {}
        self._setting_doc = None

    @property
    def config(self):
        return self._config

    @property
    def parameter(self):
        return self._parameter

    def set_param(self, *args, **kwargs):
        """
        Args:
            args (list): The first parameter.
            kwargs (dict): The second parameter.

        Returns:
            None

        """
        if args:
            raise Exception(f"ambiguous argument(s) {args}")
        #print(vars(self))
        #print(self.__init__.__code__.co_varnames)
        #print(f"here is MP: {kwargs}")

        for pkey, pval in kwargs.items():
            if f"_{pkey}" in vars(self) and f"_{pkey}" != "_pg_private":
                #print(f"key {pkey} exists")
                setattr(self, f"_{pkey}", pval)
            else:
                print(self._setting_doc)
                raise Exception(f"parameter {pkey} does not exist")

    def set_profile(self, profile_name: str) -> bool:
        try:
            if profile_name in self._config.parameters['config_file'].sections():
                for key in self._config.parameters['config_file'][profile_name]:
                    self._parameter = {**self._parameter, **{key: self._config.parameters["config_file"][profile_name][key]}}
                return True
            else:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                              f"profile not found, valid profile: {self._config.parameters['config_file'].sections()}")
                sys.exit(99)

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            sys.exit(99)




class PGDataDefault(BaseModel):
    pass