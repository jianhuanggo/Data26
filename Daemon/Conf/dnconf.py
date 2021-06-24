#!/usr/bin/python

import re
import os
from types import SimpleNamespace
from Data.Utils import pgyaml
from Data.Utils import pgdirectory
from Data.Security import encrypt

NUM_OF_DELETE_TRY = 2
NUM_OF_TRY_UNTIL_FULL = 3


class DNConf:

    def __init__(self, env_prefix=None):
        self._parameters = {}
        self._parameters['ENV_VAR_PREFIX'] = env_prefix
        if not self._parameters['ENV_VAR_PREFIX']:
            _yaml_filename = pgdirectory.filedirectory(__file__) + "/" + self.__class__.__name__.lower() + ".yml"
            try:
                self._parameters = pgyaml.yaml_load(yaml_filename=_yaml_filename)
            except Exception as err:
                print(f"please make sure ENV_VAR_PREFIX is one of the entries in {_yaml_filename}")
                raise err

        for nm, val in os.environ.items():
            # match when regex == "start of string + 'list' + any whitespace characters + opening bracket"
            # same matching for tuple and dict
            if re.search('^(list\s*\(|tuple\s*\(|dict\s*\()', val):
                val = eval(val)
            elif re.match('^\d+$', val):
                val = int(val)
            if self._parameters['ENV_VAR_PREFIX'] in nm:
                self._parameters[nm] = val
            setattr(self, nm, val)

    @property
    def parameters(self):
        return self._parameters

    def print_all_parameters(self):
        if self._parameters:
            for key, value in self._parameters.items():
                print(f"{key}: {value}")
        else:
            print("the parameter list is empty")

    def setup_daemon(self):
        if "logger_file_filedir" not in dir(self):
            if f"{self._parameters['ENV_VAR_PREFIX']}DATA_HOME" in self._parameters:
                setattr(self, 'logger_file_filedir',
                        os.path.join(self._parameters[f"{self._parameters['ENV_VAR_PREFIX']}DATA_HOME"] + '/logs/daemon_log'))
            else:
                setattr(self, 'logger_file_filedir', os.path.join(pgdirectory.filedirectory(__file__)  + '/logs/daemon_log'))

        if "logger_file_when" not in dir(self):
            setattr(self, 'logger_file_when', 'h')

        if "logger_file_interval" not in dir(self):
            setattr(self, 'logger_file_interval', 1)

    def setup_db(self, db_name):
        return SimpleNamespace(host=getattr(self, f"{self._parameters['ENV_VAR_PREFIX']}{db_name.upper()}_HOST"),
                               username=getattr(self, f"{self._parameters['ENV_VAR_PREFIX']}{db_name.upper()}_USERNAME"),
                               userpass=getattr(self, f"{self._parameters['ENV_VAR_PREFIX']}{db_name.upper()}_PASS"),
                               port=getattr(self, f"{self._parameters['ENV_VAR_PREFIX']}{db_name.upper()}_PORT"),
                               db=getattr(self, f"{self._parameters['ENV_VAR_PREFIX']}{db_name.upper()}_DB"))

    def setup_secure_pass(self, db_system, post_fix):
        if db_system == 'rds':
            return SimpleNamespace(system_keypass=getattr(self, 'SCOOT_RDS_POST_KEYPASS' + post_fix),
                                   system_secret=getattr(self, 'SCOOT_RDS_POST_SECRET' + post_fix))
        elif db_system == 'redshift':
            return SimpleNamespace(system_keypass=getattr(self, 'SCOOT_REDSHIFT_KEYPASS' + post_fix),
                                   system_secret=getattr(self, 'SCOOT_REDSHIFT_SECRET' + post_fix))
        elif db_system == 'meta':
            return SimpleNamespace(system_keypass=getattr(self, 'SCOOT_META_POST_KEYPASS' + post_fix),
                                   system_secret=getattr(self, 'SCOOT_META_POST_SECRET' + post_fix))

    def setup_data_home(self):
        return SimpleNamespace(data_home=getattr(self, f"{self._parameters['ENV_VAR_PREFIX']}DATA_HOME"))

    def get_parameter(self, db_parameter):
        sp = encrypt.SecurityPass(db_system='meta', postfix='')
        meta_userpass = sp.gen_decrypt(entity='meta')

        if db_parameter == 'meta':
            return SimpleNamespace(system_host=getattr(self, 'SCOOT_META_POST_HOST'),
                                   system_username=getattr(self, 'SCOOT_META_POST_USERNAME'),
                                   system_userpass=meta_userpass,
                                   system_port=getattr(self, 'SCOOT_META_POST_PORT'),
                                   system_db=getattr(self, 'SCOOT_META_POST_DB'))
        elif db_parameter == 'redshift':
            # user_keypass = getattr(self, 'SCOOT_REDSHIFT_KEYPASS')
            # user_secret = getattr(self, 'SCOOT_REDSHIFT_SECRET')

            sp = encrypt.SecurityPass(db_system='redshift', postfix='')
            redshift_userpass = sp.gen_decrypt(entity='redshift')
            # print(self._redshift_userpass)

            return SimpleNamespace(system_host=getattr(self, 'SCOOT_REDSHIFT_HOST'),
                                   system_username=getattr(self, 'SCOOT_REDSHIFT_USERNAME'),
                                   # system_userpass=getattr(self, 'SCOOT_REDSHIFT_PASS'),
                                   system_userpass=redshift_userpass,
                                   system_port=getattr(self, 'SCOOT_REDSHIFT_PORT'),
                                   system_db=getattr(self, 'SCOOT_REDSHIFT_DB'))
        else:
            db_sys, db_postfix = db_parameter.split('_')
            if db_sys == 'rds':
                return SimpleNamespace(system_host=getattr(self, 'SCOOT_RDS_POST_HOST' + f"_{db_postfix}"),
                                       system_username=getattr(self, 'SCOOT_RDS_POST_USERNAME' + f"_{db_postfix}"),
                                       system_userpass=getattr(self, 'SCOOT_RDS_POST_PASS' + f"_{db_postfix}"),
                                       system_port=getattr(self, 'SCOOT_RDS_POST_PORT' + f"_{db_postfix}"),
                                       system_db=getattr(self, 'SCOOT_RDS_POST_DB') + f"_{db_postfix}")


def conf_with_db_conn():
    conf_db_conn = DNConf()
    conf_db_conn._parameters['SCOOT_REDSHIFT_URL'] = conf_db_conn.setup_redshift().redshift_url
    # print (conf_db_conn.parameters['SCOOT_REDSHIFT_URL'])
    # print(conf_db_conn.parameters['SCOOT_CLIENT_DBSHELL'])

    return conf_db_conn


if __name__ == '__main__':
    test = DNConf()
    test.print_all_parameters()
    test.setup_db("meta")

