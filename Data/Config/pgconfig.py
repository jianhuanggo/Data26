#!/usr/bin/python

import re
import os
from Data.Utils import pgdirectory as dirFunc
from collections import namedtuple
from types import SimpleNamespace
from Data.Security import encrypt

"""
class Config():
    def __init__(self, default=False):
        for nm, val in os.environ.items():
            # match when regex == "start of string + 'list' + any whitespace characters + opening bracket"
            # same matching for tuple and dict
            if re.search('^(list\s*\(|tuple\s*\(|dict\s*\()', val):
                val=eval(val)
            elif re.match('^\d+$', val):
                val=int(val)
            setattr(self, nm, val)

        if default == True:
            if "logger_file_filedir" not in dir(self):
                setattr(self, 'logger_file_filedir', os.path.join(dirFunc.currentdirectory() + '/logs/daemon_log'))

            if "logger_file_when" not in dir(self):
                setattr(self, 'logger_file_when', 'h')

            if "logger_file_interval" not in dir(self):
                setattr(self, 'logger_file_interval', 1)

    def set_default(self):
        if "logger_file_filedir" not in dir(self):
            setattr(self, 'logger_file_filedir', os.path.join(dirFunc.currentdirectory() + '/logs/daemon_log'))

        if "logger_file_when" not in dir(self):
            setattr(self, 'logger_file_when', 'h')

        if "logger_file_interval" not in dir(self):
            setattr(self, 'logger_file_interval', 1)
            
"""
NUM_OF_DELETE_TRY = 2
NUM_OF_TRY_UNTIL_FULL = 3


class Config:

    def __init__(self, env_prefix="PG_"):
        self.parameters = {}
        for nm, val in os.environ.items():
            # match when regex == "start of string + 'list' + any whitespace characters + opening bracket"
            # same matching for tuple and dict
            if re.search('^(list\s*\(|tuple\s*\(|dict\s*\()', val):
                val=eval(val)
            elif re.match('^\d+$', val):
                val=int(val)
            if env_prefix in nm:
                self.parameters[nm] = val
            setattr(self, nm, val)

    def print_all_parameters(self):
        if self.parameters:
            for key, value in self.parameters.items():
                print(f"{key}: {value}")
        else:
            print("the parameter list is empty")

    def setup_daemon(self):
        if "logger_file_filedir" not in dir(self):
            if "SCOOT_DATA_HOME" in self.parameters:
                setattr(self, 'logger_file_filedir', os.path.join(self.parameters['SCOOT_DATA_HOME'] + '/logs/daemon_log'))
            else:
                setattr(self, 'logger_file_filedir', os.path.join(dirFunc.currentdirectory() + '/logs/daemon_log'))

        if "logger_file_when" not in dir(self):
            setattr(self, 'logger_file_when', 'h')

        if "logger_file_interval" not in dir(self):
            setattr(self, 'logger_file_interval', 1)

    def setup_db(self, db_name: str):
        return SimpleNamespace(host=getattr(self, f"{self.parameters['ENV_VAR_PREFIX']}{db_name.upper()}_HOST"),
                               username=getattr(self, f"{self.parameters['ENV_VAR_PREFIX']}{db_name.upper()}_USERNAME"),
                               userpass=getattr(self, f"{self.parameters['ENV_VAR_PREFIX']}{db_name.upper()}_PASS"),
                               port=getattr(self, f"{self.parameters['ENV_VAR_PREFIX']}{db_name.upper()}_PORT"),
                               db=getattr(self, f"{self.parameters['ENV_VAR_PREFIX']}{db_name.upper()}_DB"))


    def setup_rds(self):
        rds_post_host = getattr(self, 'SCOOT_RDS_POST_HOST')
        rds_post_username = getattr(self, 'SCOOT_RDS_POST_USERNAME')
        rds_post_userpass = getattr(self, 'SCOOT_RDS_POST_PASS')
        rds_post_port = getattr(self, 'SCOOT_RDS_POST_PORT')
        rds_post_db = getattr(self, 'SCOOT_RDS_POST_DB')
        #rds_post_url = getattr(self, 'SCOOT_RDS_POST_URL')

        Rds = namedtuple('Rds', ['post_host',
                                 'post_username',
                                 'post_userpass',
                                 'post_port',
                                 'post_db'])

        return Rds(post_host=rds_post_host,
                   post_username=rds_post_username,
                   post_userpass=rds_post_userpass,
                   post_port=rds_post_port,
                   post_db=rds_post_db)

    def setup_meta(self):

        """

        meta_post_host = getattr(self, 'SCOOT_META_POST_HOST')
        meta_post_username = getattr(self, 'SCOOT_META_POST_USERNAME')
        meta_post_userpass = getattr(self, 'SCOOT_META_POST_PASS')
        meta_post_port = getattr(self, 'SCOOT_META_POST_PORT')
        meta_post_db = getattr(self, 'SCOOT_META_POST_DB')
        #meta_post_url = getattr(self, 'SCOOT_META_POST_URL')



        return Meta(post_host=meta_post_host,
                    post_username=meta_post_username,
                    post_userpass=meta_post_userpass,
                    post_port=meta_post_port,
                    post_db=meta_post_db)

        """

        Meta = namedtuple('Meta', ['post_host',
                                   'post_username',
                                   'post_userpass',
                                   'post_port',
                                   'post_db'])

        parameters = self.get_parameter('meta')

        return Meta(post_host=parameters.system_host,
                    post_username=parameters.system_username,
                    post_userpass=parameters.system_userpass,
                    post_port=parameters.system_port,
                    post_db=parameters.system_db)



    def setup_redshift(self):
        redshift_host = getattr(self, 'SCOOT_REDSHIFT_HOST')
        redshift_username = getattr(self, 'SCOOT_REDSHIFT_USERNAME')
        redshift_userpass = getattr(self, 'SCOOT_REDSHIFT_PASS')
        redshift_port = getattr(self,'SCOOT_REDSHIFT_PORT')
        redshift_db = getattr(self,'SCOOT_REDSHIFT_DB')

        Redshift = namedtuple('Redshift', ['redshift_host',
                                           'redshift_username',
                                           'redshift_userpass',
                                           'redshift_port',
                                           'redshift_db',
                                           'redshift_url'])

        return Redshift(redshift_host=redshift_host,
                        redshift_username=redshift_username,
                        redshift_userpass=redshift_userpass,
                        redshift_port=redshift_port,
                        redshift_db=redshift_db,
                        redshift_url=f"postgresql://{redshift_username}:{redshift_userpass}@{redshift_host}:"
                                     f"{redshift_port}/{redshift_db}")

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
        return SimpleNamespace(data_home=getattr(self, 'SCOOT_DATA_HOME'))

    def get_parameter(self, db_parameter):
        sp = encrypt.SecurityPass(db_system='meta', postfix='')
        meta_userpass = sp.gen_decrypt(entity='meta')

        if db_parameter == 'meta':
            return SimpleNamespace(system_host=getattr(self, 'SCOOT_META_POST_HOST'),
                                   system_username=getattr(self, 'SCOOT_META_POST_USERNAME'),
                                   system_userpass=meta_userpass,
                                   system_port = getattr(self, 'SCOOT_META_POST_PORT'),
                                   system_db = getattr(self, 'SCOOT_META_POST_DB'))
        elif db_parameter == 'redshift':
            #user_keypass = getattr(self, 'SCOOT_REDSHIFT_KEYPASS')
            #user_secret = getattr(self, 'SCOOT_REDSHIFT_SECRET')

            sp = encrypt.SecurityPass(db_system='redshift', postfix='')
            redshift_userpass = sp.gen_decrypt(entity='redshift')
            #print(self._redshift_userpass)

            return SimpleNamespace(system_host=getattr(self, 'SCOOT_REDSHIFT_HOST'),
                                   system_username=getattr(self, 'SCOOT_REDSHIFT_USERNAME'),
                                   #system_userpass=getattr(self, 'SCOOT_REDSHIFT_PASS'),
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
    conf_db_conn = Config()
    conf_db_conn.parameters['SCOOT_REDSHIFT_URL'] = conf_db_conn.setup_redshift().redshift_url
    #print (conf_db_conn.parameters['SCOOT_REDSHIFT_URL'])
    #print(conf_db_conn.parameters['SCOOT_CLIENT_DBSHELL'])

    return conf_db_conn


if __name__ == '__main__':

    conn = conf_with_db_conn()

    #if "myappl" not in dir(conftest):
    #    print("This is not there")
