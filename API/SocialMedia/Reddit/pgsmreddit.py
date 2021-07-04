import inspect
import requests
import pandas as pd
from itertools import repeat
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator, Generic, TypeVar, Optional, List, Union
from API.SocialMedia import pgsocialmediabase
from API.SocialMedia import pgsocialmediacommon
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation
from Data.Storage import pgstorage
from Data.Utils import pgfile, pgdirectory, pgyaml


#import warnings
#warnings.simplefilter(action='ignore', category=FutureWarning)

__version__ = "1.7"


class PGSMReddit(pgsocialmediabase.PGSocialMediaBase, pgsocialmediacommon.PGSocialMediaCommon):
    def __init__(self, project_name: str = "smreddit", logging_enable: str = False):
        super(PGSMReddit, self).__init__(project_name=project_name,
                                         object_short_name="PG_SM_RD",
                                         config_file_pathname=__file__.split('.')[0] + ".ini",
                                         logging_enable=logging_enable,
                                         config_file_type="ini")

        ### Common Variables
        self._name = "smreddit"
        self._api_base_url = 'https://oauth.reddit.com'
        self._data_inputs = {}
        self._headers = {}
        self._min_record_cnt_4_pred = 1
        self._column_heading = None
        self._data = {}
        self._pattern_match = {}
        self._parameter = {'storage_type': 'localdisk',  # default storage to save and load models
                           'storage_name': 'lr',  # name of storage
                           'dirpath': pgdirectory.get_filename_from_dirpath(__file__) + "/model",
                           'test_size': 0.2}  # if test_flag set to true, percentage of data will be used as testing dataset
        self.set_profile("default")
        self.clean_profile()
        self._get_client(self._parameter['client_id'],
                         self._parameter['secret_token'],
                         self._parameter['username'],
                         self._parameter['password']
                         )

    def _get_client(self, client_id: str, secret_token: str, username: str, password: str):
        try:
            # first create authentication object
            auth = requests.auth.HTTPBasicAuth(client_id, secret_token)
            # build login dictionary
            login = {'grant_type': 'password',
                     'username': username,
                     'password': password}
            # setup header info (incl description of API)
            self._headers['User-Agent'] = "MyBot/0.0.1"
            # send request for OAuth token
            res = requests.post(f'https://www.reddit.com/api/v1/access_token', auth=auth, data=login, headers=self._headers)
            # pull auth bearer token from response
            # add authorization to headers dictionary
            self._headers['Authorization'] = f"bearer {res.json()['access_token']}"

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    @property
    def data(self):
        return self._data

    def get_tasks(self, pg_data: Union[str, list], pg_parameters: dict = None) -> bool:

        """Prepares inputs into a standard format
        [(name(string), parameters(dict), data(varies), (name1(string), parameters1(dict), data1(varies), ...]

        Three type of inputs:
        1) if dataset is a string, then expects it to be a filename in yaml format
        2) if dataset is a list with no parameter, then expects to be the final format above
        3) if dataset is a list with parameter, then it will convert it to be the final format above

        Args:
            pg_data: data
            pg_parameters: parameters

        Returns:

        """

        try:
            if isinstance(pg_data, str):
                _file_ext = pg_data.split('.')[-1]
                if _file_ext in ("yml", "yaml"):
                    self._data_inputs = [(key, item) for key, item in pgyaml.yaml_load(yaml_filename=pg_data).items()]
                elif _file_ext in (".csv"):
                    _df = pd.read_csv(pg_data, sep=pg_parameters["separator"])
                    self._column_heading = _df.columns.values.tolist()
                    self._parameter = {**self._parameter, **pg_parameters}
                    self._data_inputs = [x for x in zip(repeat(pg_parameters['name']),
                                                        _df.values.tolist())] if pg_parameters else _df.values.tolist()
            elif isinstance(pg_data, list) and pg_parameters:
                self._data_inputs = list(zip(pg_data, pg_parameters))
            else:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, "pg_data needs to be a list or str")
                return False
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _process(self, pg_data_name: str, pg_data, pg_parameters: dict = {}):

        """Expects a format below and returns
        [(name(string), parameters(dict), data(varies), (name1(string), parameters1(dict), data1(varies), ...]

        Two type of inputs:
        1) The dataset is actual data, we can then just go ahead to process them
        2) The dataset is actual data is a set of parameters which we use to obtain additional data via API or web interface

        Args:
            pg_data_name: name of the dataset
            pg_data: dataset, either raw data usually in dataframe or a set of parameters
            pg_parameters: parameters


        Returns:

        """
        try:
            _url = f"{self._api_base_url}/r/{pg_data['subreddit']}/{pg_data['topic']}"
            df = pd.DataFrame()

            self._data[pg_data_name] = df

            # initialize parameters dictionary
            params = {'limit': 100}
            # iterate through several times to make sure we get all the data available
            print(_url)
            earliest = 0
            while True:
                # make request
                res = requests.get(_url, headers=self._headers, params=params)

                if len(res.json()['data']['children']) == 0 or len(self._data[pg_data_name]) + len(df) >= 3000:
                    self._data[pg_data_name] = pd.concat([self._data[pg_data_name], df], ignore_index=True)
                    self.save(df, f"{pg_data['subreddit']}_{pg_data['topic']}")
                    return self._data[pg_data_name]
                elif len(df) % 1000 == 0 and len(df) != 0:
                    self._data[pg_data_name] = pd.concat([self._data[pg_data_name], df], ignore_index=True)
                    self.save(df, f"{pg_data['subreddit']}_{pg_data['topic']}")
                    df = pd.DataFrame()
                else:
                    # from tqdm import tqdm
                    if len(df) % 100 == 0:
                        print(f"processed {len(self._data[pg_data_name]) + len(df)} reddits")

                # iterate through each thread recieved
                for thread in res.json()['data']['children']:
                    # add info to dataframe
                    df = df.append({
                        'id': thread['data']['name'],
                        'created_utc': int(thread['data']['created_utc']),
                        'subreddit': thread['data']['subreddit'],
                        'title': thread['data']['title'],
                        'selftext': thread['data']['selftext'],
                        'upvote_ratio': thread['data']['upvote_ratio'],
                        'ups': thread['data']['ups'],
                        'downs': thread['data']['downs'],
                        'score': thread['data']['score']
                    }, ignore_index=True)
                # get earliest ID
                earliest = df['id'].iloc[len(df) - 1]
                # add earliest ID to params
                params['after'] = earliest

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return None

    def process(self, name: str = None, *args: object, **kwargs: object) -> bool:
        try:
            if name:
                _item = self._data_inputs[name]
                self._data[name] = self._process(name, _item)
            else:
                for _index, _data in self._data_inputs.items():
                    _item = self._data_inputs[_index]
                    self._data[_index] = self._process(_index, _data)
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

    def load(self, filepath: str) -> bool:
        try:
            self._data = pd.concat([self._data, pd.read_csv(filepath, sep="|")], ignore_index=True)
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False


class PGSMRedditExt(PGSMReddit):
    def __init__(self, project_name: str = "smredditext", logging_enable: str = False):
        super(PGSMRedditExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

        ### Specific Variables


class PGSMRedditSingleton(PGSMReddit):
    __instance = None

    @staticmethod
    def get_instance():
        if PGSMRedditSingleton.__instance == None:
            PGSMRedditSingleton()
        else:
            return PGSMRedditSingleton.__instance

    def __init__(self, project_name: str = "smredditext", logging_enable: str = False):
        super(PGSMRedditSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGSMRedditSingleton.__instance = self


if __name__ == '__main__':

    test = PGSMReddit()

    #test._process(parameters={'subreddit': "investing", 'topic': "new"})
    test.get_tasks("/Users/jianhuang/opt/anaconda3/envs/Data26/Data26/API/SocialMedia/Input/input.yml")
    #client_id = "2jompVLsWJYi8g"
    #secret_token = "bVSeSlrdlqLmPbCW4xMLxrj1YhzuKA"
    #username = "pantherggg2222"
    #password = "28cqDApJH6"

    test.process()
    print(test._data)
    exit(0)
    """
    
    test1 = PGSMReddit()
    test1.set_profile("default")
    print(test1._parameter['save_dir'])
    for filename in pgdirectory.files_in_dir(test1._parameter['save_dir']):
        test1.load(f"{test1._parameter['save_dir']}/{filename}")
    print(test1.data)
    exit(0)
    """

    test.set_profile("cloud_storage")
    test.get_log("/Users/jianhuang/opt/anaconda3/envs/pg_learning/Learning/Reinforcement/Example/Training/Logs/PPO_4")
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

    print(test.data)








