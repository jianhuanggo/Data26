import hashlib
import inspect
from datetime import datetime
from time import mktime
from collections import deque
from pprint import pprint
from typing import Callable, Union, Any, TypeVar, Tuple, Iterable, Generator
from newscatcher import Newscatcher
from newscatcher import urls as newscatcher_urls
from Data.Utils import pgparse
from API.News import pgnewsbase
from Meta import pgclassdefault, pggenericfunc
from Data.Utils import pgoperation


__version__ = "1.7"

#deque(map(self.do_someting, native_object))


class PGNewsCatcher(pgnewsbase.PGNewsBase, pgclassdefault.PGClassDefault):
    def __init__(self, project_name: str = "newscatcher", logging_enable: str = False):

        super(PGNewsCatcher, self).__init__(project_name=project_name,
                                            object_short_name="PG_NC",
                                            config_file_pathname=__file__.split('.')[0] + ".ini",
                                            logging_enable=logging_enable,
                                            config_file_type="ini")

        ### Common Variables
        self._name = "newscatcher"
        self._data = {}
        self._pattern_match = {}
        self._parameter = {}
        # supported country [US, GB, DE, FR, IN, RU, ES, BR, IT, CA, AU, NL, PL, NZ, PT, RO, UA, JP, AR, IR, IE, PH, IS,
        #                    ZA, AT, CL, HR, BG, HU, KR, SZ, AE, EG, VE, CO, SE, CZ, ZH, MT, AZ, GR, BE, LU, IL, LT, NI,
        #                    MY, TR, BM, NO, ME, SA, RS, BA]
        #
        # Supported topics: tech, news, business, science, finance, food, politics, economics, travel, entertainment,
        #                   music, sport, world
        #
        # urls(topic = None, language = None, country = None) - Get a list of all supported news websites given any combination of topic, language, country
        #
        # Supported languages: EL, IT, ZH, EN, RU, CS, RO, FR, JA, DE, PT, ES, AR, HE, UK, PL, NL, TR, VI, KO, TH, ID,
        #                      HR, DA, BG, NO, SK, FA, ET, SV, BN, GU, MK, PA, HU, SL, FI, LT, MR, HI
        #
        #


        ### Specific Variables
        self._country = None
        self._topic = None
        self._language = None

        ### setting default
        self.set_settings()

        #print(self._country)
        #print(self._topic)
        #print(self._language)

    @property
    def name(self):
        return self._name

    @property
    def country(self):
        return self._country

    @property
    def topic(self):
        return self._topic

    @property
    def language(self):
        return self._language

    @property
    def data(self):
        return self._data

    @property
    def process(self) -> Callable:
        return self._process

    def get_tasks(self):
        return self.get_urls()

    def set_settings(self, topic=None, country=None, language=None) -> bool:
        try:
            if "topic" in self._config.parameters["config_file"]['default']:
                self.set_param(**{'topic': self._config.parameters["config_file"]['default']['topic']})

            if "language" in self._config.parameters["config_file"]['default']:
                self.set_param(**{'language': self._config.parameters["config_file"]['default']['language']})

            if "country" in self._config.parameters["config_file"]['default']:
                self.set_param(**{'country': self._config.parameters["config_file"]['default']['country']})

            _topic = topic or self._topic or 'tech'
            _country = country or self._country or 'US'
            _language = language or self._language or 'EN'

            self.set_param(**{'country': _country, 'topic': _topic, 'language': _language})

            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def get_urls(self, topic=None, country=None, language=None) -> list:
        try:
            self.set_settings(topic=topic, country=country, language=language)
            return newscatcher_urls(topic=self.topic, country=self.country, language=self.language)
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return []

    #@pgoperation.pg_retry(3)
    def _process_item(self, link: str) -> bool:
        try:
            print(f"link: {link}")
            nc = Newscatcher(website=link).get_news()
            if nc is not None:
                for newsfeed in nc['articles']:
                    _tag = []
                    _tag += set(pgparse.pg_flatten_object(newsfeed['tags'])) if "tag" in newsfeed else []
                    _tag += set(pgparse.pg_flatten_object(newsfeed['authors'])) if "authors" in newsfeed else []
                    #_tag += [pgparse.pg_common_text_extract(newsfeed['summary'], {'prefix': "<p>", 'surfix': "</p>"})] if "summary" in newsfeed else []

                    _id = str(newsfeed['id']) if "id" in newsfeed else hashlib.sha256(newsfeed['title'].encode()).hexdigest()
                    self._data[_id] = {'id': _id,
                                       'title': newsfeed['title'] if "title" in newsfeed else "na",
                                       'timestamp': str(datetime.fromtimestamp(mktime(newsfeed['published_parsed']))),
                                       'tags': _tag,
                                       'authors': list(pgparse.pg_flatten_object(newsfeed['authors'])) if "authors" in newsfeed else "na",
                                       'summary': pgparse.pg_common_text_extract(newsfeed['summary'],
                                                                                 {'prefix': "<p>",
                                                                                  'surfix': "</p>"}) if "summary" in newsfeed else "na",
                                       'link': newsfeed['link'] if "link" in newsfeed else "na"
                                      }
            return True
        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
        return False

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        try:
            if isinstance(iterable, (tuple, list)):
                for _link in iterable:
                    self._process_item(_link)
            elif isinstance(iterable, str):
                self._process_item(iterable)
            else:
                pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name,
                                              f"Unexpected type for iterable, expecting tuple, list, str, got {type(iterable)}")

                return False
            return True

        except Exception as err:
            pggenericfunc.pg_error_logger(self._logger, inspect.currentframe().f_code.co_name, err)
            return False


class PGNewsCatcherExt(PGNewsCatcher):
    def __init__(self, project_name: str = "newscatcherext", logging_enable: str = False):
        super(PGNewsCatcherExt, self).__init__(project_name=project_name, logging_enable=logging_enable)

        if "topic" in self._config.parameters["config_file"]['advance']:
            self.set_param(**{'topic': self._config.parameters["config_file"]['advance']['topic'].split(',')})

        if "language" in self._config.parameters["config_file"]['advance']:
            self.set_param(**{'language': self._config.parameters["config_file"]['advance']['language'].split(',')})

        if "country" in self._config.parameters["config_file"]['advance']:
            self.set_param(**{'country': self._config.parameters["config_file"]['advance']['country'].split(',')})

        _country = self._country or None
        _topic = self._topic or None
        _language = self._language or None

        self.set_param(**{'country': _country, 'topic': _topic, 'language': _language})

    @staticmethod
    def city_state_parser(address: str) -> Tuple[str, str]:
        return address.split(',')[1].strip(), address.split(',')[2].split()[0].strip()

    def get_all_urls(self, topics: Iterable = None) -> list:
        _topics = topics or self._topic
        _url_list = []
        for _topic in _topics:
            _url_list += newscatcher_urls(topic=_topic, country=None, language=None)

        return _url_list

    def _process(self, iterable: Iterable, *args: object, **kwargs: object) -> bool:
        pass


class PGNewsCatcherSingleton(PGNewsCatcher):

    __instance = None

    @staticmethod
    def get_instance():
        if PGNewsCatcherSingleton.__instance == None:
            PGNewsCatcherSingleton()
        else:
            return PGNewsCatcherSingleton.__instance

    def __init__(self, project_name: str = "newscatchersingleton", logging_enable: str = False):
        super(PGNewsCatcherSingleton, self).__init__(project_name=project_name, logging_enable=logging_enable)
        PGNewsCatcherSingleton.__instance = self




if __name__ == '__main__':
    test = PGNewsCatcherExt()
    print(f"country: {test.country}")
    print(f"topic: {test.topic}")
    print(f"language: {test.language}")


    exit(0)
    test.process(0)
    for key, item in test._data.items():
        print(item)




    #print(PGRedfinExt().city_state_parser("10125 Barston Ct, Johns Creek, GA 30022")[0])

    english_economics_urls = newscatcher_urls(language='EN')
    print(english_economics_urls)
    nc = Newscatcher(website="facebook.com")
    results = nc.get_news()
    pprint(results['articles'][0].keys())
    pprint(results['articles'][0]['title'])
    pprint(results['articles'][0]['title_detail'])
    pprint(results['articles'][0]['link'])
    pprint(results['articles'][0]['links'])
    print(results.keys())
    print(results['articles'][0].keys())
    for item in results['articles']:
        print(item['title_detail'])
    data = []
    print(results['articles'][0]['tags'])
    print(results['articles'][0]['id'])
    print(results['articles'][0]['summary'])
    print(results['articles'][0]['authors'])
    print(results['articles'][0]['published_parsed'])
    print(datetime.fromtimestamp(mktime(results['articles'][0]['published_parsed'])))





    bb = [{'term': 'ML Applications', 'scheme1': ['label', {"hello": nc}]}, {'term': 'Networking & Traffic', 'scheme': None, 'label': None}, {'term': 'Video Engineering', 'scheme': None, 'label': None}]
    print(bb)

    for item in pgparse.pg_flatten_object(bb):
        print(item)

    print(results['articles'][0]['links'])
    ab = f"href='https://engineering.fb.com/2021/04/05/video-engineering/how-facebook-encodes-your-videos/'>"
    #pg_common_text_extract(results['articles'][0]['links'], {'prefix': "href=", 'surfix': ">"})
    pgparse.pg_common_text_extract(ab, {'prefix': "href=", 'surfix': ">"})


    print(pgparse.pg_common_text_extract(results['articles'][0]['summary'], {'prefix': "<p>", 'surfix': "</p>"}))

