import scrapy
import inspect


from Data.Utils import pgyaml, pgfile, pgdirectory
#from WebScraping import pgwebscrapingcommon
from pprint import pprint
from ..items import PgscrapyItem, PgscrapyPastActivityItem, PgscrapyAuctions, PgscrapyGeneral
from scrapy.loader import ItemLoader, Item
from scrapy import Field, Item
from typing import List, Dict, Union
from Meta import pggenericfunc
#from Meta import pgclassdefault

"""
Update chromoedriver 
1) download appropriate driver from https://chromedriver.chromium.org/downloads
2) rename the file to /Users/jianhuang/chromedriver
3) give permission to run:  xattr -d com.apple.quarantine /Users/jianhuang/chromedriver
"""


class BaseClass(object):
    def __init__(self, classtype):
        self._type = classtype


def ClassFactory(name, argnames, BaseClass=BaseClass):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            # here, the argnames variable is the one passed to the
            # ClassFactory call
            if key not in argnames:
                raise TypeError("Argument %s not valid for %s"
                    % (key, self.__class__.__name__))
            setattr(self, key, value)
        BaseClass.__init__(self, name[:-len("Class")])
    newclass = type(name, (BaseClass,), {"__init__": __init__})
    return newclass


def apply_defaults(cls):
    defaults = {
        "end_in": scrapy.Field(),
        "day": scrapy.Field(),
        "hour": scrapy.Field(),
        "minute": scrapy.Field(),
        "second": scrapy.Field(),
        "icon": scrapy.Field(),
        "image_url": scrapy.Field(),
        "player_name": scrapy.Field(),
        "serial_number": scrapy.Field(),
        "card_set": scrapy.Field(),
        "current_bid_text": scrapy.Field(),
        "current_bid": scrapy.Field(),
        "buy_now_text": scrapy.Field(),
        "buy_now": scrapy.Field(),
        "crypto_icon": scrapy.Field(),
        "rarity": scrapy.Field()
    }
    for name, value in defaults.items():
        setattr(cls, name, value)
    return cls


@apply_defaults
class PgscrapyItem1(scrapy.Item):
    defaultsss = scrapy.Field()






class PGScrapy(scrapy.Spider):
    name = 'pgsiders'

    def __init__(self, *args, **kwargs):
        super(PGScrapy, self).__init__(PGScrapy.name, **kwargs)
        self._config = {}
        # self._switch = None
        if kwargs:
            self._pg_arguments = kwargs
        else:
            self._pg_arguments = None
        print(f"arguments: {self._pg_arguments}")



    #download_delay = 5.0
    # start_urls = ['https://www.realtor.com/soldhomeprices/Johns-Creek_GA']
    # Request.meta has dont_obey_robotstxt
    # https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
    # Implementing support for a new parser

    """
    custom_settings = {
        'SOME_SETTING': 'some value',
        'COMPRESSION_ENABLED': False,
    }
    """

    def start_requests(self):


        # _pgscrapy_items = ClassFactory("PgscrapyAuctions",
        #                                "end_in day hour minute second icon image_url player_name serial_number card_set current_bid_text current_bid buy_now_text buy_now crypto_icon rarity".split()
        #                               ,BaseClass=scrapy.Item)
        #
        #
        # print(_pgscrapy_items)
        #
        # print("aaaaaaa")
        #
        # exit(0)
        _metadata = {"panini_activity": PgscrapyPastActivityItem,
                     "panini_auction": PgscrapyAuctions}
        if self._pg_arguments and self._pg_arguments["input_yaml_file"]:
            self._config['parameters'] = pgyaml.yaml_load(yaml_filename=self._pg_arguments["input_yaml_file"])
        else:
            self._config['parameters'] = pgyaml.yaml_load(yaml_filename=__file__.split('.')[0] + ".yml")
        # self._switch = self._config['parameters'].get('content', 'None')
        # print(self._switch)
        print(self._config['parameters'])
        #raise SystemExit("ends here.....")

        if isinstance(self._config['parameters'].get('url_list', None), str):
            _pg_url_list = [self._config['parameters'].get('url_list', None)]
        else:
            _pg_url_list = self._config['parameters'].get('url_list', None)

        if isinstance(self._config['parameters'].get('url_name', None), str):
            _pg_url_name = [self._config['parameters'].get('url_name', None)]
        else:
            _pg_url_name = self._config['parameters'].get('url_name', None)

        _pg_downloader_intermediate_dir = self._config["parameters"].get("downloader_intermediate_dir", None)

        _pg_url_parse_type = self._config["parameters"].get("url_parse_type", None)

        if _pg_url_list is None:
            raise SystemExit("url list is not identified!!!")

        if _pg_url_name is None:
            raise SystemExit("url name is not identified!!!")

        if _pg_downloader_intermediate_dir is None:
            raise SystemExit("downloader intermediate dir is not identified!!!")


        #self._config["parameters"].get("downloader_intermediate_dir", "")

        for url_item in zip(_pg_url_list, _pg_url_name):
            ### common settings
            _req = scrapy.Request(f"{url_item[0]}", self.pg_parser)
            _req.pageitem = _metadata.get(url_item[1])
            _req.pagetag = url_item[1]

            #print(url_item)

            #_req.dirpath = "/home/pant/anaconda3/envs/pgscrapydownloader_1/data"
            #_req.filename = f"{pgfile.get_random_filename('job')}.html"

            ### specific settings
            if _pg_url_parse_type and _pg_url_parse_type.lower() == "page":
                _req.pg_url_parse_type = "page"
                _req.downloader_intermediate_dir = pgdirectory.get_random_directory2(_pg_downloader_intermediate_dir)
                _req.url_entity = self._config['parameters'].get('url_entity', None)
                _req.url_function = self._config['parameters'].get('url_function', None)
                _req.url_start_page = self._config['parameters'].get('url_start_page', 1)
                _req.url_end_page = self._config['parameters'].get('url_end_page', 5)
            else:
                _req.pg_url_parse_type = "scroll"
                _req.dirpath = _pg_downloader_intermediate_dir
                _req.filename = f"{pgfile.get_random_filename('pgdownloader')}.html"

            yield _req

    def pg_parser(self, response):
        def generate_item(fields: List) -> Item:
            item = Item()
            for f in fields:
                item.fields[f] = Field()
            return item

        try:

            _pg_item_fields = None

            print(response.content_json)
            #raise SystemExit("continue here.......")

            for _, pg_item in response.content_json[0].items():
                _pg_item_fields = [index for index, value in pg_item.items()]

            print(_pg_item_fields)

            item_inst = generate_item(_pg_item_fields)
            print(type(item_inst))

            #raise SystemExit("continue here.......")
            for _each_page in response.content_json:
                for _, pg_item in _each_page.items():
                    for index, value in pg_item.items():
                        item_inst[index] = value
                    yield item_inst

        except Exception as err:
            pggenericfunc.pg_error_logger(None, inspect.currentframe().f_code.co_name, err)
        return None


        #new_response = response

        #l = ItemLoader(item=PgscrapyAuctions(), response=new_response)  # first arg can be scrapy.Item object
        # l = ItemLoader(response=new_response)  # first arg can be scrapy.Item object
        # for _, pg_item in new_response.content_json.items():
        #     for index, value in pg_item.items():
        #         print(index, value)
        #         l.add_value(index, value)
        #         print(l.load_item())
        #     return l.load_item()


            #yield _pgscrapy_items


        #     for index, value in pg_item.items():
        #         _pgscrapy_items[index] = value
        #     yield _pgscrapy_items
        # for item in response:
        #     l.add_value(item, response[item])  # you can also use literal values
        # return l.load_item()


        #pprint(new_response.content_json)

        # for _, pg_item in new_response.content_json.items():
        #     for index, value in pg_item.items():
        #         _pgscrapy_items[index] = value
        #     yield _pgscrapy_items

        # exit(0)
        #
        # for _item in new_response.xpath("//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'ab-address-margin-c1', ' ' ))]"):
        #     #print(_item.css('a').xpath("@href").css('a::attr(href)'))
        #     #print(_item.css('a::attr(href)').extract())
        #     #print(_item.css('span::text').extract())
        #     _pgscrapy_items['_address'] = ' '.join([x.strip() for x in _item.css('span::text').extract()])
        #     yield _pgscrapy_items

