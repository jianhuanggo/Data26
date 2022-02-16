import scrapy
from Data.Utils import pgyaml, pgdirectory
from WebScraping import pgwebscrapingcommon
from ..items import PgscrapyItem
from Meta import pgclassdefault


class PGScrapy(scrapy.Spider):
    name = 'pgsiders'

    def __init__(self, *args, **kwargs):
        super(PGScrapy, self).__init__(PGScrapy.name, **kwargs)
        self._config = {}
        self._switch = None

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
        self._config['parameters'] = pgyaml.yaml_load(yaml_filename=__file__.split('.')[0] + ".yml")
        self._switch = self._config['parameters'].get('content', 'None')
        print(self._switch)

        for url_item in self._config['parameters'].get('url_list', 'None'):
            yield scrapy.Request(f"{url_item}", self.pg_parser)

        #yield scrapy.Request(f"{self._config['parameters'].get('start_url', 'None')}", self.pg_parser)

        #if self._switch:
        #    yield scrapy.Request('http://quotes.toscrape.com/', self.parse)
        #else:
        #    yield scrapy.Request(f"{self._config['parameters'].get('start_url', 'None')}", self.parse)

        #yield scrapy.Request('http://www.example.com/1.html', self.parse)
        #yield scrapy.Request('http://www.example.com/2.html', self.parse)
        #yield scrapy.Request('http://www.example.com/3.html', self.parse)

    def pg_parser(self, response):
        _pgscrapy_items = PgscrapyItem()
        new_response = response
        """
        if self._switch:
            print("I'm here")
            for filename in pgdirectory.file_or_file_in_dir(self._switch):
                print(filename)
                with open(f"{self._switch}/{filename}") as file:
                    #print(type(file.read()))
                    new_response = response.replace(body=file.read())
                    break
        """
        for _item in new_response.xpath("//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'ab-address-margin-c1', ' ' ))]"):
            #print(_item.css('a').xpath("@href").css('a::attr(href)'))
            #print(_item.css('a::attr(href)').extract())
            #print(_item.css('span::text').extract())
            _pgscrapy_items['_address'] = ' '.join([x.strip() for x in _item.css('span::text').extract()])
            yield _pgscrapy_items

        #exit(0)
        #_next_page = response.css(".page a").xpath("@href").get()
        #print(_next_page)

        #if _next_page is not None:
        #    yield response.follow(f"https://www.realtor.com{_next_page}", callback=self.parse)



        #' '.join(response.xpath("//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'ab-address-margin-c1', ' ' ))]")[1].css('span::text').extract()).strip()



        #' '.join(response.xpath("//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'ab-address-margin-c1', ' ' ))]")[1].css('span::text').extract()).strip()
        #max([int(x) for x in response.css(".page a").css("a::text").extract()])
        #response.css(".page a").css("a").css("a[href*='pg']::text").extract()
        ##response.css(".page a").xpath("@href").extract()[-1]
        ##response.css("a").xpath("@href").extract()
        #print(f"Existing settings: {self.settings.attributes.keys()}")
            #print(response)
