import scrapy
from Data.Utils import pgyaml, pgdirectory
from WebScraping import pgwebscrapingcommon
from ..items import PgpaninispiderItem
from Meta import pgclassdefault


class PGPaniniSpider(scrapy.Spider):
    name = 'pgpaninispider'

    def __init__(self, *args, **kwargs):
        super(PGPaniniSpider, self).__init__(PGPaniniSpider.name, **kwargs)
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
        _pgpaninispider_items = PgpaninispiderItem()
        new_response = response

        _item_parser = {'list_of_players': "p.sub-title::text"}
        #print(item.css("p.group.inner.list-group-item-text").getall())



        #print(new_response.xpath("//*[@id='body-div']/section/div/div/div[1]/div[2]/div[2]/div[2]/div[1]/div/div/div[*]/a"))

        # new_response.xpath("//*[@id='body-div']/section/div/div/div[1]/div[2]/div[2]/div[2]/div[1]/div/div/div[*]/a").get():
        #print(new_response.xpath("//*[@id='body-div']/section/div/div/div[1]/div[2]/div[2]/div[2]/div[1]/div/div/div[1]/a").get())

        #for item in new_response.xpath("//*[@id='body-div']/section/div/div/div[1]/div[2]/div[2]/div[2]/div[1]/div/div/div[*]/a/text()").get():
        print(new_response.text)
        for item in new_response.xpath("//*[@id='body-div']/section/div/div/div[1]"):
            #_pgpaninispider_items['_description'] = item.css("p.sub-title::text").getall()
            print(item)
            print(item.css("p.sub-title::text").getall())
            #print(item.css("p.group.inner.list-group-item-text").getall())
            #div.product-name::text
            # p.group.inner.list-group-item-text::text
            #yield _pgpaninispider_items



            #sel = scrapy.Selector(text=item)
            #print(f"type : {type(item)}")
            #print(f"type : {type(sel)}")
            #print(sel.css('p.groupinnerlist-group-item-text ').extract())

            #"p class="group inner list-group-item-text"
            #print(item.xpath("//p[contains(@class, 'group')]").extract())
            #print(sel)
            #print(sel.css('p::text').get())
            #print(sel.xpath('a/div/div[2]/div[1]/p/text()').extract())


            #_pgpaninispider_items['_description'] = ' '.join([x.strip() for x in _item.css('span::text').extract()])
        """

        for _item in new_response.xpath("//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'ab-address-margin-c1', ' ' ))]"):
            #print(_item.css('a').xpath("@href").css('a::attr(href)'))
            #print(_item.css('a::attr(href)').extract())
            #print(_item.css('span::text').extract())
            _pgscrapy_items['_address'] = ' '.join([x.strip() for x in _item.css('span::text').extract()])
            yield _pgscrapy_items
        """




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
