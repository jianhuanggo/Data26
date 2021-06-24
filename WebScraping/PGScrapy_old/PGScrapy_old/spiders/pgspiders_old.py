import scrapy
from Data.Utils import pgyaml, pgdirectory
from ..items import PgscrapyItem


class PGScrapy(scrapy.Spider):
    name = 'pgsiders_old'
    conf = pgyaml.yaml_load(yaml_filename='/Users/jianhuang/opt/anaconda3/envs/Data20/Data20/WebScraping/conf.yml')
    _switch = conf.get('content', 'None')
    if _switch:
        start_urls = ['http://quotes.toscrape.com/']
    else:
        start_urls = ['https://www.realtor.com/soldhomeprices/Johns-Creek_GA']


    #download_delay = 5.0

    """
    custom_settings = {
        'SOME_SETTING': 'some value',
    }
    """

    def parse(self, response):
        _pgscrapy_items = PgscrapyItem()
        if PGScrapy._switch:
            print("I'm here")
            exit(0)
            for filename in pgdirectory.file_or_file_in_dir(PGScrapy._switch):
                print(filename)
                with open(filename) as file:
                    response.text = file.read()

        for _item in response.xpath("//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'ab-address-margin-c1', ' ' ))]"):
            #print(_item.css('a').xpath("@href").css('a::attr(href)'))
            #print(_item.css('a::attr(href)').extract())
            #print(_item.css('span::text').extract())
            _pgscrapy_items['_address'] = ' '.join([x.strip() for x in _item.css('span::text').extract()])
            yield _pgscrapy_items

        exit(0)
        _next_page = response.css(".page a").xpath("@href").get()
        print(_next_page)

        #if _next_page is not None:
        #    yield response.follow(f"https://www.realtor.com{_next_page}", callback=self.parse)



        #' '.join(response.xpath("//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'ab-address-margin-c1', ' ' ))]")[1].css('span::text').extract()).strip()



        #' '.join(response.xpath("//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'ab-address-margin-c1', ' ' ))]")[1].css('span::text').extract()).strip()
        #max([int(x) for x in response.css(".page a").css("a::text").extract()])
        #response.css(".page a").css("a").css("a[href*='pg']::text").extract()
        ##response.css(".page a").xpath("@href").extract()[-1]
        ##response.css("a").xpath("@href").extract()
        print(f"Existing settings: {self.settings.attributes.keys()}")
            #print(response)
