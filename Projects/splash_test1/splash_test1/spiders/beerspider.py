import scrapy
from scrapy_splash import SplashRequest


class BeerSpider(scrapy.Spider):
    name = 'beer'

    def start_requests(self):
        url = 'https://www.beerwulf.com/en-gb'

        yield SplashRequest(url=url, callback=self.parse)

    def parse(self, response):
        products = response.css('a.product.search-product.draught-product.notranslate.pack-product').get()
        for item in products:
            yield {'name': item.css('h4::text').get(),
                   'price': item.css('span.price::text').get()
                   }
