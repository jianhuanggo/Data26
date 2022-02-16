# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PgscrapyItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class PgscrapyPastActivityItem(scrapy.Item):
    # define the fields for your item here like:
    image_url = scrapy.Field()
    player_name = scrapy.Field()
    serial_number = scrapy.Field()
    price = scrapy.Field()
    card_set = scrapy.Field()
    buyer_name = scrapy.Field()
    seller_name = scrapy.Field()
    date_time = scrapy.Field()


class PgscrapyAuctions(scrapy.Item):
    # define the fields for your item here like:
    end_in = scrapy.Field()
    day = scrapy.Field()
    hour = scrapy.Field()
    minute = scrapy.Field()
    second = scrapy.Field()
    icon = scrapy.Field()
    image_url = scrapy.Field()
    player_name = scrapy.Field()
    serial_number = scrapy.Field()
    card_set = scrapy.Field()
    current_bid_text = scrapy.Field()
    current_bid = scrapy.Field()
    buy_now_text = scrapy.Field()
    buy_now = scrapy.Field()
    crypto_icon = scrapy.Field()
    rarity = scrapy.Field()


class PgscrapyGeneral(scrapy.Item):
    pass



