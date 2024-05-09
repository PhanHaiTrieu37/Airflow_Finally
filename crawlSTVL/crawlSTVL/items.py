# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlstvlItem(scrapy.Item):
    link = scrapy.Field()
    JobName = scrapy.Field()
    CompanyName = scrapy.Field()
    Deadline = scrapy.Field()
    Salary = scrapy.Field()
    Quantity = scrapy.Field()
    pass
