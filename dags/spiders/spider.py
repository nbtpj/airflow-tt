from scrapy.spiders import SitemapSpider

import config
from .util import parse_response, parse_tags, New
from datetime import datetime


class BaoMoiSpider(SitemapSpider):
    name = "bao_moi"
    allowed_domains = ['baomoi.com']
    sitemap_urls = ['https://baomoi.com/robots.txt']
    start_urls = ['https://baomoi.com']
    sitemap_rules = [(r'/tag/', 'parse_tag'), (r'^((?!/tag/).)*$', 'parse_new'), ]
    pattern_config = config.pattern_config

    def parse_tag(self, response):
        return parse_tags(response, **self.pattern_config)

    @staticmethod
    def save(new: New) -> bool:
        raise NotImplementedError

    def parse_new(self, response):
        new = parse_response(response, **self.pattern_config)
        self.save(new)
        yield new

    @staticmethod
    def get_lastmod(loc) -> datetime:
        return datetime.fromisoformat('2012-07-19T12:29+07:00')

    def sitemap_filter(self, entries):
        for entry in entries:
            if 'lastmod' in entry:
                date_time = datetime.fromisoformat(entry['lastmod'])
                if date_time > self.get_lastmod(entry['loc']):
                    yield entry
            else:
                yield entry
