import scrapy
from datetime import datetime
from scrapy.item import Item, Field


class New(Item):
    url = Field()
    title = Field()
    abstract = Field()
    body = Field()
    source = Field()
    publisher = Field()
    tags = Field()
    date = Field()
    lastmod = Field()
    place = Field()
    author = Field()


def parse_tags(response: scrapy.http.Response,
               next_urls_pattern: str = '.bm_Ah a::attr(href)', **kwargs):
    urls = response.css(next_urls_pattern).getall()
    for request in response.follow_all(urls, parse_response):
        yield request


def parse_response(response: scrapy.http.Response,
                   title_pattern: str = 'title::text',
                   abstract_pattern: str = 'h3.bm_Ak::text',
                   body_pattern: str = '.bm_FO ::text',
                   body_pattern_1: str = '.bm_Ev ::text',
                   source_pattern: str = 'div.bm_AS > p > a > span:nth-child(2)::text',
                   publisher_pattern: str = '.bm_As.bm_CB>img::attr("alt")',
                   tags_pattern: str = 'li.bm_Bv>a::text',
                   datetime_pattern: str = 'time::attr("datetime")',
                   place_pattern: str = '.bm_EW > span::text',
                   author_pattern: str = 'div.bm_FO > p.bm_W.bm_FR > strong::text',
                   **kwargs) -> New:
    url = response.url
    title = response.css(title_pattern).get()
    abstract = response.css(abstract_pattern).get()
    body = '\n\n '.join(response.css(body_pattern).getall())
    if not body:
        body = '\n\n '.join(response.css(body_pattern_1).getall())
    source = response.css(source_pattern).get()
    publisher = response.css(publisher_pattern).get()
    tags = response.css(tags_pattern).getall()
    date = response.css(datetime_pattern).get()
    lastmod = datetime.today().isoformat()
    place = response.css(place_pattern).get()
    author = ' '.join(response.css(author_pattern).getall())

    d = {
        'url': url if url else None,
        'title': title if title else None,
        'abstract': abstract if abstract else None,
        'body': body if body else None,
        'source': source if source else None,
        'publisher': publisher if publisher else None,
        'tags': tags if tags else None,
        'date': date if date else None,
        'lastmod': lastmod if lastmod else None,
        'place': place if place else None,
        'author': author if author else None,
    }
    new = New(**d)
    return new
