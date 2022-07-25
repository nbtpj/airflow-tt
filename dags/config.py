scrapy_pattern_config = {
    'title_pattern': 'title::text',
    'abstract_pattern': 'h3.bm_Ak::text',
    'body_pattern': '.bm_FO ::text',
    'body_pattern_1': '.bm_Ev ::text',
    'source_pattern': 'div.bm_AS > p > a > span:nth-child(2)::text',
    'publisher_pattern': '.bm_As.bm_CB>img::attr("alt")',
    'tags_pattern': 'li.bm_Bv::text',
    'datetime_pattern': 'time::attr("datetime")',
    'place_pattern': '.bm_EW > span::text',
    'author_pattern': 'div.bm_FO > p.bm_W.bm_FR > strong::text',
    'next_urls_pattern': '.bm_Ah a::attr(href)'
}
scrapy_table = 'crawl_raw'

fact_table = 'songplays'

# WARNING!: MUST NOT change the order of these name
dimension_tables = ['users', 'songs', 'artists', 'time']
paging_tables = ['song_temp', 'session_temp']

db_connection_config = {
    'host': 'postgres',
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'port': 5432
}

paging_connection_config = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

author = {
    'owner': 'Minh Quang',
    'email': ['19020405@vnu.edu.vn'],
}
