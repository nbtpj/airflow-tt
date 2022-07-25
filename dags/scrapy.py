from scrapy.crawler import CrawlerProcess
import config
from spiders import BaoMoiSpider, New
from db_plugin import *
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import logging


def save_new_to_raw_table(new) -> bool:
    command_insert =''
    with DATABASE_CONN.cursor() as curs:
        command_insert = curs.mogrify(
            f'INSERT INTO {config.scrapy_table}(url, title, abstract, body, "source", publisher, tags, "date", lastmod, place, author) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
            (
                new['url'], new['title'],
                new['abstract'], new['body'],
                new['source'], new['publisher'],
                '; '.join(new['tags']) if new[
                    'tags'] else None, new['date'],
                new['lastmod'], new['place'],
                new['author']))
    return execute_or_log(DATABASE_CONN, command_insert)


def get_item(loc: str):
    if DATABASE_CONN is None:
        logging.warning(
            f'[{datetime.datetime.now().isoformat()}] No connection with database found, can not find your data!')
        return None
    else:
        try:
            data_from_db = {}
            with DATABASE_CONN.cursor() as curs:
                command_get = f"SELECT * FROM {config.scrapy_table} WHERE url=\'{loc}\'"
                data_from_db = curs.execute(command_get)
                if data_from_db is not None:
                    data_from_db = data_from_db.fetchone()[0]
                DATABASE_CONN.commit()
            return data_from_db
        except Exception as e:
            logging.error(f'[{datetime.datetime.now().isoformat()}] Can not fetch: {e.__str__()}')
            return None


class CrawlerWithSaver(BaoMoiSpider):
    @staticmethod
    def save(new: New):
        return save_new_to_raw_table(DATABASE_CONN, config.scrapy_table, new)

    @staticmethod
    def get_lastmod(loc):
        import datetime
        item = get_item(DATABASE_CONN, config.scrapy_table, loc)
        if item:
            try:
                date = datetime.datetime.fromisoformat(item['lastmod'])
                return date
            except Exception as e:
                pass
        return datetime.datetime.fromisoformat('2012-07-19T12:29+07:00')


def crawl():
    process = CrawlerProcess(
        settings={
            'CONCURRENT_REQUESTS_PER_DOMAIN': 3,
            'CONCURRENT_ITEMS': 3,
            'CONCURRENT_REQUESTS': 3,
            'DOWNLOAD_DELAY': 0.25
        })
    process.crawl(CrawlerWithSaver)
    process.start()
    return True


def create_table():
    with DATABASE_CONN.cursor() as curs:
        curs.execute(f'CREATE TABLE IF NOT EXISTS {config.scrapy_table} ('
                     f'url VARCHAR (400) UNIQUE NOT NULL,'
                     f'title VARCHAR (2000) NULL,'
                     f'abstract VARCHAR (5000) NULL,'
                     f'body VARCHAR (20000) NULL,'
                     f'"source" VARCHAR (400) NULL,'
                     f'publisher VARCHAR (600) NULL,'
                     f'tags VARCHAR (2000) NULL,'
                     f'"date" timestamptz NULL,'
                     f'lastmod timestamptz NULL,'
                     f'place VARCHAR (600) NULL,'
                     f'author VARCHAR (600) NULL'
                     f')')
        DATABASE_CONN.commit()


default_args = {
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    **config.author
}

with DAG(
        'scrapy_flow',
        default_args=default_args,
        description='Scrapy DAG',
        schedule_interval=timedelta(weeks=1),
) as dag:
    """
    Định nghĩa task đơn giản
    """
    init = PythonOperator(task_id="create_table",
                          python_callable=create_table,
                          dag=dag)

    crawl_task = PythonOperator(task_id="crawler",
                                python_callable=crawl,
                                dag=dag)
    init >> crawl_task
