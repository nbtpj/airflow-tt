from scrapy.crawler import CrawlerProcess
import config
from spiders import BaoMoiSpider, New
from db_plugin import *
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


class CrawlerWithSaver(BaoMoiSpider):

    @staticmethod
    def save(new: New):
        return save_new_to_raw_table(DATABASE_CONN, config.raw_table_name, new)

    @staticmethod
    def get_lastmod(loc):
        import datetime
        item = get_item(DATABASE_CONN, config.raw_table_name, loc)
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


default_args = {
    'owner': 'Minh Quang',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['19020405@vnuedu.vn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'scrapy_flow',
        default_args=default_args,
        description='Scrapy DAG',
        schedule_interval=timedelta(days=1),
) as dag:
    crawl_task = PythonOperator(task_id="crawler",
                                python_callable=crawl,
                                dag=dag)
    crawl_task
