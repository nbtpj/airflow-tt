import datetime
import json
import logging
from typing import List, Any, Dict
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from db_plugin import DATABASE_CONN, execute_or_log
import config


def scan(dir, pattern):
    from os import walk
    from os.path import isfile, join
    import re
    log_dirs = []
    for root, dirs, files in walk(dir, topdown=True):
        for file_name in files:
            path = join(root, file_name)
            if bool(re.search(pattern, path)):
                log_dirs.append(path)
    return log_dirs


def collect(dir):
    with open(dir, encoding='utf8') as f:
        for line in f:
            item = json.loads(line)
            item['source'] = dir
            yield item


def insert2users(record):
    command_insert = ''
    with DATABASE_CONN.cursor() as curs:
        command_insert = curs.mogrify(
            f'INSERT INTO {config.dimension_tables[0]}(user_id, first_name, last_name, gender, "level") VALUES(%s, %s, %s, %s, %s);',
            (
                record['userId'] if 'userId' in record else None,
                record['firstName'] if 'firstName' in record else None,
                record['lastName'] if 'lastName' in record else None,
                record['gender'] if 'gender' in record else None,
                record['level'] if 'level' in record else None,
            ))
    return execute_or_log(DATABASE_CONN, command_insert)


def insert2songplays(record):
    command_insert = ''
    with DATABASE_CONN.cursor() as curs:
        command_insert = curs.mogrify(
            f'INSERT INTO {config.fact_table}(start_time, user_id, "level", song_id, artist_id, session_id, location, user_agent) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);',
            (
                str(pd.to_datetime(record['ts'], utc=True, unit='ms')) if 'ts' in record else None,
                record['userId'] if 'userId' in record else None,
                record['level'] if 'level' in record else None,
                record['song_id'] if 'song_id' in record else None,
                record['artist_id'] if 'artist_id' in record else None,
                record['sessionId'] if 'sessionId' in record else None,
                record['location'] if 'location' in record else None,
                record['userAgent'] if 'userAgent' in record else None,
            ))
    return execute_or_log(DATABASE_CONN, command_insert)


def insert2songs(record):
    command_insert = ''
    with DATABASE_CONN.cursor() as curs:
        command_insert = curs.mogrify(
            f'INSERT INTO {config.dimension_tables[1]}(song_id, title, artist_id, "year", duration) VALUES(%s, %s, %s, %s, %s);',
            (
                record['song_id'] if 'song_id' in record else None,
                record['title'] if 'title' in record else None,
                record['artist_id'] if 'artist_id' in record else None,
                record['year'] if 'year' in record else None,
                record['duration'] if 'duration' in record else None,
            ))
    return execute_or_log(DATABASE_CONN, command_insert)


def insert2artists(record):
    command_insert = ''
    with DATABASE_CONN.cursor() as curs:
        command_insert = curs.mogrify(
            f'INSERT INTO {config.dimension_tables[2]}(artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s);',
            (
                record['artist_id'] if 'artist_id' in record else None,
                record['artist_name'] if 'artist_name' in record else None,
                record['artist_location'] if 'artist_location' in record else None,
                record['artist_latitude'] if 'artist_latitude' in record else None,
                record['artist_longitude'] if 'artist_longitude' in record else None,
            ))
    return execute_or_log(DATABASE_CONN, command_insert)


def insert2time(record):
    command_insert = ''
    timestamp = None
    try:
        timestamp = pd.to_datetime(record['ts'], utc=True, unit='ms') if 'ts' in record else None
    except Exception as e:
        logging.warning(f'can not parse time: Error {str(e)}')
        pass
    if timestamp is not None:
        with DATABASE_CONN.cursor() as curs:
            command_insert = curs.mogrify(
                f'INSERT INTO {config.dimension_tables[-1]}(start_time,"hour" ,"day" ,"week" ,"month" ,"year" ,"weekda") VALUES(%s, %s, %s, %s, %s, %s, %s);',
                (
                    str(timestamp),
                    str(timestamp.hour),
                    str(timestamp.daysinmonth),
                    str(timestamp.weekofyear),
                    str(timestamp.month),
                    str(timestamp.year),
                    str(timestamp.weekday())
                ))
    return execute_or_log(DATABASE_CONN, command_insert)


@dag(
    default_args=config.author,
    # the period that a dag should last
    schedule_interval=None,
    # the date that task start for the first time
    start_date=pendulum.datetime(2022, 7, 25, tz='UTC'),
    # should the dag auto run non-triggered DAG the times they should be from the start_date
    catchup=False,
    # tags for quick searching
    tags=['data collector', 'mart builder']
)
def data_mart_builder():
    """
    ### Data Mart builder Documentation
    This is a basic collector dag, which carry on collecting data from log files, preprocess then fill them into data base
    :return:
    """

    @task()
    def init():
        """
        #### Set up data warehouse

        :return:
        """
        with DATABASE_CONN.cursor() as curs:
            curs.execute(f'CREATE TABLE IF NOT EXISTS {config.fact_table} ('
                         f'songplay_id SERIAL PRIMARY KEY, '
                         f'user_id INT, '
                         f'"level" VARCHAR (15), '
                         f'song_id VARCHAR (20), '
                         f'artist_id VARCHAR (20), '
                         f'session_id INT, '
                         f'location TEXT, '
                         f'user_agent TEXT, '
                         f'start_time TIMESTAMP'
                         f')')
            curs.execute(f'CREATE TABLE IF NOT EXISTS {config.dimension_tables[0]} ('
                         f'user_id INT PRIMARY KEY, '
                         f'first_name TEXT, '
                         f'last_name TEXT, '
                         f'gender VARCHAR (10), '
                         f'"level" VARCHAR (15)'
                         f')')
            curs.execute(f'CREATE TABLE IF NOT EXISTS {config.dimension_tables[1]} ('
                         f'song_id VARCHAR (20) PRIMARY KEY, '
                         f'title TEXT, '
                         f'artist_id  VARCHAR (20), '
                         f'"year" INT, '
                         f'duration FLOAT'
                         f')')
            curs.execute(f'CREATE TABLE IF NOT EXISTS {config.dimension_tables[2]} ('
                         f'artist_id VARCHAR (20) PRIMARY KEY, '
                         f'"name" TEXT, '
                         f'location TEXT, '
                         f'latitude  FLOAT, '
                         f'longitude FLOAT'
                         f')')
            curs.execute(f'CREATE TABLE IF NOT EXISTS {config.dimension_tables[-1]} ('
                         f'start_time TIMESTAMP PRIMARY KEY, '
                         f'"hour" INT, '
                         f'"day" INT, '
                         f'"week"  INT, '
                         f'"month" INT, '
                         f'"year" INT,'
                         f'"weekda" VARCHAR (10)'
                         f')')
            """
            Setup paging data table
            """
            # curs.execute(f'CREATE TABLE IF NOT EXISTS {config.paging_tables[0]} ('
            #              f'num_songs INT (20), '
            #              f'artist_id VARCHAR (20), '
            #              f'artist_latitude FLOAT, '
            #              f'artist_longitude  FLOAT, '
            #              f'artist_location TEXT, '
            #              f'artist_name TEXT, '
            #              f'song_id VARCHAR (20), '
            #              f'title TEXT, '
            #              f'duration FLOAT, '
            #              f'"year" INT, '
            #              f')')
            # curs.execute(f'CREATE TABLE IF NOT EXISTS {config.paging_tables[1]} ('
            #              f'artist TEXT, '
            #              f'auth TEXT, '
            #              f'firstName TEXT, '
            #              f'lastName TEXT, '
            #              f'gender VARCHAR (10), '
            #              f'itemInSession INT, '
            #              f'"length" FLOAT, '
            #              f'"level" VARCHAR (15), '
            #              f'location TEXT, '
            #              f'ts INT (30), '
            #              f'userId INT, '
            #              f'userAgent TEXT'
            #              f'sessionId INT, '
            #              f'song TEXT'
            #              f'method VARCHAR  (10)'
            #              f'page TEXT'
            #              f'registration FLOAT'
            #              f'status INT (5)'
            #              f')')

            DATABASE_CONN.commit()

        return True

    @task()
    def scan_log(init_stage: bool, dir: str = '/opt/airflow/data/log_data', pattern: str = 'events.json') -> List[str]:
        """
        #### Scan log files
        This task contains:
            * Scanning pre-defined directory for log files
        :param dir: directory (from current DAG directory)
        :param pattern: log file directory pattern, use for matching
        :return:
        """
        if 'scan_log__dir' in get_current_context():
            dir = get_current_context()['scan_log__dir']

        return scan(dir, pattern)

    @task()
    def scan_song(init_stage: bool, dir: str = '/opt/airflow/data/song_data', pattern: str = '.json') -> List[str]:
        """
        #### Scan song files
        This task contains:
            * Scanning pre-defined directory for song files
        :param dir: directory (from current DAG directory)
        :param pattern: log file directory pattern, use for matching
        :return:
        """
        if 'scan_song__dir' in get_current_context():
            dir = get_current_context()['scan_song__dir']
        return scan(dir, pattern)

    @task()
    def collect_log(log_dir: str) -> List[Dict]:
        """
        #### Extract data from log files then store in data warehouse (user)
        Perform a single mapping from log file into row in data warehouse table
        :param log_dir:
        :return: List of parsable items
        """
        rs = []
        for record in collect(log_dir):
            insert2users(record)
            insert2time(record)
            rs.append(record)
        return rs

    @task()
    def collect_song(song_dir: str) -> Any:
        """
        #### Extract  data from song files then store in warehouse (songs & artists)
        Perform a single mapping from log file into row in data warehouse table
        :param song_dir:
        :return: List of parsable items
        """
        for record in collect(song_dir):
            insert2songs(record)
            insert2artists(record)
            return record
        return {}

    @task()
    def collectSongPlay(previous_state, l_records):
        """
        #### Extract  data from song files then store in warehouse (songplays)
        :param previous_state:
        :param l_records:
        :return:
        """
        for records in l_records:
            for record in records:
                data_from_db = {}
                try:
                    with DATABASE_CONN.cursor() as curs:
                        command_get = curs.mogrify(
                            f'SELECT * FROM {config.dimension_tables[1]} WHERE title = %s;',
                            (record['song'] if 'song' in record and record['song'] else None,))
                        logging.info(command_get)
                        data_from_db = curs.execute(command_get)
                        if data_from_db is not None:
                            data_from_db = data_from_db.fetchone()[0]
                except Exception as e:
                    logging.error(f'[{datetime.datetime.now().isoformat()}] Can not fetch: {e.__str__()}')
                if data_from_db is not None:
                    for k, v in data_from_db.items():
                        record[k] = v
                insert2songplays(record)

    t = init()
    log_dirs = scan_log(t)
    song_dirs = scan_song(t)
    collectSongPlay(previous_state=collect_song.expand(song_dir=song_dirs),
                    l_records=collect_log.expand(log_dir=log_dirs))


dag_ = data_mart_builder()
