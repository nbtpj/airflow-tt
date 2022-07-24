import datetime
import logging

import scrapy


def save_new_to_raw_table(DATABASE_CONN, RAW_TABLE, new) -> bool:
    if DATABASE_CONN is None:
        logging.warning(
            f'[{datetime.datetime.now().isoformat()}] No connection with database found, your data is not saved!')
        return False
    else:
        try:
            with DATABASE_CONN.cursor() as curs:
                command_insert = curs.mogrify(
                    f'INSERT INTO {RAW_TABLE}(url, title, abstract, body, "source", publisher, tags, "date", lastmod, place, author) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                    (
                        new['url'], new['title'],
                        new['abstract'], new['body'],
                        new['source'], new['publisher'],
                        '; '.join(new['tags']) if new[
                            'tags'] else None, new['date'],
                        new['lastmod'], new['place'],
                        new['author']))

                print(command_insert)
                curs.execute(command_insert)
                DATABASE_CONN.commit()
            return True
        except Exception as e:
            logging.error(f'[{datetime.datetime.now().isoformat()}] Can not save item: error: {e.__str__()}')
            return False


def get_item(DATABASE_CONN, RAW_TABLE, loc: str) -> scrapy.Item:
    if DATABASE_CONN is None:
        logging.warning(
            f'[{datetime.datetime.now().isoformat()}] No connection with database found, can not find your data!')
        return None
    else:
        try:
            data_from_db = {}
            with DATABASE_CONN.cursor() as curs:
                command_get = f"SELECT * FROM {RAW_TABLE} WHERE url=\'{loc}\'"
                data_from_db = curs.execute(command_get)
                if data_from_db is not None:
                    data_from_db = data_from_db.fetchone()[0]
                DATABASE_CONN.commit()
            return data_from_db
        except Exception as e:
            logging.error(f'[{datetime.datetime.now().isoformat()}] Can not fetch: {e.__str__()}')
            return None
