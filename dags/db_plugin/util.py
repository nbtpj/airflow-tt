import datetime
import logging


def execute_or_log(DATABASE_CONN, command):
    if DATABASE_CONN is None:
        logging.warning(
            f'[{datetime.datetime.now().isoformat()}] No connection with database found, your data is not saved!')
        return False
    else:
        try:
            with DATABASE_CONN.cursor() as curs:
                logging.info(command)
                curs.execute(command)
                DATABASE_CONN.commit()
            return True
        except Exception as e:
            DATABASE_CONN.rollback()
            logging.error(f'[{datetime.datetime.now().isoformat()}] Can not execute: error: {e.__str__()}')
            return False
