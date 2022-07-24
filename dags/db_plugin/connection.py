import psycopg2
from config import db_connection_config, raw_table_name

DATABASE_CONN = psycopg2.connect(**db_connection_config)

with DATABASE_CONN.cursor() as curs:
    curs.execute(f'CREATE TABLE IF NOT EXISTS {raw_table_name} ('
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