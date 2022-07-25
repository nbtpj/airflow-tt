import psycopg2
import redis
from config import db_connection_config, paging_connection_config

DATABASE_CONN = psycopg2.connect(**db_connection_config)
# PAGING_CONN = redis.Redis(**paging_connection_config)