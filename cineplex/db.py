import redis
from settings import (
    DB,
    DB_HOST,
    DB_PORT
)


def get_db():
    return redis.StrictRedis(host=DB_HOST, port=DB_PORT, db=DB)
