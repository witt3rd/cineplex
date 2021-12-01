import redis
from config import Settings

settings = Settings()


def get_db():
    return redis.StrictRedis(
        host=settings.db_host,
        port=settings.db_port,
        db=settings.db)
