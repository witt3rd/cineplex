from pymongo import MongoClient
from cineplex.config import Settings

settings = Settings()


class Database:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls, *args, **kwargs)
            cls._instance.client = MongoClient(settings.mongo_url)
            cls._instance.db = cls._instance.client[settings.mongo_db]
        return cls._instance


def get_db():
    return Database().db


def create_indices():
    db = get_db()

    db.yt_videos.create_index([('$**', "text")])
