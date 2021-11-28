import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# Logging
LOG_NAME = os.getenv("LOG_NAME", "raytube")
LOG_DIR = os.getenv("LOG_DIR", "./logs")
LOG_LEVEL = os.getenv("LOGLEVEL", "DEBUG")

# API
API_HOST = os.getenv("API_HOST")
API_PORT = os.getenv("API_PORT")

# Datavase
DB = os.getenv("DB")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Paths
DATA_DIR = os.getenv("DATA_DIR", "./data")
MEDIA_ROOT = os.getenv("MEDIA_ROOT")
METADATA_DIR = os.getenv("METADATA_DIR")
THUMBNAIL_DIR = os.getenv("THUMBNAIL_DIR")
VIDEO_DIR = os.getenv("VIDEO_DIR")
YOUTUBE_DIR = os.getenv("YOUTUBE_DIR")

# Legacy
OLD_MEDIA_ROOT = os.getenv("OLD_MEDIA_ROOT")
OLD_METADATA_ROOT = os.getenv("OLD_METADATA_ROOT")
YOUTUBE_CRAWLED_METADATA_FILE = os.getenv("YOUTUBE_CRAWLED_METADATA_FILE")
YOUTUBE_LOG_FILE = os.getenv("YOUTUBE_LOG_FILE")

# YouTube
YOUTUBE_MY_CHANNEL_ID = os.getenv("YOUTUBE_MY_CHANNEL_ID")
