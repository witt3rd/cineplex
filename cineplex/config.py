from pydantic import BaseSettings


class Settings(BaseSettings):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Settings, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    my_youtube_channel_id: str = "UC-lHJZR3Gqxm24_Vd_AJ5Yw"

    db: str
    db_host: str
    db_port: int

    mongo_url: str = "mongodb://localhost:27017"
    mongo_db: str = "cineplex"

    tmp_dir: str = './tmp'
    data_dir: str = './data'
    youtube_channels_dir: str = '/Volumes/Cineplex00/youtube/channels'

    log_name: str = ''
    log_level: str = 'INFO'
    log_dir: str = './logs'
    log_to_console: bool = True

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
