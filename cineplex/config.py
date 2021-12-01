from pydantic import BaseSettings


class Settings(BaseSettings):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Settings, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    youtube_my_channel_id: str = "UC-lHJZR3Gqxm24_Vd_AJ5Yw"

    db: str
    db_host: str
    db_port: int

    data_dir: str = './data'

    youtube_videos_dir: str = './data/youtube/videos'
    youtube_thumbnails_dir: str = '/Volumes/media/youtube/thumbnails'
    youtub_metadata_dir: str = '/Volumes/media/youtube/metadata'

    log_name: str = ''
    log_level: str = 'INFO'
    log_dir: str = './logs'
    log_to_console: bool = True

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
