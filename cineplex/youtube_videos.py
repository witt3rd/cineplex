import glob
import json
import os
import re
from datetime import datetime
import yt_dlp
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()


ydl_opts = {
    'logger': Logger(),
    'writethumbnail': True,
    'paths': {
        'home': settings.tmp_dir,
    }
}


image_exts = ['.jpg', '.webp', '.png']
videos_exts = ['.webm', '.mkv', '.mp4']


def resolve_files(json_filename):

    logger = Logger()
    logger.debug(f"resolving from {json_filename=}")

    # expecting filenames of the form: <path>/<title>.info.json
    # and the corresponding thumbnail and video files (with the same title)
    # in the same directory

    filepath, ext = os.path.splitext(json_filename)
    if ext != '.json':
        logger.error(f"unexpected file extension {ext=} (expecting '.json')")
        return None

    filepath, ext = os.path.splitext(filepath)
    if ext != '.info':
        logger.error(f"unexpected file extension {ext=} (expecting '.info')")
        return None

    # find the corresponding thumbnail and video files
    glob_path = re.sub('([\[\]])', '[\\1]', filepath)
    files = glob.glob(f"{glob_path}.*")

    if len(files) != 3:
        logger.error(f"found {len(files)} files (expecting 3) at {filepath=}")
        return None

    thumbnail_filename = None
    video_filename = None
    info_filename = json_filename.split('/')[-1]

    for file in files:
        _, ext = os.path.splitext(file)
        filename = file.split('/')[-1]
        if ext in image_exts:
            thumbnail_filename = filename
        elif ext in videos_exts:
            video_filename = filename

    if video_filename is None:
        logger.error(f"no video file found for {info_filename=} | {files=}")
        return None

    if thumbnail_filename is None:
        logger.warning(
            f"no thumbnail file found for {info_filename=} | {files=}")

    return {
        'thumbnail_filename': thumbnail_filename,
        'video_filename': video_filename,
        'info_filename': info_filename
    }


def extract_video_info(json_filename, files=None):

    logger = Logger()
    logger.debug(f"extracting video info from json {json_filename=}")

    if files is None:
        files = resolve_files(json_filename)
        if files is None:
            return None

    with open(json_filename, 'r') as f:
        data = json.load(f)

    if 'id' not in data:
        logger.error(f"no id in {json_filename=}: {data=}")
        return None

    info_with_meta = {
        '_id': data['id'],
        'as_of': str(datetime.now()),
        'video': {
            'title': data['title'] if 'title' in data else id,
            'description': data['description'] if 'description' in data else '',
            'tags': data['tags'] if 'tags' in data else [],
            'categories': data['categories'] if 'categories' in data else [],
            'channel_id': data['channel_id'] if 'channel_id' in data else '',
            'channel_title': data['channel'] if 'channel' in data else '',
            'uploader': data['uploader'] if 'uploader' in data else '',
            'uploader_id': data['uploader_id'] if 'uploader_id' in data else '',
            'upload_date': str(datetime.strptime(data['upload_date'], "%Y%m%d")) if 'upload_date' in data else '',
            'duration_seconds': data['duration'] if 'duration' in data else 0,
            'view_count': data['view_count'] if 'view_count' in data else 0,
            'like_count': data['like_count'] if 'like_count' in data else 0,
            'dislike_count': data['dislike_count'] if 'dislike_count' in data else 0,
            'average_rating': data['average_rating'] if 'average_rating' in data else 0,
            'files': files
        }
    }
    return info_with_meta


def download_video(video_url):
    logger = Logger()
    logger.debug(f"downloading video {video_url=}")

    try:

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url)
            info = ydl.sanitize_info(info)

            # find the thumbnail file
            for t in info['thumbnails']:
                if "filepath" in t:
                    thumbnail_filename = t["filepath"]
                    break

            # use it to derive the other filenames
            basename, _ = os.path.splitext(thumbnail_filename)
            video_filename = f"{basename}.{info['ext']}"
            info_filename = f"{basename}.info.json"

            # write info to file
            with open(info_filename, 'w') as f:
                json.dump(info, f, indent=2)

            return {
                'video_filename': video_filename,
                'info_filename': info_filename,
                'thumbnail_filename': thumbnail_filename,
                'info': info
            }

    except Exception as e:
        logger.error(
            f"unhandled exception downloading video {video_url=}", exc_info=True)
        raise e


def get_video_from_db(video_id):

    logger = Logger()
    logger.debug(f"getting video from db for {video_id=}")

    return get_db().yt_videos.find_one({'_id': video_id})


def get_videos_from_db(video_ids):

    logger = Logger()
    logger.debug(f"getting {len(video_ids)} videos from db")

    videos_cursor = get_db().yt_videos.find({'_id': {'$in': video_ids}})

    videos = list(videos_cursor)

    logger.debug(f"got {len(videos)} videos for {len(video_ids)} from db")

    return videos


def save_video(video_with_meta, to_disk=True):

    logger = Logger()
    logger.debug(f"saving video to db: {video_with_meta['video']['title']}")

    id = video_with_meta['_id']

    if to_disk:
        dir = os.path.join(settings.data_dir, 'videos')
        os.makedirs(dir, exist_ok=True)
        with open(os.path.join(dir, f'video_{id}.json'), 'w') as f:
            json.dump(video_with_meta, f, indent=2)

    get_db().yt_videos.update_one(
        {'_id': id}, {'$set': video_with_meta}, upsert=True)


def save_videos(videos_with_meta, to_disk=True):

    logger = Logger()
    logger.debug(f"saving {len(videos_with_meta)} videos to db")

    for video_with_meta in videos_with_meta:
        save_video(video_with_meta, to_disk)

    logger.debug(f"saved {len(videos_with_meta)} videos to db")
