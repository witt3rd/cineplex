import glob
import json
import os
import re
import shutil
from datetime import datetime
from tkinter import E
import yt_dlp
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()

videos_data_dir = os.path.join(settings.data_dir, 'yt_videos')
os.makedirs(videos_data_dir, exist_ok=True)

os.makedirs(settings.tmp_dir, exist_ok=True)

image_exts = ['.jpg', '.webp', '.png']
videos_exts = ['.webm', '.mkv', '.mp4']


def _resolve_files(json_filename):

    # expecting filenames of the form: <path>/<title>.info.json
    # and the corresponding thumbnail and video files (with the same title)
    # in the same directory

    filepath, ext = os.path.splitext(json_filename)
    if ext != '.json':
        Logger().error(f"unexpected file extension {ext=} (expecting '.json')")
        return None

    filepath, ext = os.path.splitext(filepath)
    if ext != '.info':
        Logger().error(f"unexpected file extension {ext=} (expecting '.info')")
        return None

    # find the corresponding thumbnail and video files
    glob_path = re.sub('([\[\]])', '[\\1]', filepath)
    files = glob.glob(f"{glob_path}.*")

    if len(files) != 3:
        Logger().error(
            f"found {len(files)} files (expecting 3) at {filepath=}")
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
        Logger().error(f"no video file found for {info_filename=} | {files=}")
        return None

    if thumbnail_filename is None:
        Logger().warning(
            f"no thumbnail file found for {info_filename=} | {files=}")

    return {
        'thumbnail_filename': thumbnail_filename,
        'video_filename': video_filename,
        'info_filename': info_filename
    }


def extract_video_info_from_file(json_filename, files=None):

    try:

        if files is None:
            files = _resolve_files(json_filename)
            if files is None:
                return None

        with open(json_filename, 'r') as f:
            data = json.load(f)

        if 'id' not in data:
            Logger().error(f"no id in {json_filename=}: {data=}")
            return None

        return extract_video_info(data, files)

    except Exception as e:
        Logger().error(e)
        return None


def extract_video_info(data, files=None):

    try:

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

    except Exception as e:
        Logger().error(e)
        return None


def move_file(src_dir, dst_dir, filename):
    src_filename = os.path.join(src_dir, filename)
    dst_filename = os.path.join(dst_dir, filename)
    shutil.move(src_filename, dst_filename)


def get_video_from_youtube(video_url):

    try:

        ydl_opts = {
            'logger': Logger(),
            'writethumbnail': True,
            'paths': {
                'home': settings.tmp_dir,
            },
            'outtmpl': '%(title)s-%(id)s.%(ext)s',
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url)
            info = ydl.sanitize_info(info)

            # find the thumbnail file
            for thumbnail in info['thumbnails']:
                if "filepath" in thumbnail:
                    thumbnail_filename = thumbnail["filepath"].split('/')[-1]
                    break

            # derive the other filenames
            basename, _ = os.path.splitext(thumbnail_filename)
            video_filename = f"{basename}.{info['ext']}"
            info_filename = f"{basename}.info.json"

            # write info to file
            with open(os.path.join(settings.tmp_dir, info_filename), 'w') as f:
                json.dump(info, f, indent=2)

            dst_dir = os.path.join(
                settings.youtube_channels_dir, info['channel'])
            os.makedirs(dst_dir, exist_ok=True)

            # move files to destination
            move_file(settings.tmp_dir, dst_dir, thumbnail_filename)
            move_file(settings.tmp_dir, dst_dir, video_filename)
            move_file(settings.tmp_dir, dst_dir, info_filename)

            return extract_video_info(info, {
                'video_filename': video_filename,
                'info_filename': info_filename,
                'thumbnail_filename': thumbnail_filename,
            })

    except Exception as e:
        Logger().error(e)
        return None


def get_video_from_db(video_id):

    try:

        return get_db().yt_videos.find_one({'_id': video_id})

    except Exception as e:
        Logger().error(e)
        return None


def get_video_from_db_batch(video_id_batch):

    try:

        videos_cursor = get_db().yt_videos.find(
            {'_id': {'$in': video_id_batch}})
        return list(videos_cursor)

    except Exception as e:
        Logger().error(e)
        return None


def save_video_to_db(video_with_meta, to_disk=True):

    try:

        id = video_with_meta['_id']

        if to_disk:
            with open(os.path.join(videos_data_dir, f'video_{id}.json'), 'w') as f:
                json.dump(video_with_meta, f, indent=2)

        get_db().yt_videos.update_one(
            {'_id': id}, {'$set': video_with_meta}, upsert=True)

    except Exception as e:
        Logger().error(e)


def save_video_to_db_batch(video_with_meta_batch, to_disk=True):

    try:

        for video_with_meta in video_with_meta_batch:
            save_video_to_db(video_with_meta, to_disk)

    except Exception as e:
        Logger().error(e)
