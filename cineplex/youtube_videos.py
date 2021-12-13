import glob
from importlib.metadata import files
import json
import os
import re
from datetime import datetime
import yt_dlp
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings
from cineplex.utils import (
    IMAGE_EXTS,
    VIDEO_EXTS,
    move_file
)

settings = Settings()

videos_data_dir = os.path.join(settings.data_dir, 'yt_videos')
os.makedirs(videos_data_dir, exist_ok=True)

os.makedirs(settings.tmp_dir, exist_ok=True)


def _expand_video_files(video_with_meta):

    try:
        video = video_with_meta['video']
        files = video['files']

        uploader = video['uploader'] if 'uploader' in video else ''
        channel_title = video['channel_title'] if video['channel_title'] else uploader

        if not channel_title:
            Logger().error(
                f"No channel title for {video_with_meta}")
            return None, None, None

        video_filename = os.path.join(settings.youtube_channels_dir,
                                      channel_title,
                                      files['video_filename']) if 'video_filename' in files else None
        if not video_filename:
            Logger().error(
                f"No video filename for {video_with_meta['_id']}")
            return None, None, None

        if not os.path.exists(video_filename):
            Logger().error(
                f"Video file {video_filename} does not exist")
            return None, None, None

        if not video['title']:
            Logger().error(
                f"No video title for {video_with_meta}")
            return None, None, None

        video_title = video['title']
        video_title = video_title.replace('/', '-')
        video_title = video_title.replace('\\', '-')
        video_title = video_title.replace('*', '-')
        video_title = video_title.replace('?', '-')
        video_title = video_title.replace('"', '-')
        info_filename = os.path.join(settings.youtube_channels_dir,
                                     channel_title,
                                     files['info_filename'])
        thumbnail_filename = os.path.join(settings.youtube_channels_dir,
                                          channel_title,
                                          files['thumbnail_filename'])

        return video_filename, info_filename, thumbnail_filename

    except Exception as e:
        Logger().error(e)
        return None, None, None


def audit_video_files(video_with_meta):

    try:

        video_filename, info_filename, thumbnail_filename = _expand_video_files(
            video_with_meta)

        if video_filename and not os.path.exists(video_filename):
            Logger().error(f"Missing video file: {video_filename}")
            return False

        if info_filename and not os.path.exists(info_filename):
            Logger().error(f"Missing info file: {info_filename}")
            return False

        if thumbnail_filename and not os.path.exists(thumbnail_filename):
            Logger().error(f"Missing thumbnail file: {thumbnail_filename}")
            return False

        return True

    except Exception as e:
        Logger().error(e)
        return False


def delete_video_files(video_with_meta):

    try:

        video_filename, info_filename, thumbnail_filename = _expand_video_files(
            video_with_meta)

        if os.path.exists(video_filename):
            os.remove(video_filename)

        if os.path.exists(info_filename):
            os.remove(info_filename)

        if os.path.exists(thumbnail_filename):
            os.remove(thumbnail_filename)

    except Exception as e:
        Logger().error(e)


def _resolve_multiple_files(file1, file2):
    size1 = os.path.getsize(file1)
    size2 = os.path.getsize(file2)
    if size2 > size1:
        os.remove(file1)
        return file2
    return file1


def resolve_video_files(json_filename):

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

    thumbnail_filename = None
    video_filename = None
    info_filename = json_filename.split('/')[-1]

    for file in files:
        _, ext = os.path.splitext(file)
        if ext in IMAGE_EXTS:
            if thumbnail_filename is None:
                thumbnail_filename = file
            else:
                thumbnail_filename = _resolve_multiple_files(
                    thumbnail_filename, file)
        elif ext in VIDEO_EXTS:
            if video_filename is None:
                video_filename = file
            else:
                video_filename = _resolve_multiple_files(video_filename, file)

    if video_filename is None:
        Logger().error(f"no video file found for {info_filename=} | {files=}")
        return None

    if thumbnail_filename is None:
        Logger().warning(
            f"no thumbnail file found for {info_filename=} | {files=}")

    video_filename = video_filename.split('/')[-1]
    thumbnail_filename = thumbnail_filename.split(
        '/')[-1] if thumbnail_filename else None
    return {
        'thumbnail_filename': thumbnail_filename,
        'video_filename': video_filename,
        'info_filename': info_filename
    }


def extract_video_info_from_file(json_filename, files=None):

    try:

        if files is None:
            files = resolve_video_files(json_filename)
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

        uploader = data['uploader'] if 'uploader' in data else ''
        channel_title = data['channel'] if 'channel' in data else uploader

        info_with_meta = {
            '_id': data['id'],
            'as_of': str(datetime.now()),
            'video': {
                'title': data['title'] if 'title' in data else id,
                'description': data['description'] if 'description' in data else '',
                'tags': data['tags'] if 'tags' in data else [],
                'categories': data['categories'] if 'categories' in data else [],
                'channel_id': data['channel_id'] if 'channel_id' in data else '',
                'channel_title': channel_title,
                'uploader': uploader,
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


def delete_video_from_db(video_id):

    try:

        x = get_db().yt_videos.delete_one({'_id': video_id})
        return x.deleted_count

    except Exception as e:
        Logger().error(e)
        return 0


def delete_video_from_db_batch(video_id_batch):

    try:

        x = get_db().yt_videos.delete_many({'_id': {'$in': video_id_batch}})
        return x.deleted_count

    except Exception as e:
        Logger().error(e)
        return 0


def get_videos_for_audit():

    try:

        videos_cursor = get_db().yt_videos.find()
        for video_with_meta in videos_cursor:
            id = video_with_meta['_id']
            video = video_with_meta['video']
            channel_title = video['channel_title']
            uploader = video['uploader']
            files = video['files']
            yield {
                '_id': id,
                'video': {
                    'channel_title': channel_title if channel_title else uploader,
                    'files': files
                }
            }

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


def search_db(query):

    try:

        videos_cursor = get_db().yt_videos.find(
            {'$text': {'$search': query}})  # .sort({'score': {'$meta': "textScore"}})
        return list(videos_cursor)

    except Exception as e:
        Logger().error(e)
        return None
