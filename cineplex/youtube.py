import os
import glob
import pickle
import json
import os
import re
from datetime import datetime
import pymongo
import click
import yt_dlp
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings
from cineplex.utils import (
    IMAGE_EXTS,
    VIDEO_EXTS,
    move_file
)

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


settings = Settings()

#
# Paths
#

os.makedirs(settings.tmp_dir, exist_ok=True)

yt_channels_bkp_dir = os.path.join(settings.bkp_dir, 'yt_channels')
os.makedirs(yt_channels_bkp_dir, exist_ok=True)

yt_channel_playlists_bkp_dir = os.path.join(
    settings.bkp_dir, 'yt_channel_playlists')
os.makedirs(yt_channel_playlists_bkp_dir, exist_ok=True)

yt_playlists_bkp_dir = os.path.join(settings.bkp_dir, 'yt_playlists')
os.makedirs(yt_playlists_bkp_dir, exist_ok=True)

yt_playlist_items_bkp_dir = os.path.join(settings.bkp_dir, 'yt_playlist_items')
os.makedirs(yt_playlist_items_bkp_dir, exist_ok=True)

videos_bkp_dir = os.path.join(settings.bkp_dir, 'yt_videos')
os.makedirs(videos_bkp_dir, exist_ok=True)


#
# YouTube API
#

scopes = ["https://www.googleapis.com/auth/youtube"]


def youtube_api():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "youtube.keys.json"
    token_file = "token.pickle"
    credentials = None

    if os.path.exists(token_file):
        with open(token_file, "rb") as token:
            credentials = pickle.load(token)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secrets_file, scopes)
            credentials = flow.run_local_server(
                port=8080, prompty="consent", authorization_prompt_message="")

        with open(token_file, "wb") as token:
            pickle.dump(credentials, token)

    youtube = build(
        api_service_name, api_version, credentials=credentials)

    return youtube


#
# Channels
#


def get_channel_from_youtube(channel_id):
    channel_with_meta_batch = get_channel_from_youtube_batch([channel_id])
    if channel_with_meta_batch:
        return channel_with_meta_batch[0]


def get_channel_from_youtube_batch(channel_id_batch):

    try:
        youtube = youtube_api()
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics,brandingSettings",
            id=channel_id_batch,
            maxResults=50,
        )

        channel_with_meta_batch = []

        while request:
            response = request.execute()
            if 'items' not in response:
                break
            for channel in response['items']:
                channel_with_meta = {}
                channel_with_meta['_id'] = channel['id']
                channel_with_meta['as_of'] = str(datetime.now())
                channel_with_meta['channel'] = channel
                channel_with_meta_batch.append(channel_with_meta)
            request = youtube.channels().list_next(request, response)

        return channel_with_meta_batch

    except Exception as e:
        Logger().exception(e)


def get_channel_from_db(channel_id):

    try:
        return get_db().yt_channels.find_one({'_id': channel_id})

    except Exception as e:
        Logger().exception(e)


def get_channel_from_db_batch(channel_id_batch):

    try:
        channels_cursor = get_db().yt_channels.find(
            {'_id': {'$in': channel_id_batch}})

        return list(channels_cursor)

    except Exception as e:
        Logger().exception(e)


def save_channel_to_db(channel_with_meta, to_disk=True):

    try:
        channel_id = channel_with_meta['_id']

        if to_disk:
            with open(os.path.join(yt_channels_bkp_dir, f"yt_channel_{channel_id}.json"), "w") as result:
                json.dump(channel_with_meta, result, indent=2)

        get_db().yt_channels.update_one(
            {'_id': channel_id}, {'$set': channel_with_meta}, upsert=True)

    except Exception as e:
        Logger().exception(e)


def save_channel_to_db_batch(channel_with_meta_batch, to_disk=True):

    try:
        for channel_with_meta in channel_with_meta_batch:
            save_channel_to_db(channel_with_meta, to_disk)

    except Exception as e:
        Logger().exception(e)


def get_channel_playlists_from_youtube(channel_id):

    try:
        youtube = youtube_api()
        request = youtube.playlists().list(
            channelId=channel_id,
            part="id,snippet,contentDetails",
            maxResults=50,
        )

        playlists = []

        while request:
            response = request.execute()
            if 'items' not in response:
                break
            playlists.extend(response['items'])
            request = youtube.playlists().list_next(request, response)

        channel_playlists_with_meta = {}
        channel_playlists_with_meta['_id'] = channel_id
        channel_playlists_with_meta['as_of'] = str(datetime.now())
        channel_playlists_with_meta['playlists'] = playlists

        return channel_playlists_with_meta

    except Exception as e:
        Logger().exception(e)


def get_channel_playlists_from_youtube_batch(channel_id_batch):
    channel_playlists_with_meta_batch = []
    for channel_id in channel_id_batch:
        channel_playlists_with_meta = get_channel_playlists_from_youtube(
            channel_id)
        if channel_playlists_with_meta:
            channel_playlists_with_meta_batch.append(
                channel_playlists_with_meta)
    return channel_playlists_with_meta_batch


def get_channel_playlists_from_db(channel_id):

    try:
        return get_db().yt_channel_playlists.find_one({'_id': channel_id})

    except Exception as e:
        Logger().exception(e)


def get_channel_playlists_from_db_batch(channel_id_batch):

    try:
        channels_cursor = get_db().yt_channel_playlists.find(
            {'_id': {'$in': channel_id_batch}})

        return list(channels_cursor)

    except Exception as e:
        Logger().exception(e)


def save_channel_playlists_to_db(channel_playlists_with_meta, to_disk=True):

    try:
        channel_id = channel_playlists_with_meta['_id']

        if to_disk:
            with open(os.path.join(yt_channel_playlists_bkp_dir, f"yt_channel_playlists_{channel_id}.json"), "w") as result:
                json.dump(channel_playlists_with_meta, result, indent=2)

        get_db().yt_channel_playlists.update_one(
            {'_id': channel_id},
            {'$set': channel_playlists_with_meta}, upsert=True)

    except Exception as e:
        Logger().exception(e)


def save_channel_playlists_to_db_batch(channel_playlists_with_meta_batch, to_disk=True):

    try:
        for channel_playlists_with_meta in channel_playlists_with_meta_batch:
            save_channel_playlists_to_db(channel_playlists_with_meta, to_disk)

    except Exception as e:
        Logger().exception(e)


def save_offline_channel_to_db(channel_id, as_of: datetime = None, is_auto: bool = False):

    try:
        res = get_db().yt_channels.update_one(
            {'_id': channel_id},
            {'$set': {'offline': is_auto,
                      'offline_as_of': as_of if as_of else str(datetime.now())}},
        )
        return res.modified_count

    except Exception as e:
        Logger().exception(e)


def get_offline_channels_from_db():

    try:
        return list(get_db().yt_channels.find({'offline': True}).sort('offline_as_of', pymongo.DESCENDING))

    except Exception as e:
        Logger().exception(e)


#
# Playlists
#


def get_playlist_from_youtube(playlist_id):

    try:
        playlist_with_meta = get_playlist_from_youtube_batch([playlist_id])[0]
        return playlist_with_meta

    except Exception as e:
        Logger().exception(e)


def get_playlist_from_youtube_batch(playlist_id_batch):

    try:
        youtube = youtube_api()
        request = youtube.playlists().list(
            id=playlist_id_batch,
            part="id,snippet,contentDetails",
            maxResults=50,
        )

        playlist_with_meta_batch = []

        while request:

            response = request.execute()

            if 'items' not in response:
                break

            for playlist in response['items']:
                playlist_with_meta = {}
                playlist_with_meta['_id'] = playlist['id']
                playlist_with_meta['as_of'] = str(datetime.now())
                playlist_with_meta['playlist'] = playlist
                playlist_with_meta_batch.append(playlist_with_meta)

            request = youtube.playlists().list_next(request, response)

        return playlist_with_meta_batch

    except Exception as e:
        Logger().exception(e)


def get_playlist_from_db(playlist_id):

    try:
        playlist = get_db().yt_playlists.find_one({'_id': playlist_id})

        return playlist

    except Exception as e:
        Logger().exception(e)


def get_playlist_from_db_batch(playlist_id_batch):

    try:
        playlist_cursor = get_db().yt_playlists.find(
            {'_id': {'$in': playlist_id_batch}})

        return list(playlist_cursor)

    except Exception as e:
        Logger().exception(e)


def save_playlist_to_db(playlist_with_meta, to_disk=True):

    try:
        playlist_id = playlist_with_meta['_id']

        if to_disk:
            with open(os.path.join(yt_playlists_bkp_dir, f"yt_playlist_{playlist_id}.json"), "w") as result:
                json.dump(playlist_with_meta, result, indent=2)

        get_db().yt_playlists.update_one(
            {'_id': playlist_id},
            {'$set': playlist_with_meta}, upsert=True)

    except Exception as e:
        Logger().exception(e)


def save_playlist_to_db_batch(playlist_with_meta_batch, to_disk=True):

    try:
        for playlist_with_meta in playlist_with_meta_batch:
            save_playlist_to_db(playlist_with_meta, to_disk)

    except Exception as e:
        Logger().exception(e)


def get_playlist_items_from_youtube(playlist_id):

    try:
        youtube = youtube_api()
        request = youtube.playlistItems().list(
            playlistId=playlist_id,
            part="id,snippet,contentDetails",
            maxResults=50,
        )

        playlist_items_with_meta = {
            '_id': playlist_id,
            'as_of': str(datetime.now())
        }

        items = []

        while request:
            response = request.execute()
            if 'items' not in response:
                break
            items.extend(response['items'])
            request = youtube.playlistItems().list_next(request, response)

        playlist_items_with_meta['items'] = items

        return playlist_items_with_meta

    except Exception as e:
        Logger().exception(e)


def get_playlist_items_from_youtube_batch(playlist_id_batch):

    try:

        playlist_items_with_meta_batch = []

        for playlist_id in playlist_id_batch:
            playlist_items_with_meta = get_playlist_items_from_youtube(
                playlist_id)
            if playlist_items_with_meta:
                playlist_items_with_meta_batch.append(playlist_items_with_meta)

        return playlist_items_with_meta_batch

    except Exception as e:
        Logger().exception(e)


def get_playlist_items_from_db(playlist_id):

    try:
        return get_db().yt_playlist_items.find_one({'_id': playlist_id})

    except Exception as e:
        Logger().exception(e)


def get_playlist_items_from_db_batch(playlist_id_batch):

    try:
        playlist_items_with_meta_batch = []

        for playlist_id in playlist_id_batch:
            playlist_items_with_meta = get_playlist_items_from_db(playlist_id)
            if playlist_items_with_meta:
                playlist_items_with_meta_batch.append(playlist_items_with_meta)
        return playlist_items_with_meta_batch

    except Exception as e:
        Logger().exception(e)


def save_playlist_items_to_db(playlist_items_with_meta, to_disk=True):

    try:
        playlist_id = playlist_items_with_meta['_id']

        if to_disk:
            with open(os.path.join(yt_playlist_items_bkp_dir, f"yt_playlist_items_{playlist_id}.json"), "w") as result:
                json.dump(playlist_items_with_meta, result, indent=2)

        get_db().yt_playlist_items.update_one(
            {'_id': playlist_id}, {'$set': playlist_items_with_meta}, upsert=True)

    except Exception as e:
        Logger().exception(e)


def save_playlist_items_to_db_batch(playlist_items_with_meta_batch, to_disk=True):

    for playlist_items_with_meta in playlist_items_with_meta_batch:
        save_playlist_items_to_db(playlist_items_with_meta, to_disk)


def save_offline_playlist_to_db(playlist_id, as_of: datetime = None, is_auto: bool = False):

    try:
        res = get_db().yt_playlists.update_one(
            {'_id': playlist_id},
            {'$set': {'offline': is_auto,
                      'offline_as_of': as_of if as_of else str(datetime.now())}},
        )
        return res.modified_count

    except Exception as e:
        Logger().exception(e)


def get_offline_playlists_from_db():

    try:
        return list(get_db().yt_playlists.find({'offline': True}).sort('offline_as_of', pymongo.DESCENDING))

    except Exception as e:
        Logger().exception(e)


#
# Videos
#

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

        video_id = video_with_meta['_id']

        video_filename, info_filename, thumbnail_filename = _expand_video_files(
            video_with_meta)

        if not video_filename or not os.path.exists(video_filename):
            Logger().error(f"Missing video file for {video_id}")
            return False

        if not info_filename or not os.path.exists(info_filename):
            Logger().error(f"Missing info file for {video_id}")
            return False

        if not thumbnail_filename or not os.path.exists(thumbnail_filename):
            Logger().error(f"Missing thumbnail fil for {video_id}")
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


class MyLogger:

    def __init__(self, show_progress=True):
        self._show_progress = show_progress
        self._progress_bar = None
        self._last_progress = 0
        self._progress_length = 0

    def debug(self, msg):
        # For compatability with youtube-dl, both debug and info are passed into debug
        # You can distinguish them by the prefix '[debug] '
        if not self._show_progress and not msg.startswith('[download] '):
            Logger().debug(msg)
        elif msg.startswith('[debug] ') or msg.startswith('[info] ') or msg.startswith('[download] '):
            pass
        else:
            self.info(msg)

    def info(self, msg):
        Logger().info(msg)

    def warning(self, msg):
        Logger().warning(msg)

    def error(self, msg):
        Logger().error(msg)

    def progress_hook(self, d):

        def _total_bytes():
            return int(d['total_bytes'] if 'total_bytes' in d else d['total_bytes_estimate'])

        if d['status'] == 'downloading':

            if self._progress_bar is None:
                self._progress_length = _total_bytes()
                self._progress_bar = click.progressbar(
                    length=self._progress_length, fill_char=click.style(
                        "█", fg="green"),
                    show_percent=True, show_pos=True, show_eta=True)

            self._progress_bar.label = f"{d['_speed_str']:14s}"

            length = _total_bytes()
            if self._progress_length != length:
                self._progress_length = length
                self._progress_bar.length = self._progress_length

            update = int(d['downloaded_bytes']) - self._last_progress
            self._last_progress += update
            self._progress_bar.update(update)


def get_video_from_youtube(video_url, show_progress=True):

    try:

        yt_logger = MyLogger(show_progress)
        ydl_opts = {
            'logger': yt_logger,
            'writethumbnail': True,
            'paths': {
                'home': settings.tmp_dir,
            },
            'outtmpl': '%(title)s-%(id)s.%(ext)s',
            'progress_hooks': [yt_logger.progress_hook] if show_progress else [],
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


def get_video_from_db(video_id):

    try:

        return get_db().yt_videos.find_one({'_id': video_id})

    except Exception as e:
        Logger().error(e)


def get_video_from_db_batch(video_id_batch):

    try:

        videos_cursor = get_db().yt_videos.find(
            {'_id': {'$in': video_id_batch}})
        return list(videos_cursor)

    except Exception as e:
        Logger().error(e)


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


def save_video_to_db(video_with_meta, to_disk=True):

    try:

        id = video_with_meta['_id']

        if to_disk:
            with open(os.path.join(videos_bkp_dir, f'video_{id}.json'), 'w') as f:
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
