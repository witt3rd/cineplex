from cmath import log
from genericpath import exists
import json
import os
from datetime import datetime
from tkinter import E
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()

yt_channels_data_dir = os.path.join(settings.data_dir, 'yt_channels')
os.makedirs(yt_channels_data_dir, exist_ok=True)

yt_channel_playlists_data_dir = os.path.join(
    settings.data_dir, 'yt_channel_playlists')
os.makedirs(yt_channel_playlists_data_dir, exist_ok=True)


def get_channel_from_youtube_batch(channel_id_batch):

    Logger.log_get_batch('yt_channels', 'YouTube', channel_id_batch)

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

        Logger.log_got_batch('yt_channels', 'YouTube', channel_with_meta_batch)

        return channel_with_meta_batch

    except Exception as e:
        Logger.exception(e)
        return []


def get_channel_from_db(channel_id):

    Logger.log_get('yt_channel', 'db', channel_id)

    try:
        channel = get_db().yt_channels.find_one({'_id': channel_id})

        if channel is None:
            return None

        title = channel['channel']['snippet']['title']

        Logger.log_got('yt_channel', 'db', channel_id, f"{title=}")

        return channel

    except Exception as e:
        Logger.exception(e)
        return None


def get_channel_from_db_batch(channel_id_batch):

    Logger.log_get_batch('yt_channels', 'db', channel_id_batch)

    try:
        channels_cursor = get_db().yt_channels.find(
            {'_id': {'$in': channel_id_batch}})

        channel_batch = list(channels_cursor)

        Logger.log_got_batch('yt_channels', 'db', channel_batch)

        return channel_batch

    except Exception as e:
        Logger.exception(e)
        return []


def save_channel_to_db(channel_with_meta, to_disk=True):

    channel_id = channel_with_meta['_id']

    Logger.log_save('yt_channel', 'db', channel_id, f"{to_disk=}")

    try:
        if to_disk:
            with open(os.path.join(yt_channels_data_dir, f"yt_channel_{channel_id}.json"), "w") as result:
                json.dump(channel_with_meta, result, indent=2)

        get_db().yt_channels.update_one(
            {'_id': channel_id}, {'$set': channel_with_meta}, upsert=True)

        Logger.log_saved('yt_channel', 'db', channel_id)

    except Exception as e:
        Logger.exception(e)


def save_channel_to_db_batch(channel_with_meta_batch, to_disk=True):

    Logger.log_save_batch('yt_channels', 'db',
                          channel_with_meta_batch, f"{to_disk=}")

    try:
        for channel_with_meta in channel_with_meta_batch:
            save_channel_to_db(channel_with_meta, to_disk)

        Logger.log_saved_batch('yt_channels', 'db', channel_with_meta_batch)

    except Exception as e:
        Logger.exception(e)


def get_channel_videos_from_youtube(channel_id):
    pass

    # logger = Logger()
    # logger.debug(f"getting playlist items for {playlist_id=}")

    # youtube = youtube_api()

    # request = youtube.playlistItems().list(
    #     playlistId=playlist_id,
    #     part="id,snippet,contentDetails",
    #     maxResults=50,
    #     fields='nextPageToken,items(id,snippet,contentDetails)'
    # )

    # items = []

    # while request:
    #     response = request.execute()
    #     items.extend(response['items'])
    #     request = youtube.playlistItems().list_next(request, response)

    # items_with_meta = {}
    # items_with_meta['playlist_id'] = playlist_id
    # items_with_meta['as_of'] = str(datetime.now())
    # items_with_meta['items'] = items

    # save_playlist_items(playlist_id, items_with_meta)

    # logger.info(
    #     f"retrieved and saved {len(items)} items for {playlist_id=}")

    # return items_with_meta


def get_channel_videos_from_db(channel_id):
    pass

    # logger = Logger()
    # logger.debug(f"getting playlist items from db for {playlist_id=}")

    # return json.loads(get_db().get(f'playlist_items#{playlist_id}'))


def save_channel_videos(channel_id, videos_with_meta, to_disk=True):
    pass

    # logger = Logger()
    # logger.debug(f"saving playlist items for {playlist_id=}")

    # if to_disk:
    #     with open(os.path.join(settings.data_dir, f"playlist_items_{playlist_id}.json"), "w") as result:
    #         json.dump(items_with_meta, result, indent=2)

    # get_db().set(f'playlist_items#{playlist_id}', json.dumps(items_with_meta))


def get_channel_playlists_from_youtube(channel_id):

    Logger.log_get('yt_channel_playlists', 'YouTube', channel_id)

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

        Logger.log_got('yt_channel_playlists', 'YouTube', channel_id)

        return channel_playlists_with_meta

    except Exception as e:
        Logger.exception(e)
        return {}


def get_channel_playlists_from_db(channel_id):

    Logger.log_get('yt_channel_playlists', 'db', channel_id)

    try:
        channel_playlist = get_db().yt_channel_playlists.find_one(
            {'_id': channel_id})

        Logger.log_got('yt_channel_playlists', 'db',
                       channel_id, len(channel_playlist['playlists']))

        return channel_playlist

        return channel_playlist

    except Exception as e:
        Logger.exception(e)
        return None


def save_channel_playlists(channel_playlists_with_meta, to_disk=True):

    channel_id = channel_playlists_with_meta['_id']

    Logger.log_save('yt_channel_playlists', 'db', channel_id, f"{to_disk=}")

    try:
        if to_disk:
            with open(os.path.join(yt_channel_playlists_data_dir, f"yt_channel_playlists_{channel_id}.json"), "w") as result:
                json.dump(channel_playlists_with_meta, result, indent=2)

        get_db().yt_channel_playlists.update_one(
            {'_id': channel_id},
            {'$set': channel_playlists_with_meta}, upsert=True)

        Logger.log_saved('yt_channel_playlists', 'db', channel_id, len(
            channel_playlists_with_meta['playlists']))

    except Exception as e:
        Logger.exception(e)
