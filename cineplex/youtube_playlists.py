import json
import os
from datetime import datetime
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()

yt_playlists_data_dir = os.path.join(settings.data_dir, 'yt_playlists')
os.makedirs(yt_playlists_data_dir, exist_ok=True)

yt_playlist_items_data_dir = os.path.join(
    settings.data_dir, 'yt_playlist_items')
os.makedirs(yt_playlist_items_data_dir, exist_ok=True)


def get_playlist_from_youtube_batch(playlist_id_batch):

    Logger.log_get_batch('yt_playlists', 'YouTube', playlist_id_batch)

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

        Logger.log_got_batch('yt_playlists', 'YouTube',
                             playlist_with_meta_batch)

        return playlist_with_meta_batch

    except Exception as e:
        Logger.exception(e)
        return []


def get_playlist_from_db(playlist_id):

    Logger.log_get('yt_playlist', 'db', playlist_id)

    try:
        playlist = get_db().yt_playlists.find_one({'_id': playlist_id})

        if (playlist is None):
            return None

        title = playlist['playlist']['snippet']['title']

        Logger.log_got('yt_playlist', 'db', playlist_id, f"{title=}")

        return playlist

    except Exception as e:
        Logger.exception(e)
        return None


def get_playlist_from_db_batch(playlist_id_batch):

    Logger.log_get_batch('yt_playlists', 'db', playlist_id_batch)

    try:
        playlist_cursor = get_db().yt_playlists.find(
            {'_id': {'$in': playlist_id_batch}})

        playlist_batch = list(playlist_cursor)

        Logger.log_got_batch('yt_playlists', 'db', playlist_batch)

        return playlist_batch

    except Exception as e:
        Logger.exception(e)
        return []


def save_playlist_to_db(playlist_with_meta, to_disk=True):

    playlist_id = playlist_with_meta['_id']

    Logger.log_save('yt_playlist', 'db', playlist_id, f"{to_disk=}")

    try:
        if to_disk:
            with open(os.path.join(yt_playlists_data_dir, f"yt_playlist_{playlist_id}.json"), "w") as result:
                json.dump(playlist_with_meta, result, indent=2)

        get_db().yt_playlists.update_one(
            {'_id': playlist_id},
            {'$set': playlist_with_meta}, upsert=True)

        Logger.log_saved('yt_playlist', 'db', playlist_id)

    except Exception as e:
        Logger.exception(e)


def save_playlist_to_db_batch(playlist_with_meta_batch, to_disk=True):

    Logger.log_save_batch('yt_playlists', 'db',
                          playlist_with_meta_batch, f"{to_disk=}")

    try:
        for playlist_with_meta in playlist_with_meta_batch:
            save_playlist_to_db(playlist_with_meta, to_disk)

        Logger.log_saved_batch('yt_playlists', 'db', playlist_with_meta_batch)

    except Exception as e:
        Logger.exception(e)


def get_playlist_items_from_youtube(playlist_id):

    Logger.log_get('yt_playlist_items', 'YouTube', playlist_id)

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

        Logger.log_got('yt_playlist_items', 'YouTube',
                       playlist_id, len(items))

        return playlist_items_with_meta

    except Exception as e:
        Logger.exception(e)
        return []


def get_playlist_items_from_db(playlist_id):

    Logger.log_get('yt_playlist_items', 'db', playlist_id)

    try:
        playlist_items = get_db().yt_playlist_items.find_one(
            {'_id': playlist_id})

        if playlist_items is None:
            return None

        items = playlist_items['items']

        Logger.log_got('yt_playlist_items', 'db', playlist_id, len(items))

        return playlist_items

    except Exception as e:
        Logger.exception(e)
        return None


def save_playlist_items_to_db(playlist_items_with_meta, to_disk=True):

    playlist_id = playlist_items_with_meta['_id']

    Logger.log_save('yt_playlist_items', 'db', playlist_id, f"{to_disk=}")

    try:
        if to_disk:
            with open(os.path.join(yt_playlist_items_data_dir, f"yt_playlist_items_{playlist_id}.json"), "w") as result:
                json.dump(playlist_items_with_meta, result, indent=2)

        get_db().yt_playlist_items.update_one(
            {'_id': playlist_id}, {'$set': playlist_items_with_meta}, upsert=True)

        Logger.log_saved('yt_playlist_items', 'db', playlist_id)

    except Exception as e:
        Logger.exception(e)
