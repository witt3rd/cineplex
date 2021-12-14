import json
from operator import mod
import os
from datetime import datetime
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()

yt_playlists_bkp_dir = os.path.join(settings.bkp_dir, 'yt_playlists')
os.makedirs(yt_playlists_bkp_dir, exist_ok=True)

yt_playlist_items_bkp_dir = os.path.join(
    settings.bkp_dir, 'yt_playlist_items')
os.makedirs(yt_playlist_items_bkp_dir, exist_ok=True)


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


def save_offline_playlist_to_db(playlist_id, as_of: datetime = None):

    try:
        res = get_db().yt_playlists.update_one(
            {'_id': playlist_id},
            {'$set': {'offline': True,
                      'offline_as_of': as_of if as_of else str(datetime.now())}},
        )
        return res.modified_count

    except Exception as e:
        Logger().exception(e)


def get_offline_playlists_from_db():

    try:
        return list(get_db().yt_playlists.find({'offline': True}))

    except Exception as e:
        Logger().exception(e)
