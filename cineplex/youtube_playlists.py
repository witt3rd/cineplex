import json
import os
from datetime import datetime
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()

playlists_data_dir = os.path.join(settings.data_dir, 'yt_playlists')
os.makedirs(playlists_data_dir, exist_ok=True)


def get_playlist_from_youtube_batch(playlist_id_batch):

    logger = Logger()
    logger.debug(f"getting playlist batch from YouTube: {playlist_id_batch=}")

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

    logger.debug(
        f"got batch of {len(playlist_with_meta_batch)} (of {len(playlist_id_batch)}) playlists from YouTube")

    return playlist_with_meta_batch


def get_playlist_from_db(playlist_id):

    logger = Logger()
    logger.debug(f"getting playlist from db: {playlist_id=}")

    playlist = get_db().yt_playlists.find_one({'_id': playlist_id})

    title = playlist['playlist']['snippet']['title']

    logger.debug(f"got playlist from db: {playlist_id=} {title=}")

    return playlist


def get_playlist_from_db_batch(playlist_id_batch):

    logger = Logger()
    logger.debug(
        f"getting playlist batch from db: {playlist_id_batch}")

    playlist_cursor = get_db().yt_playlists.find(
        {'_id': {'$in': playlist_id_batch}})

    playlist_batch = list(playlist_cursor)

    logger.debug(
        f"got batch of {len(playlist_batch)} (of {len(playlist_id_batch)} playlists from db")

    return playlist_batch


def save_playlist_to_db(playlist_with_meta, to_disk=True):

    playlist_id = playlist_with_meta['_id']

    logger = Logger()
    logger.debug(f"saving playlist to db: {playlist_id=} ({to_disk=})")

    if to_disk:
        with open(os.path.join(playlists_data_dir, f"yt_playlist_{playlist_id}.json"), "w") as result:
            json.dump(playlist_with_meta, result, indent=2)

    get_db().yt_playlists.update_one(
        {'_id': playlist_id},
        {'$set': playlist_with_meta}, upsert=True)


def save_playlist_to_db_batch(playlist_with_meta_batch, to_disk=True):

    logger = Logger()
    logger.debug(
        f"saving playlist batch to db: {[x['_id'] for x in playlist_with_meta_batch]} ({to_disk=}")

    for playlist_with_meta in playlist_with_meta_batch:
        save_playlist_to_db(playlist_with_meta, to_disk)

    logger.debug(
        f"saved batch of {len(playlist_with_meta_batch)} playlists to db")


def get_playlist_items_from_youtube(playlist_id):

    logger = Logger()
    logger.debug(f"getting playlist items for {playlist_id=}")

    youtube = youtube_api()

    request = youtube.playlistItems().list(
        playlistId=playlist_id,
        part="id,snippet,contentDetails",
        maxResults=50,
        fields='nextPageToken,items(id,snippet,contentDetails)'
    )

    items = []

    while request:
        response = request.execute()
        if 'items' not in response:
            break
        items.extend(response['items'])
        request = youtube.playlistItems().list_next(request, response)

    items_with_meta = {}
    items_with_meta['playlist_id'] = playlist_id
    items_with_meta['as_of'] = str(datetime.now())
    items_with_meta['items'] = items

    logger.debug(
        f"retrieved {len(items)} items for {playlist_id=}")

    return items_with_meta


def get_playlist_items_from_db(playlist_id):
    pass


def save_playlist_items(playlist_id, items_with_meta, to_disk=True):
    pass
