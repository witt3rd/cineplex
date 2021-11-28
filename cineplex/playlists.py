import json
import os
from datetime import datetime
from youtube import youtube_api
from db import get_db
from logger import Logger
from settings import DATA_DIR


def get_playlists_from_youtube(channel_id):

    logger = Logger()
    logger.debug(f"getting playlists for {channel_id=}")

    youtube = youtube_api()

    request = youtube.playlists().list(
        channelId=channel_id,
        part="id,snippet,contentDetails",
        maxResults=50,
        fields='nextPageToken,items(id,snippet,contentDetails)'
    )

    playlists = []

    while request:
        response = request.execute()
        playlists.extend(response['items'])
        request = youtube.playlists().list_next(request, response)

    playlists_with_meta = {}
    playlists_with_meta['channel_id'] = channel_id
    playlists_with_meta['retrieved_on'] = str(datetime.now())
    playlists_with_meta['playlists'] = playlists

    save_playlists(channel_id, playlists_with_meta)

    logger.info(
        f"retrieved and saved {len(playlists)} playlists for {channel_id=}")

    return playlists_with_meta


def get_playlists_from_db(channel_id):

    logger = Logger()
    logger.debug(f"getting playlists from db for {channel_id=}")

    return json.loads(get_db().get(f'playlists#{channel_id}'))


def save_playlists(channel_id, playlists, to_disk=True):

    logger = Logger()
    logger.debug(f"saving playlists for {channel_id=}")

    if to_disk:
        with open(os.path.join(DATA_DIR, f"playlists_{channel_id}.json"), "w") as result:
            json.dump(playlists, result, indent=2)

    get_db().set(f'playlists#{channel_id}', json.dumps(playlists))


def get_playlist_items_from_youtube(playlist_id):

    logger = Logger()
    logger.debug(f"getting playlists for {playlist_id=}")

    youtube = youtube_api()

    request = youtube.playlists().list(
        channelId=playlist_id,
        part="id,snippet,contentDetails",
        maxResults=50,
        fields='nextPageToken,items(id,snippet,contentDetails)'
    )

    items = []

    while request:
        response = request.execute()
        items.extend(response['items'])
        request = youtube.playlists().list_next(request, response)

    save_playlist_items(playlist_id, items)

    logger.info(
        f"retrieved and saved {len(items)} items for {playlist_id=}")

    return items


def get_playlist_items_from_db(playlist_id):

    logger = Logger()
    logger.debug(f"getting playlist items from db for {playlist_id=}")

    return json.loads(get_db().get(playlist_id))


def save_playlist_items(playlist_id, items, to_disk=True):

    logger = Logger()
    logger.debug(f"saving playlist items for {playlist_id=}")

    if to_disk:
        with open(os.path.join(DATA_DIR, f"playlist_items_{playlist_id}.json"), "w") as result:
            json.dump(items, result, indent=2)

    get_db().set(playlist_id, json.dumps(items))
