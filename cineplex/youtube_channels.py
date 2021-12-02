import json
import os
from datetime import datetime
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()


def get_channel_from_youtube(channel_ids):

    channel_ids_string = ','.join(channel_ids)

    logger = Logger()
    logger.debug(f"getting channel for {channel_ids_string=}")

    youtube = youtube_api()

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics,brandingSettings",
        id=channel_ids_string,
        maxResults=50,
    )

    channels = []

    while request:
        response = request.execute()
        channels.extend(response['items'])
        request = youtube.channels().list_next(request, response)

    for channel in channels:
        playlists_with_meta = {}
        playlists_with_meta['channel_id'] = channel['id']
        playlists_with_meta['retrieved_on'] = str(datetime.now())
        playlists_with_meta[''] = playlists

        save_playlists(channel_id, playlists_with_meta)

    logger.info(
        f"retrieved and saved {len(playlists)} playlists for {channel_id=}")

    return playlists_with_meta


def get_playlists_from_db(channel_id):

    logger = Logger()
    logger.debug(f"getting playlists from db for {channel_id=}")

    return json.loads(get_db().get(f'playlists#{channel_id}'))


def save_playlists(channel_id, playlists_with_meta, to_disk=True):

    logger = Logger()
    logger.debug(f"saving playlists for {channel_id=}")

    if to_disk:
        with open(os.path.join(settings.data_dir, f"playlists_{channel_id}.json"), "w") as result:
            json.dump(playlists_with_meta, result, indent=2)

    get_db().set(f'playlists#{channel_id}', json.dumps(playlists_with_meta))


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
        items.extend(response['items'])
        request = youtube.playlistItems().list_next(request, response)

    items_with_meta = {}
    items_with_meta['playlist_id'] = playlist_id
    items_with_meta['retrieved_on'] = str(datetime.now())
    items_with_meta['items'] = items

    save_playlist_items(playlist_id, items_with_meta)

    logger.info(
        f"retrieved and saved {len(items)} items for {playlist_id=}")

    return items_with_meta


def get_playlist_items_from_db(playlist_id):

    logger = Logger()
    logger.debug(f"getting playlist items from db for {playlist_id=}")

    return json.loads(get_db().get(f'playlist_items#{playlist_id}'))


def save_playlist_items(playlist_id, items_with_meta, to_disk=True):

    logger = Logger()
    logger.debug(f"saving playlist items for {playlist_id=}")

    if to_disk:
        with open(os.path.join(settings.data_dir, f"playlist_items_{playlist_id}.json"), "w") as result:
            json.dump(items_with_meta, result, indent=2)

    get_db().set(f'playlist_items#{playlist_id}', json.dumps(items_with_meta))
