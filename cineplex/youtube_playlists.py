import json
import os
from datetime import datetime
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()


def get_channel_playlists_from_youtube(channel_id):

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
        if 'items' not in response:
            break
        playlists.extend(response['items'])
        request = youtube.playlists().list_next(request, response)

    playlists_with_meta = {}
    playlists_with_meta['channel_id'] = channel_id
    playlists_with_meta['retrieved_on'] = str(datetime.now())
    playlists_with_meta['playlists'] = playlists

    logger.info(
        f"retrieved {len(playlists)} playlists for {channel_id=}")

    return playlists_with_meta


def get_channel_playlists_from_db(channel_id):

    logger = Logger()
    logger.debug(f"getting playlists from db for {channel_id=}")

    return get_db().yt_ch_playlists.find_one({'_id': channel_id})


def save_channel_playlists(channel_id, playlists_with_meta, to_disk=True):

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
        if 'items' not in response:
            break
        items.extend(response['items'])
        request = youtube.playlistItems().list_next(request, response)

    items_with_meta = {}
    items_with_meta['playlist_id'] = playlist_id
    items_with_meta['retrieved_on'] = str(datetime.now())
    items_with_meta['items'] = items

    logger.info(
        f"retrieved {len(items)} items for {playlist_id=}")

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
