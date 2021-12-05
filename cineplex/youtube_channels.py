from cmath import log
import json
import os
from datetime import datetime
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()


def get_channels_from_youtube(channel_ids):

    # if channel_ids is a list
    if isinstance(channel_ids, str):
        channel_ids_string = channel_ids
    else:
        channel_ids_string = ','.join(channel_ids)

    logger = Logger()
    logger.debug(f"getting channel for {channel_ids_string=}")

    youtube = youtube_api()

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics,brandingSettings",
        id=channel_ids,
        maxResults=50,
    )

    channels_with_meta = []

    while request:
        response = request.execute()
        for channel in response['items']:
            channel_with_meta = {}
            channel_with_meta['channel_id'] = channel['id']
            channel_with_meta['retrieved_on'] = str(datetime.now())
            channel_with_meta['channel'] = channel
            channels_with_meta.append(channel_with_meta)
        request = youtube.channels().list_next(request, response)

    logger.info(
        f"retrieved {len(channels_with_meta)} channels for {channel_ids_string=}")

    return channels_with_meta


def get_channel_from_db(channel_id):
    logger = Logger()
    logger.debug(f"getting channel for {channel_id=}")

    channel = get_db().yt_channel_info.find_one({'_id': channel_id})
    logger.debug(f"got channel {channel=}")

    return channel


def save_channel(channel_with_meta, to_disk=True):
    channel_id = channel_with_meta['channel_id']

    logger = Logger()
    logger.debug(f"saving channel for {channel_id=}")

    if to_disk:
        dir = os.path.join(settings.data_dir, "channels")
        os.makedirs(dir, exist_ok=True)
        with open(os.path.join(dir, f"channel_{channel_id}.json"), "w") as result:
            json.dump(channel_with_meta, result, indent=2)

    info = channel_with_meta.copy()
    info['_id'] = channel_id
    del info['channel_id']

    get_db().yt_channel_info.update_one(
        {'_id': info['_id']}, {'$set': info}, upsert=True)


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
    # items_with_meta['retrieved_on'] = str(datetime.now())
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
    pass


def get_channel_playlists_from_db(channel_id):
    pass


def save_channel_playlists(channel_id, playlists_with_meta, to_disk=True):
    pass
