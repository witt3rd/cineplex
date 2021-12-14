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

yt_channels_bkp_dir = os.path.join(settings.bkp_dir, 'yt_channels')
os.makedirs(yt_channels_bkp_dir, exist_ok=True)

yt_channel_playlists_bkp_dir = os.path.join(
    settings.bkp_dir, 'yt_channel_playlists')
os.makedirs(yt_channel_playlists_bkp_dir, exist_ok=True)


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


def save_offline_channel_to_db(channel_id, as_of: datetime = None):

    try:
        res = get_db().yt_channels.update_one(
            {'_id': channel_id},
            {'$set': {'offline': True,
                      'offline_as_of': as_of if as_of else str(datetime.now())}},
        )
        return res.modified_count

    except Exception as e:
        Logger().exception(e)


def get_offline_channels_from_db():

    try:
        return list(get_db().yt_channels.find({'offline': True}))

    except Exception as e:
        Logger().exception(e)
