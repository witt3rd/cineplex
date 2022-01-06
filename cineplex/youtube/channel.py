from operator import sub
import os
import glob
import pickle
import json
from textwrap import indent
from bson import ObjectId
import os
import re
from datetime import datetime
import time
#
import pymongo
import typer
import yt_dlp
#
from typing import TypeVar, List, Union
from pydantic import BaseModel, Field, HttpUrl
#
import ray
from ray import serve
from fastapi import FastAPI
#
from cineplex.db import PyObjectId, get_db
from cineplex.logger import Logger
from cineplex.config import Settings
from cineplex.utils import (
    IMAGE_EXTS,
    VIDEO_EXTS,
    move_file,
    ensure_batch_impl,
    green,
    blue,
    yellow,
    red,
    magenta
)
from cineplex.youtube.api import youtube_api

settings = Settings()


#
# Paths
#


yt_channels_bkp_dir = os.path.join(settings.bkp_dir, 'yt_channels')
os.makedirs(yt_channels_bkp_dir, exist_ok=True)

yt_channel_playlists_bkp_dir = os.path.join(
    settings.bkp_dir, 'yt_channel_playlists')
os.makedirs(yt_channel_playlists_bkp_dir, exist_ok=True)


#
# Channel model
#

class ChannelId(str):

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError('string required')
        if not (len(v) == 24 and v.startswith('UC')):
            raise ValueError('invalid channel id')
        return cls(v)

    def __repr__(self):
        return f'ChannelId({super().__repr__()})'


class Channel(BaseModel):
    id: str = Field(alias="_id")
    as_of: datetime = str(datetime.now())
    offline: bool = False
    offline_as_of: datetime = None

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

    title: str
    description: str
    key_words: List[str] = []
    published_at: datetime = None
    subscriber_count: int = 0
    view_count: int = 0
    video_count: int = 0
    likes_playlist: str = None
    uploads_playlist: str = None
    thumbnail_url: HttpUrl = None

    @classmethod
    def from_youtube(cls, data):
        channel = data['channel'] if 'channel' in data else data
        snippet = channel['snippet']
        statistics = channel['statistics']
        content_details = channel['contentDetails']
        branding = channel['brandingSettings']
        return cls(
            _id=data.get('_id') if '_id' in data else data.get('id'),
            as_of=data.get('as_of', datetime.utcnow()),
            offline=data.get('offline', False),
            offline_as_of=data.get('offline_as_of'),
            title=snippet.get('title'),
            description=snippet.get('description', None),
            key_words=branding.get('keywords', []),
            published_at=snippet.get('publishedAt', None),
            thumbnail_url=snippet.get('thumbnails', {}).get(
                'high', {}).get('url', None),
            subscriber_count=statistics.get('subscriberCount', 0),
            view_count=statistics.get('viewCount', 0),
            video_count=statistics.get('videoCount', 0),
            likes_playlist=content_details.get(
                'relatedPlaylists').get('likes', None),
            uploads_playlist=content_details.get(
                'relatedPlaylists').get('uploads', None)
        )


#
# Channel functions
#

def show(channel: Channel) -> None:
    typer.echo(
        f"📺 YouTube channel {green(channel.id)} as of {green(channel.as_of)}:")
    typer.echo(f'- Title         : {green(channel.title)}')
    typer.echo(f'- Description   : {green(channel.description)}')
    typer.echo(f'- Key words     : {green(channel.key_words)}')
    typer.echo(f'- Published at  : {green(channel.published_at)}')
    typer.echo(f'- Subscribers   : {green(channel.subscriber_count)}')
    typer.echo(f'- Views         : {green(channel.view_count)}')
    typer.echo(f'- Videos        : {green(channel.video_count)}')
    typer.echo(f'- Likes PL      : {green(channel.likes_playlist)}')
    typer.echo(f'- Uploads PL    : {green(channel.uploads_playlist)}')
    typer.echo(f'- Thumbnail     : {green(channel.thumbnail_url  )}')


def show_batch(channels: List[Channel]) -> None:
    try:
        for channel in channels:
            channel.show()
    except Exception as e:
        Logger().exception(e)


def read_from_db(id: ChannelId) -> Channel:
    try:
        return get_db().yt_channels.find_one({'_id': id})
    except Exception as e:
        Logger().exception(e)


def read_from_db_batch(ids: List[ChannelId]) -> List[Channel]:
    try:
        return list(get_db().yt_channels.find({'_id': {'$in': list(ids)}}))
    except Exception as e:
        Logger().exception(e)


def write_to_db(channel: Channel) -> int:
    try:
        channel.as_of = datetime.utcnow()
        res = get_db().yt_channels.update_one(
            {'_id': channel.id},
            {'$set': channel.dict(exclude_unset=True)},
            upsert=True
        )
        return res.modified_count
    except Exception as e:
        Logger().exception(e)
        return 0


def write_to_db_batch(channels: List[Channel]) -> int:
    return sum([write_to_db(channel) for channel in channels])


def ensure_batch(ids: List[ChannelId], force: bool = False) -> List[Channel]:
    return ensure_batch_impl(ids, read_from_db_batch, read_from_youtube_batch, force)


def delete_from_db(id: ChannelId) -> None:
    """Delete an entity from the database"""
    try:
        x = get_db().yt_channels.delete_one({'_id': id})
        return x.deleted_count
    except Exception as e:
        Logger().error(e)
        return 0


def delete_from_db_batch(ids: List[ChannelId]) -> None:
    """Delete a set of entities from the database"""
    try:
        x = get_db().yt_channels.delete_many({'_id': {'$in': list(ids)}})
        return x.deleted_count
    except Exception as e:
        Logger().error(e)
        return 0


def read_from_youtube(id: ChannelId, save: bool = False) -> Channel:
    """Fetch an entity from YouTube"""
    res = read_from_youtube_batch(list([id]), save)
    return res[0] if res else None


def read_from_youtube_batch(ids: List[ChannelId], save: bool = False) -> List[Channel]:
    """Fetch a set of entities from YouTube"""
    try:
        youtube = youtube_api()

        channel_batch = []

        N = 50
        for i in range(0, len(ids), N):
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics,brandingSettings",
                id=ids[i:i+N],
                maxResults=50,
            )
            while request:
                response = request.execute()
                if 'items' not in response:
                    break
                for item in response['items']:
                    channel = Channel.from_youtube(item)
                    channel_batch.append(channel)
                request = youtube.channels().list_next(request, response)

        if save:
            write_to_db_batch(channel_batch)

        return channel_batch

    except Exception as e:
        Logger().exception(e)


def ensure(Id: Channel, force: bool = False) -> Channel:
    """Ensure an entity exists in the database"""
    pass


def ensure_batch(Ids: List[ChannelId], force: bool = False) -> List[Channel]:
    """Ensure a set of entities exist in the database"""
    pass


def search(query: str) -> List[Channel]:
    """Search for entities in the database"""
    pass

###############################################################################
###############################################################################
###############################TODO############################################
###############################################################################
###############################################################################

#
# Channel playlists
#

# def ensure_youtube_channel_playlists_batch(id_batch, force: bool = False):
#     return ensure_batch(id_batch, yt.get_channel_playlists_from_db_batch, sync_youtube_channel_playlists, force)

# def ensure_youtube_channel_uploads_batch(id_batch, force: bool = False):
#     return ensure_batch(id_batch, yt.get_channel_uploads_from_db_batch, sync_youtube_channel_uploads, force)

# def print_yt_channel_batch(channel_with_meta_batch):
#     for channel_with_meta in channel_with_meta_batch:
#         print_yt_channel(channel_with_meta)

# def print_yt_channel_playlists(channel_playlists_with_meta):
#     channel_id = channel_playlists_with_meta['_id']
#     as_of = channel_playlists_with_meta['as_of']
#     playlists = channel_playlists_with_meta['playlists']

#     typer.echo(
#         f"📝 YouTube playlists for {green(channel_id)} as of {green(as_of)}:")

#     # sort by item count
#     playlists.sort(key=lambda x: x['contentDetails']
#                    ['itemCount'], reverse=True)

#     for playlist in playlists:
#         id = playlist['id']
#         snippet = playlist['snippet']
#         contentDetails = playlist['contentDetails']
#         typer.echo(
#             f'{id}: {green(snippet["title"])} ({blue(contentDetails["itemCount"])})')

# def print_yt_channel_playlists_batch(channel_playlists_with_meta_batch):
#     for channel_playlists_with_meta in channel_playlists_with_meta_batch:
#         print_yt_channel_playlists(channel_playlists_with_meta)

###############################################################################

# def sync_channel_batch(channel_id_batch: List[str]):
#     """Get a channel from YouTube and save it to the database."""
#     channel_id_batch = list(channel_id_batch)
#     print(f"🔄 Syncing {len(channel_id_batch)} channels from YouTube...")

#     channel_with_meta_batch = yt.get_channel_from_youtube_batch(
#         channel_id_batch)
#     if not channel_with_meta_batch:
#         plural = 's' if len(channel_id_batch) > 1 else ''
#         msg = 'Channel' + plural + ' not found'
#         typer.echo(f"❗ {red(msg)}: {green(channel_id_batch)}")
#         return

#     yt.save_channel_to_db_batch(channel_with_meta_batch)
#     typer.echo(f"✅ {green(len(channel_with_meta_batch))} channels synced")

#     return channel_with_meta_batch

# def sync_channel(channel_id: str):
#     res = sync_channel_batch([channel_id])
#     return res[0] if res else None

# def sync_my_channel():
#     """Get my channel from YouTube and save it to the database."""
#     res = sync_channel_batch([settings.my_youtube_channel_id])
#     return res[0] if res else None

# def get_channel_from_youtube(channel_id):
#     channel_with_meta_batch = get_channel_from_youtube_batch([channel_id])
#     if channel_with_meta_batch:
#         return channel_with_meta_batch[0]

# def get_channel_from_youtube_batch(channel_id_batch):

#     try:
#         youtube = youtube_api()

#         channel_with_meta_batch = []

#         N = 50
#         for i in range(0, len(channel_id_batch), N):

#             request = youtube.channels().list(
#                 part="snippet,contentDetails,statistics,brandingSettings",
#                 id=channel_id_batch[i:i+N],
#                 maxResults=50,
#             )

#             while request:
#                 response = request.execute()
#                 if 'items' not in response:
#                     break
#                 for channel in response['items']:
#                     channel_with_meta = {}
#                     channel_with_meta['_id'] = channel['id']
#                     channel_with_meta['as_of'] = str(datetime.now())
#                     channel_with_meta['channel'] = channel
#                     channel_with_meta_batch.append(channel_with_meta)
#                 request = youtube.channels().list_next(request, response)

#         return channel_with_meta_batch

#     except Exception as e:
#         Logger().exception(e)

# def get_all_channel_ids_from_db():

#     try:
#         return [channel['_id'] for channel in get_db().yt_channels.find()]

#     except Exception as e:
#         Logger().exception(e)

# def get_channel_from_db_batch(channel_id_batch):

#     try:
#         channels_cursor = get_db().yt_channels.find(
#             {'_id': {'$in': channel_id_batch}})

#         return list(channels_cursor)

#     except Exception as e:
#         Logger().exception(e)

# def save_channel_to_db(channel_with_meta, to_disk=True):

#     try:
#         channel_id = channel_with_meta['_id']

#         if to_disk:
#             with open(os.path.join(yt_channels_bkp_dir, f"yt_channel_{channel_id}.json"), "w") as result:
#                 json.dump(channel_with_meta, result, indent=2)

#         get_db().yt_channels.update_one(
#             {'_id': channel_id}, {'$set': channel_with_meta}, upsert=True)

#     except Exception as e:
#         Logger().exception(e)

# def save_channel_to_db_batch(channel_with_meta_batch, to_disk=True):

#     try:
#         for channel_with_meta in channel_with_meta_batch:
#             save_channel_to_db(channel_with_meta, to_disk)

#     except Exception as e:
#         Logger().exception(e)

# def get_channel_playlists_from_youtube(channel_id):

#     try:
#         youtube = youtube_api()
#         request = youtube.playlists().list(
#             channelId=channel_id,
#             part="id,snippet,contentDetails",
#             maxResults=50,
#         )

#         playlists = []

#         while request:
#             response = request.execute()
#             if 'items' not in response:
#                 break
#             playlists.extend(response['items'])
#             request = youtube.playlists().list_next(request, response)

#         channel_playlists_with_meta = {}
#         channel_playlists_with_meta['_id'] = channel_id
#         channel_playlists_with_meta['as_of'] = str(datetime.now())
#         channel_playlists_with_meta['playlists'] = playlists

#         return channel_playlists_with_meta

#     except Exception as e:
#         Logger().exception(e)

# def get_channel_playlists_from_youtube_batch(channel_id_batch):
#     channel_playlists_with_meta_batch = []
#     for channel_id in channel_id_batch:
#         channel_playlists_with_meta = get_channel_playlists_from_youtube(
#             channel_id)
#         if channel_playlists_with_meta:
#             channel_playlists_with_meta_batch.append(
#                 channel_playlists_with_meta)
#     return channel_playlists_with_meta_batch

# def get_channel_playlists_from_db(channel_id):

#     try:
#         return get_db().yt_channel_playlists.find_one({'_id': channel_id})

#     except Exception as e:
#         Logger().exception(e)

# def get_channel_playlists_from_db_batch(channel_id_batch):

#     try:
#         channels_cursor = get_db().yt_channel_playlists.find(
#             {'_id': {'$in': channel_id_batch}})

#         return list(channels_cursor)

#     except Exception as e:
#         Logger().exception(e)

# def save_channel_playlists_to_db(channel_playlists_with_meta, to_disk=True):

#     try:
#         channel_id = channel_playlists_with_meta['_id']

#         if to_disk:
#             with open(os.path.join(yt_channel_playlists_bkp_dir, f"yt_channel_playlists_{channel_id}.json"), "w") as result:
#                 json.dump(channel_playlists_with_meta, result, indent=2)

#         get_db().yt_channel_playlists.update_one(
#             {'_id': channel_id},
#             {'$set': channel_playlists_with_meta}, upsert=True)

#     except Exception as e:
#         Logger().exception(e)

# def save_channel_playlists_to_db_batch(channel_playlists_with_meta_batch, to_disk=True):

#     try:
#         for channel_playlists_with_meta in channel_playlists_with_meta_batch:
#             save_channel_playlists_to_db(channel_playlists_with_meta, to_disk)

#     except Exception as e:
#         Logger().exception(e)

# def get_channel_videos_from_db(channel_id):

#     try:
#         return list(get_db().yt_videos.find({'video.channel_id': channel_id, }))

#     except Exception as e:
#         Logger().exception(e)

# def save_offline_channel_to_db(channel_id, as_of: datetime = None, is_auto: bool = False):

#     try:
#         res = get_db().yt_channels.update_one(
#             {'_id': channel_id},
#             {'$set': {'offline': is_auto,
#                       'offline_as_of': as_of if as_of else str(datetime.now())}},
#         )
#         return res.modified_count

#     except Exception as e:
#         Logger().exception(e)

# def get_offline_channels_from_db():


#     try:
#         return list(get_db().yt_channels.find({'offline': True}).sort('offline_as_of', pymongo.DESCENDING))
#     except Exception as e:
#         Logger().exception(e)
###############################################################################
#
# REST API
#
api = FastAPI


###############################################################################


#
# CLI
#

cli = typer.Typer()


@cli.command("show")
def cli_show(id: str) -> None:
    """Show a channel from the database."""
    channel_id = ChannelId.validate(id)
    channel_db = read_from_db(channel_id)
    channel = Channel.from_youtube(channel_db)
    show(channel)
    # channel_id_batch = list(channel_id_batch)
    # channel_with_meta_batch = ensure_youtube_channel_batch(channel_id_batch)
    # if channel_with_meta_batch:
    #     print_yt_channel_batch(channel_with_meta_batch)


@cli.command("show-my")
def cli_show_my():
    """Show my channel from the database."""
    cli_show(settings.my_youtube_channel_id)


@cli.command("update")
def cli_update(id: str) -> None:
    """Update a channel from the database."""
    channel_id = ChannelId.validate(id)
    channel = read_from_youtube(channel_id, save=True)
    if channel:
        typer.echo(f"Channel {channel_id} updated.")


@cli.command("update-my")
def cli_update_my() -> None:
    """Update my channel from the database."""
    cli_update(settings.my_youtube_channel_id)

# @cli.command()
# def sync_youtube_channel_playlists(channel_id_batch: List[str], with_items: bool = False):
#     """Get playlists for a channel from YouTube and save them to the database."""
#     channel_id_batch = list(channel_id_batch)
#     channel_playlists_with_meta_batch = []
#     for channel_id in channel_id_batch:
#         playlists_with_meta = yt.get_channel_playlists_from_youtube(
#             channel_id)
#         if not playlists_with_meta:
#             msg = "Channel doesn't have any playlists"
#             typer.echo(f"❗ {red(msg)}: {green(channel_id)}")
#             return

#         yt.save_channel_playlists_to_db(playlists_with_meta)
#         print_yt_channel_playlists(playlists_with_meta)

#         # for each of the playlists identified for the channel, fetch
#         # the playlist info and possibly the items in the playlist
#         playlist_id_batch = [x['id'] for x in playlists_with_meta['playlists']]

#         sync_youtube_playlist(playlist_id_batch, with_items)

#         channel_playlists_with_meta_batch.append(playlists_with_meta)

#     return channel_playlists_with_meta_batch


# @cli.command()
# def sync_my_youtube_playlists(with_items: bool = False):
#     """Get my playlists from YouTube and save them to the database."""
#     channel_playlists_with_meta_batch = sync_youtube_channel_playlists(
#         [settings.my_youtube_channel_id], with_items)
#     if channel_playlists_with_meta_batch:
#         return channel_playlists_with_meta_batch[0]


# @cli.command()
# def show_youtube_channel_playlists(channel_id_batch: List[str], sync: bool = False):
#     """List playlists for a channel from the database."""
#     channel_id_batch = list(channel_id_batch)
#     channel_playlists_with_meta_batch = ensure_youtube_channel_playlists_batch(
#         channel_id_batch, force=sync)
#     if channel_playlists_with_meta_batch:
#         print_yt_channel_playlists_batch(channel_playlists_with_meta_batch)


# @cli.command()
# def show_my_youtube_playlists(sync: bool = False):
#     """List my playlists from the database."""
#     show_youtube_channel_playlists([settings.my_youtube_channel_id], sync)


# @cli.command()
# def sync_youtube_channel_uploads(channel_id_batch: List[str]):
#     channel_id_batch = list(channel_id_batch)
#     channel_with_meta_batch = ensure_youtube_channel_batch(channel_id_batch)

#     if not channel_with_meta_batch:
#         return

#     playlist_id_batch = [x['channel']['contentDetails']
#                          ['relatedPlaylists']['uploads'] for x in channel_with_meta_batch]

#     return sync_youtube_playlist(playlist_id_batch, with_items=True)


# @cli.command()
# def sync_my_youtube_channel_uploads():
#     """Get my uploads from YouTube and save them to the database."""
#     return sync_youtube_channel_uploads([settings.my_youtube_channel_id])


# @cli.command()
# def show_youtube_channel_uploads(channel_id_batch: List[str]):
#     channel_id_batch = list(channel_id_batch)
#     channel_with_meta_batch = ensure_youtube_channel_batch(channel_id_batch)
#     upload_playlist_id_batch = [
#         x['channel']['contentDetails']['relatedPlaylists']['uploads'] for x in channel_with_meta_batch]
#     return show_youtube_playlist_items(upload_playlist_id_batch)


# @cli.command()
# def show_my_youtube_channel_uploads():
#     """Get my uploads from YouTube and save them to the database."""
#     return show_youtube_channel_uploads([settings.my_youtube_channel_id])


# @cli.command()
# def offline_youtube_channel_uploads(channel_id_batch: List[str] = [], sync: bool = False, auto: bool = False, audit: bool = False):
#     """Get offline channel"""
#     channel_id_batch = list(channel_id_batch)

#     if not channel_id_batch:
#         if not auto:
#             typer.echo(f"❗ {red('No channel id specified')}")
#             return

#         channel_id_batch = [x['_id']
#                             for x in yt.get_offline_channels_from_db()]

#     channel_with_meta_batch = ensure_youtube_channel_batch(channel_id_batch)

#     if not channel_with_meta_batch:
#         return

#     playlist_id_batch = [x['channel']['contentDetails']
#                          ['relatedPlaylists']['uploads'] for x in channel_with_meta_batch]

#     res = offline_youtube_playlist(playlist_id_batch, sync=sync, audit=audit)

#     for id in [x['_id'] for x in channel_with_meta_batch]:
#         yt.save_offline_channel_to_db(id, is_auto=auto)

#     return res


# @cli.command()
# def offline_my_youtube_channel_uploads(sync: bool = False):
#     return offline_youtube_channel_uploads([settings.my_youtube_channel_id], sync)


# @cli.command()
# def offline_youtube_channels_from_file(channels_file: str):
#     """Get channels to offline from file"""
#     offline_from_file(channels_file, ensure_youtube_channel_batch,
#                       yt.save_offline_channel_to_db)


# @cli.command()
# def show_auto_offline_youtube_channels():
#     """List auto offline channels"""
#     print_auto_offline_batch(yt.get_offline_channels_from_db(), 'channel')


# @cli.command()
# def audit_youtube_channel_videos(channel_id_batch: List[str]):
#     """List videos for a channel from the database."""
#     for channel_id in channel_id_batch:
#         channel_with_meta = yt.get_channel_from_db(channel_id)
#         if not channel_with_meta:
#             msg = "Channel not found"
#             typer.echo(f"❗ {red(msg)}: {green(channel_id)}")
#             continue
#         channel_title = channel_with_meta['channel']['snippet']['title']
#         video_with_meta_batch = yt.get_channel_videos_from_db(channel_id)
#         count = 0
#         with typer.progressbar(video_with_meta_batch, label=f'{yellow(channel_title)}', fill_char=typer.style("█", fg="green"), show_pos=True) as bar:
#             for video_with_meta in bar:
#                 id = video_with_meta['_id']
#                 video = video_with_meta['video']
#                 title = video['title']
#                 video_channel_title = video['channel_title']
#                 if video_channel_title != channel_title:
#                     typer.echo(
#                         f"❗ Repairing channel title: {blue(id)} {green(title)} {red(video_channel_title)} != {green(channel_title)}")
#                     video['channel_title'] = channel_title
#                     yt.save_video_to_db(video_with_meta)
#                     count += 1
#         if count:
#             typer.echo(
#                 f"✅ {blue(count)} channel titles repaired for {green(channel_title)}")
#         else:
#             print(f"✅ No channel titles repaired for {green(channel_title)}")

#         _audit_youtube_video(video_with_meta_batch,
#                              repair=True, clean=True, label=channel_title)


# @cli.command()
# def audit_all_youtube_channel_videos():
#     """Audit all videos for a channel from the database."""
#     return(audit_youtube_channel_videos(yt.get_all_channel_ids_from_db()))


###############################################################################

if __name__ == "__main__":
    cli()
