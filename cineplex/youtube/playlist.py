from operator import sub
import os
import glob
import pickle
import json
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
from typing import TypeVar, List, Set, Union
from pydantic import BaseModel, Field
from functools import singledispatch
#
import ray
from ray import serve
from fastapi import FastAPI
#
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
#
from cineplex.db import PyObjectId, get_db

from cineplex.logger import Logger
from cineplex.config import Settings
from cineplex.utils import (
    IMAGE_EXTS,
    VIDEO_EXTS,
    move_file,
    ensure_batch,
    green,
    blue,
    yellow,
    red,
    magenta
)

cli = typer.Typer()


settings = Settings()

#
# Paths
#

yt_playlists_bkp_dir = os.path.join(settings.bkp_dir, 'yt_playlists')
os.makedirs(yt_playlists_bkp_dir, exist_ok=True)


#
# Playlist model
#


class PlaylistId(str):

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError('string required')
        if not ((len(v) == 34 and v.startswith('PL')) or (len(v) == 24 and v.startswith('FL'))):
            raise ValueError('invalid playlist id')
        return cls(v)

    def __repr__(self):
        return f'PlaylistId({super().__repr__()})'


#
# Playlists
#


def ensure_youtube_playlist_batch(id_batch):
    return ensure_batch(id_batch, yt.get_playlist_from_db_batch, sync_youtube_playlist)


@cli.command()
def sync_youtube_playlist(playlist_id_batch: List[str], with_items: bool = False):
    """Get playlist info from YouTube and save it to the database."""
    playlist_id_batch = list(playlist_id_batch)
    print(f"🔄 Syncing {len(playlist_id_batch)} playlists from YouTube...")

    playlist_with_meta_batch = yt.get_playlist_from_youtube_batch(
        playlist_id_batch)
    if not playlist_with_meta_batch:
        plural = 's' if len(playlist_id_batch) > 1 else ''
        msg = 'Playlist' + plural + ' not found'
        typer.echo(f"❗ {red(msg)}: {green(playlist_id_batch)}")
        return

    yt.save_playlist_to_db_batch(playlist_with_meta_batch)
    typer.echo(f"✅ {green(len(playlist_with_meta_batch))} plalists synced")

    if with_items:
        return sync_youtube_playlist_items(playlist_id_batch)

    return playlist_with_meta_batch


@cli.command()
def show_youtube_playlist(playlist_id_batch: List[str]):
    """List playlist info from the database."""
    playlist_id_batch = list(playlist_id_batch)
    playlist_with_meta_batch = ensure_youtube_playlist_batch(
        playlist_id_batch)
    if playlist_with_meta_batch:
        print_yt_playlist_batch(playlist_with_meta_batch)


@cli.command()
def offline_youtube_playlist(playlist_id_batch: List[str] = [], sync: bool = False, auto: bool = False, audit: bool = False):
    """Get offline playlist"""
    playlist_id_batch = list(playlist_id_batch)

    if not playlist_id_batch:
        if not auto:
            typer.echo(f"❗ {red('Playlist id not found')}")
            return

        playlist_id_batch = [x['_id']
                             for x in yt.get_offline_playlists_from_db()]

    typer.echo(f"💡 Offlining {blue(len(playlist_id_batch))} playlist(s)")

    playlist_items_with_meta_batch = sync_youtube_playlist(
        playlist_id_batch, with_items=True) if sync else ensure_youtube_playlist_items_batch(playlist_id_batch)

    if not playlist_items_with_meta_batch:
        return

    video_id_batch = []
    for playlist_items_with_meta in playlist_items_with_meta_batch:
        for item in playlist_items_with_meta['items']:
            video_id_batch.append(item['snippet']['resourceId']['videoId'])

    if not video_id_batch:
        msg = "No videos found"
        typer.echo(f"❗ {red(msg)}: {green(playlist_id_batch)}")
        return

    res = offline_youtube_video(video_id_batch, audit=audit)

    for id in [x['_id'] for x in playlist_items_with_meta_batch]:
        yt.save_offline_playlist_to_db(id, is_auto=auto)

    return res


@cli.command()
def offline_my_youtube_playlists(sync: bool = False):
    """Get my playlists"""

    playlist_with_meta_batch = sync_my_youtube_playlists() if sync else ensure_youtube_channel_playlists_batch(
        [settings.my_youtube_channel_id])
    if not playlist_with_meta_batch:
        typer.echo(f"💡 {yellow('You have no playlists')}")
        return

    return offline_youtube_playlist([x['id'] for x in playlist_with_meta_batch['playlists']], sync)


@cli.command()
def offline_youtube_playlists_from_file(playlist_file: str):
    """Get playlists to offline from file"""
    offline_from_file(playlist_file, ensure_youtube_playlist_batch,
                      yt.save_offline_playlist_to_db)


@cli.command()
def show_auto_offline_youtube_playlists():
    """List auto offline playlists"""
    print_auto_offline_batch(yt.get_offline_playlists_from_db(), 'playlist')


@cli.command()
def update_youtube_playlist_from_channel(target_playlist_id: str, source_channel_id: str, as_of: str = None, sync: bool = False, auto: bool = False):
    """Merge two playlists"""
    target_playlist_items_with_meta_batch = sync_youtube_playlist(
        [target_playlist_id], with_items=True)[0] if sync else ensure_youtube_playlist_items(target_playlist_id)

    source_playlist_items_with_meta_batch = sync_youtube_channel_uploads(
        [source_playlist_id], with_items=True)[0] if sync else ensure_youtube_ch(source_playlist_id)
    if not source_playlist_items_with_meta_batch:
        typer.echo(f"💡 {yellow('Source playlist not found or has no items')}")
        return

    typer.echo(
        f"💡 Merging {blue(len(source_playlist_items_with_meta_batch))} items from {blue(source_playlist_id)} to {blue(target_playlist_id)} {blue(len(target_playlist_items_with_meta_batch))} items")

#
# Playlist items
#


def ensure_youtube_playlist_items(id):
    res = ensure_youtube_playlist_items_batch([id])
    return res[0] if res else None


def ensure_youtube_playlist_items_batch(id_batch):
    # ensure that playlist items also has a valid playlist
    playlist_with_meta_batch = ensure_youtube_playlist_batch(id_batch)
    if playlist_with_meta_batch:
        valid_playlist_id_batch = [x['_id'] for x in playlist_with_meta_batch]
        return ensure_batch(valid_playlist_id_batch, yt.get_playlist_items_from_db_batch, sync_youtube_playlist_items)


@cli.command()
def sync_youtube_playlist_items(playlist_id: List[str]):
    """Get playlist items for a playlist"""
    playlist_id_batch = list(playlist_id)
    playlist_items_with_meta_batch = []

    with typer.progressbar(playlist_id_batch, label='Syncing playlist items', fill_char=typer.style("█", fg="green"), show_pos=True) as bar:
        for playlist_id in bar:
            playlist_items_with_meta = yt.get_playlist_items_from_youtube(
                playlist_id)
            if not playlist_items_with_meta:
                msg = "Playlist not found"
                typer.echo(f"❗ {red(msg)}: {green(playlist_id)}")
                return

            yt.save_playlist_items_to_db(playlist_items_with_meta)

            playlist_items_with_meta_batch.append(playlist_items_with_meta)

    return playlist_items_with_meta_batch


@cli.command()
def show_youtube_playlist_items(playlist_id_batch: List[str]):
    """List playlist items for a playlist"""
    playlist_id_batch = list(playlist_id_batch)
    playlist_items_with_meta_batch = ensure_youtube_playlist_items_batch(
        playlist_id_batch)
    if playlist_items_with_meta_batch:
        print_yt_playlist_items_batch(playlist_items_with_meta_batch)


#
# Videos
#


@cli.command()
def show_youtube_video(video_id_batch: List[str]):
    """Show a video from the database."""
    video_id_batch = list(video_id_batch)
    video_with_meta_batch = yt.get_video_from_db_batch(video_id_batch)
    missing, found = missing_found(video_id_batch, video_with_meta_batch)
    if found:
        print_yt_video_batch(
            [x for x in video_with_meta_batch if x['_id'] in found])
    if missing:
        plural = 's' if len(video_id_batch) > 1 else ''
        typer.echo(
            f"💡 {yellow('Video' + plural + ' not in db')} {green(missing)}")


def _refresh_video_info(info_file):

    video_with_meta = yt.extract_video_info_from_file(info_file)
    if video_with_meta is None:
        return info_file

    yt.save_video_to_db(video_with_meta, False)
    return None


def _delete_youtube_video(video_with_meta):
    yt.delete_video_files(video_with_meta)
    yt.delete_video_from_db(video_with_meta['_id'])


@cli.command()
def delete_youtube_video(video_id_batch: List[str]):
    """Delete a video from the database and its files."""
    video_with_meta_batch = yt.get_video_from_db_batch(video_id_batch)
    if not video_with_meta_batch:
        typer.echo(f'{red("❗ No video(s) to delete")}')
        return

    typer.echo(
        f'deleting {blue(len(video_with_meta_batch))} video(s)')

    typer.confirm('Do you want to continue?', abort=True)

    with typer.progressbar(video_with_meta_batch, label='Deleting', fill_char=typer.style(
            "█", fg="red"),
            show_percent=True, show_pos=True, show_eta=True) as video_with_meta_bar:
        for video_with_meta in video_with_meta_bar:
            _delete_youtube_video(video_with_meta)


def _download_youtube_video(video_id, show_progress=True):
    video_url = f'https://www.youtube.com/watch?v={video_id}'
    video_with_meta = yt.get_video_from_youtube(video_url, show_progress)
    if video_with_meta:
        yt.save_video_to_db(video_with_meta)
    return video_with_meta


@ray.remote
def _download_youtube_video_ray(video_id):
    return _download_youtube_video(video_id, False)


@cli.command()
def offline_youtube_video(video_id_batch: List[str], force: bool = False, audit: bool = False):
    """Download a video from YouTube and place it in its channel's folder."""
    video_id_batch = list(video_id_batch)

    if not force:
        typer.echo(f"💡 Processing {blue(len(video_id_batch))} video(s)")
        video_with_meta_batch = yt.get_video_from_db_batch(video_id_batch)
        missing, found = missing_found(video_id_batch, video_with_meta_batch)
        if audit:
            missing.extend(audit_youtube_video(found, repair=True, clean=True))

        verified_with_meta_batch = [
            x for x in video_with_meta_batch if x['_id'] not in missing]

        typer.echo(
            f"✅ {blue(len(verified_with_meta_batch))} verified {red(len(missing))} missing")

        if not missing:
            return verified_with_meta_batch
    else:
        missing = video_id_batch
        verified_with_meta_batch = []

    count = len(missing)
    plural = 's' if count > 1 else ''
    typer.echo(
        f"💡 {yellow('Downloading')} {blue(count)} video" + plural)

    if len(missing) > 1:
        ray.init()
        futures = [_download_youtube_video_ray.remote(x) for x in missing]
        dl_video_with_meta_batch = [x for x in ray.get(futures) if x]
    else:
        res = _download_youtube_video(missing[0])
        dl_video_with_meta_batch = [res] if res else []

    not_dl_id_batch, _ = missing_found(missing, dl_video_with_meta_batch)

    if not_dl_id_batch:
        typer.echo(
            f"❗ {red('Unable to download')} {blue(len(not_dl_id_batch))}: {green(not_dl_id_batch)}")

    if dl_video_with_meta_batch:
        count = len(missing)
        plural = 's' if count > 1 else ''
        typer.echo(f"✅  Downloaded {blue(count)} video" + plural)
        for dl_video_with_meta in dl_video_with_meta_batch:
            id = dl_video_with_meta['_id']
            video = dl_video_with_meta['video']
            title = video['title']
            typer.echo(f"⬇️  {blue(id)} {green(title)}")
            verified_with_meta_batch.append(dl_video_with_meta)
    else:
        typer.echo(f"✅  Downloaded {red(0)} videos")

    return verified_with_meta_batch


#
# Search
#

@cli.command()
def search(query):
    """Search videos in the database"""
    video_with_meta_batch = yt.search_db(query)
    if not video_with_meta_batch:
        typer.echo(f'{red("❗ No video(s) to found")}')
        return

    typer.echo(
        f'found {blue(len(video_with_meta_batch))} video(s) with metadata')

    for video_with_meta in video_with_meta_batch:
        video_id = video_with_meta['_id']
        title = video_with_meta['video']['title']
        typer.echo(f'✅ {blue(video_id)} {green(title)}')


#
# Audit (data integrity)
#

def _audit_youtube_video(video_with_meta_batch: List[str], repair: bool = False, clean: bool = False, label=None):

    missing = []

    if not video_with_meta_batch:
        typer.echo(f'{red("❗ No video(s) to audit")}')
        return missing

    label = label or 'Auditing'
    with typer.progressbar(video_with_meta_batch, label=f'{yellow(label)}', fill_char=typer.style("█", fg="green"), show_pos=True) as bar:

        unrepaired = 0
        repaired = 0
        cleaned = 0
        for video_with_meta in bar:
            video_id = video_with_meta['_id']
            title = video_with_meta['video']['title']
            if yt.audit_video_files(video_with_meta):
                continue
            if repair:
                new_video_with_meta = _download_youtube_video(video_id)
                if new_video_with_meta:
                    typer.echo(
                        f'✅ {blue(video_id)} {green(title)} Repaired missing files')
                    repaired += 1
                else:
                    typer.echo(
                        f'❗ {blue(video_id)} {green(title)} {red("Unable to repair video")}')
                    unrepaired += 1
                    missing.append(video_id)
                    if clean:
                        _delete_youtube_video(video_with_meta)
                        typer.echo(
                            f'🗑 {blue(video_id)} {green(title)} Cleaned')
                        cleaned += 1
            else:
                typer.echo(
                    f'❗ {blue(video_id)} {green(title)} {red("Missing files (no repair)")}')
                if clean:
                    _delete_youtube_video(video_with_meta)
                    typer.echo(f'🗑 {blue(video_id)} {green(title)} Cleaned')
                    cleaned += 1
    typer.echo(
        f"✅ {red(unrepaired)} unrepaired, {green(repaired)} repaired, {yellow(cleaned)} cleaned")

    return missing


@cli.command()
def audit_youtube_video(video_id_batch: List[str], repair: bool = False, clean: bool = False, label=None):
    """Audit videos in the database"""
    return _audit_youtube_video(yt.get_video_from_db_batch(video_id_batch), repair, clean, label)


@cli.command()
def audit_all_youtube_videos():
    """Audit videos in the database"""
    channel_files = get_all_files(settings.youtube_channels_dir)
    with open(os.path.join(settings.data_dir, 'channel_files.json'), 'r') as infile:
        channel_files = json.load(infile)

    with open(os.path.join(settings.data_dir, 'channel_files.json'), 'w') as outfile:
        json.dump(channel_files, outfile)
    # with open(os.path.join(settings.data_dir, 'bad_metadata.json'), 'r') as infile:
    #     channel_files = json.load(infile)

    typer.echo(
        f'found {blue(len(channel_files))} files in {green(settings.youtube_channels_dir)}')

    info_files = [x for x in channel_files if x.endswith('.info.json')]

    typer.echo(f'found {blue(len(info_files))} metadata files')

    bad_metadata = []
    for x in info_files:
        res = _refresh_video_info(x)  # .remote(x)
        if not res:
            typer.echo(f'✅ Refreshed {green(x)}')
        else:
            typer.echo(
                f'❗ {red("Unable to refresh video")} {blue(x)}')
            bad_metadata.append(res)

    with open(os.path.join(settings.data_dir, 'bad_metadata.json'), 'w') as outfile:
        json.dump(bad_metadata, outfile)

    typer.echo(f'found {blue(len(bad_metadata))} bad metadata')


@cli.command()
def audit_youtube_channel_videos(channel_id_batch: List[str]):
    """List videos for a channel from the database."""
    for channel_id in channel_id_batch:
        channel_with_meta = yt.get_channel_from_db(channel_id)
        if not channel_with_meta:
            msg = "Channel not found"
            typer.echo(f"❗ {red(msg)}: {green(channel_id)}")
            continue
        channel_title = channel_with_meta['channel']['snippet']['title']
        video_with_meta_batch = yt.get_channel_videos_from_db(channel_id)
        count = 0
        with typer.progressbar(video_with_meta_batch, label=f'{yellow(channel_title)}', fill_char=typer.style("█", fg="green"), show_pos=True) as bar:
            for video_with_meta in bar:
                id = video_with_meta['_id']
                video = video_with_meta['video']
                title = video['title']
                video_channel_title = video['channel_title']
                if video_channel_title != channel_title:
                    typer.echo(
                        f"❗ Repairing channel title: {blue(id)} {green(title)} {red(video_channel_title)} != {green(channel_title)}")
                    video['channel_title'] = channel_title
                    yt.save_video_to_db(video_with_meta)
                    count += 1
        if count:
            typer.echo(
                f"✅ {blue(count)} channel titles repaired for {green(channel_title)}")
        else:
            print(f"✅ No channel titles repaired for {green(channel_title)}")

        _audit_youtube_video(video_with_meta_batch,
                             repair=True, clean=True, label=channel_title)


@cli.command()
def audit_all_youtube_channel_videos():
    """Audit all videos for a channel from the database."""
    return(audit_youtube_channel_videos(yt.get_all_channel_ids_from_db()))


@cli.command()
def repair_all():

    with open(os.path.join(settings.data_dir, 'bad_db_videos.json'), 'r') as infile:
        bad = json.load(infile)

    for video_id in bad:
        audit_youtube_video([video_id], repair=True, clean=True)


@cli.command()
def audit_youtube_db(repair: bool = False, clean: bool = False):
    """Audit videos in the database"""
    for video in yt.get_videos_for_audit():
        if not yt.audit_video_files(video):
            audit_youtube_video(video['_id'], repair, clean)


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


# def print_yt_playlist(playlist_with_meta):
#     playlist_id = playlist_with_meta['_id']
#     as_of = playlist_with_meta['as_of']
#     playlist = playlist_with_meta['playlist']
#     snippet = playlist['snippet']
#     contentDetails = playlist['contentDetails']

#     typer.echo(
#         f"📝 YouTube playlist {green(playlist_id)} as of {green(as_of)}:")
#     typer.echo(f"- Title        : {green(snippet['title'])}")
#     typer.echo(f"- Published at : {green(snippet['publishedAt'])}")
#     typer.echo(f"- Description  : {green(snippet['description'])}")
#     typer.echo(f"- Channel title: {green(snippet['channelTitle'])}")
#     typer.echo(f"- Item count   : {green(contentDetails['itemCount'])}")


# def print_yt_playlist_batch(playlist_with_meta_batch):
#     for playlist_with_meta in playlist_with_meta_batch:
#         print_yt_playlist(playlist_with_meta)


# def print_yt_playlist_items(playlist_items_with_meta):
#     playlist_id = playlist_items_with_meta['_id']
#     as_of = playlist_items_with_meta['as_of']
#     items = playlist_items_with_meta['items']

#     typer.echo(
#         f"📝 YouTube playlist items for {green(playlist_id)} as of {blue(as_of)}:")

#     # sort items by position
#     items.sort(key=lambda x: x['snippet']['position'])

#     for item in items:
#         snippet = item['snippet']
#         pos = blue(f"{snippet['position']:04d}")
#         video_id = green(snippet['resourceId']['videoId'])
#         channel_title = yellow(snippet['channelTitle'])
#         title = green(snippet['title'])
#         published_at = blue(f"{snippet['publishedAt']}")
#         typer.echo(
#             f"{pos}) {video_id}: {channel_title}: {title} @ {published_at}")


# def print_yt_playlist_items_batch(playlist_items_with_meta_batch):
#     for playlist_items_with_meta in playlist_items_with_meta_batch:
#         print_yt_playlist_items(playlist_items_with_meta)


# def print_yt_video(video_with_meta):

#     id = green(video_with_meta['_id'])
#     video = video_with_meta['video']
#     files = video['files']
#     title = green(video['title'])
#     description = green(video['description'])
#     tags = green(video['tags'])
#     categories = green(video['categories'])
#     channel_id = green(video['channel_id'])
#     channel_title = green(video['channel_title'])
#     uploader = green(video['uploader'])
#     uploader_id = green(video['uploader_id'])
#     upload_date = blue(video['upload_date'])
#     duration_seconds = green(video['duration_seconds'])
#     view_count = green(video['view_count'])
#     like_count = green(video['like_count'])
#     dislike_count = green(video['dislike_count'])
#     average_rating = green(video['average_rating'])
#     video_filename = green(files['video_filename'])
#     info_filename = green(files['info_filename'])
#     thumbnail_filename = green(files['thumbnail_filename'])

#     typer.echo(
#         f"📼 {id}: {title} @ {upload_date}")
#     typer.echo(f"- Description  : {description}")
#     typer.echo(f"- Tags         : {tags}")
#     typer.echo(f"- Categories   : {categories}")
#     typer.echo(f"- Channel      : {channel_id} ({channel_title})")
#     typer.echo(f"- Uploader     : {uploader} ({uploader_id})")
#     typer.echo(f"- Duration     : {duration_seconds}s")
#     typer.echo(f"- Views        : {view_count}")
#     typer.echo(f"- Likes        : 👍🏻 {like_count} / 👎🏻 {dislike_count}")
#     typer.echo(f"- Rating       : {average_rating}")
#     typer.echo(f"- Video file   : {video_filename}")
#     typer.echo(f"- Info file    : {info_filename}")
#     typer.echo(f"- Thumbnail    : {thumbnail_filename}")


# def print_yt_video_batch(video_with_meta_batch):
#     for video_with_meta in video_with_meta_batch:
#         print_yt_video(video_with_meta)


# def print_auto_offline_batch(item_with_meta_batch, kind: str):
#     if not item_with_meta_batch:
#         typer.echo(f"💡 {yellow('You have no auto offline ' + kind + 's')}")
#         return

#     # item_with_meta_batch.sort(key=lambda x: x['offline_as_of'])

#     for item_with_meta in item_with_meta_batch:
#         id = item_with_meta['_id']
#         title = item_with_meta[kind]['snippet']['title']
#         as_of = item_with_meta['offline_as_of']
#         typer.echo(f"{magenta(as_of)} {blue(id)} {green(title)}")


# @singledispatch
# def ensure(Id, force: bool = False):
#     raise NotImplementedError(Id)

#
# Channels
#

@show.register
def show(entity: Channel) -> None:
    typer.echo(
        f"📺 YouTube channel {green(entity.id)} as of {green(entity.as_of)}:")
    typer.echo(f'- Title       : {green(entity.title)}')
    typer.echo(f'- Description : {green(entity.description)}')
    typer.echo(f'- Published at: {green(entity.publishedAt)}')
    typer.echo(f'- Subscribers : {green(entity.subscriberCount)}')
    typer.echo(f'- Views       : {green(entity.viewCount)}')
    typer.echo(f'- Videos      : {green(entity.videoCount)}')

    # brandingSettings = channel['brandingSettings']
    # branding_channel = brandingSettings['channel']
    # if 'title' in branding_channel:
    #     typer.echo(
    #         f"- Title (B)   : {green(branding_channel['title'])}")
    # if 'description' in branding_channel:
    #     typer.echo(
    #         f"- Desc. (B)   : {green(branding_channel['description'])}")
    # if 'keywords' in branding_channel:
    #     typer.echo(
    #         f"- Keywords (B): {green(branding_channel['keywords'])}")


@get_from_db.register(ChannelId)
def _(id: ChannelId) -> Channel:

    try:
        return get_db().yt_channels.find_one({'_id': id})

    except Exception as e:
        Logger().exception(e)

# @ensure.register(ChannelId)


def ensure_channel(id, force: bool = False):
    return ensure_batch(id_batch, yt.get_channel_from_db_batch, sync_youtube_channel_batch, force)


def sync_channel_batch(channel_id_batch: List[str]):
    """Get a channel from YouTube and save it to the database."""
    channel_id_batch = list(channel_id_batch)
    print(f"🔄 Syncing {len(channel_id_batch)} channels from YouTube...")

    channel_with_meta_batch = yt.get_channel_from_youtube_batch(
        channel_id_batch)
    if not channel_with_meta_batch:
        plural = 's' if len(channel_id_batch) > 1 else ''
        msg = 'Channel' + plural + ' not found'
        typer.echo(f"❗ {red(msg)}: {green(channel_id_batch)}")
        return

    yt.save_channel_to_db_batch(channel_with_meta_batch)
    typer.echo(f"✅ {green(len(channel_with_meta_batch))} channels synced")

    return channel_with_meta_batch


def sync_channel(channel_id: str):
    res = sync_channel_batch([channel_id])
    return res[0] if res else None


def sync_my_channel():
    """Get my channel from YouTube and save it to the database."""
    res = sync_channel_batch([settings.my_youtube_channel_id])
    return res[0] if res else None


def get_channel_from_youtube(channel_id):
    channel_with_meta_batch = get_channel_from_youtube_batch([channel_id])
    if channel_with_meta_batch:
        return channel_with_meta_batch[0]


def get_channel_from_youtube_batch(channel_id_batch):

    try:
        youtube = youtube_api()

        channel_with_meta_batch = []

        N = 50
        for i in range(0, len(channel_id_batch), N):

            request = youtube.channels().list(
                part="snippet,contentDetails,statistics,brandingSettings",
                id=channel_id_batch[i:i+N],
                maxResults=50,
            )

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


def get_all_channel_ids_from_db():

    try:
        return [channel['_id'] for channel in get_db().yt_channels.find()]

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


def get_channel_videos_from_db(channel_id):

    try:
        return list(get_db().yt_videos.find({'video.channel_id': channel_id, }))

    except Exception as e:
        Logger().exception(e)


def save_offline_channel_to_db(channel_id, as_of: datetime = None, is_auto: bool = False):

    try:
        res = get_db().yt_channels.update_one(
            {'_id': channel_id},
            {'$set': {'offline': is_auto,
                      'offline_as_of': as_of if as_of else str(datetime.now())}},
        )
        return res.modified_count

    except Exception as e:
        Logger().exception(e)


def get_offline_channels_from_db():

    try:
        return list(get_db().yt_channels.find({'offline': True}).sort('offline_as_of', pymongo.DESCENDING))

    except Exception as e:
        Logger().exception(e)


#
# Playlists
#


def get_playlist_from_youtube(playlist_id):

    try:
        playlist_with_meta = get_playlist_from_youtube_batch([playlist_id])[0]
        return playlist_with_meta

    except Exception as e:
        Logger().exception(e)


def get_playlist_from_youtube_batch(playlist_id_batch):

    try:
        youtube = youtube_api()

        playlist_with_meta_batch = []

        N = 50
        for i in range(0, len(playlist_id_batch), N):

            request = youtube.playlists().list(
                id=playlist_id_batch[i:i+N],
                part="id,snippet,contentDetails",
                maxResults=50,
            )

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


def save_offline_playlist_to_db(playlist_id, as_of: datetime = None, is_auto: bool = False):

    try:
        res = get_db().yt_playlists.update_one(
            {'_id': playlist_id},
            {'$set': {'offline': is_auto,
                      'offline_as_of': as_of if as_of else str(datetime.now())}},
        )
        return res.modified_count

    except Exception as e:
        Logger().exception(e)


def get_offline_playlists_from_db():

    try:
        return list(get_db().yt_playlists.find({'offline': True}).sort('offline_as_of', pymongo.DESCENDING))

    except Exception as e:
        Logger().exception(e)


def add_item_to_youtube_playlist(playlist_id, item_id):

    try:
        youtube = youtube_api()
        request = youtube.playlistItems().insert(
            part="snippet",
            body={
                "snippet": {
                    "playlistId": playlist_id,
                    "resourceId": {
                        "kind": "youtube#video",
                        "videoId": item_id
                    }
                }
            }
        )
        res = request.execute()

        Logger().info(
            f"Added item {item_id} to playlist {playlist_id}: {res}")

        return res

    except Exception as e:
        Logger().exception(e)


def add_item_to_youtube_playlist_batch(playlist_id, item_id_batch):

    try:
        for item_id in item_id_batch:
            res = add_item_to_youtube_playlist(playlist_id, item_id)
            print(
                f"Batch: Added item {item_id} to playlist {playlist_id}: {res}")
            # avoid rate limit
            time.sleep(1)

    except Exception as e:
        Logger().exception(e)


def get_playlist_merges_from_db(target_playlist_id=None):

    try:
        return list(get_db().yt_playlist_merges.find({'target_playlist_id': target_playlist_id}))

    except Exception as e:
        Logger().exception(e)


def add_playlist_merge_to_db(target_playlist_id, source_playlist_id, as_of: datetime = None):

    try:
        res = get_db().yt_playlist_merges.insert_one(
            {'target_playlist_id': target_playlist_id,
                'source_playlist_id': source_playlist_id},
            {'$set': {'as_of': as_of if as_of else str(datetime.now())}},
        )
        return res.inserted_id

    except Exception as e:
        Logger().exception(e)


def update_playlist_merge_to_db(target_playlist_id, source_playlist_id, as_of: datetime = None):

    try:
        res = get_db().yt_playlist_merges.update_one(
            {'target_playlist_id': target_playlist_id,
                'source_playlist_id': source_playlist_id},
            {'$set': {'as_of': as_of if as_of else str(datetime.now())}},
        )
        return res.modified_count

    except Exception as e:
        Logger().exception(e)

    try:
        get_db().yt_playlist_merges.update_one(
            {'target_playlist_id': target_playlist_id,
             'source_playlist_id': source_playlist_id},
            {'$set': {'status': status}}
        )

    except Exception as e:
        Logger().exception(e)
#
# Videos
#


def _expand_video_files(video_with_meta):

    try:
        video = video_with_meta['video']
        files = video['files']

        channel_title = video['channel_title'] if 'channel_title' in video else None
        if not channel_title:
            Logger().error(
                f"No channel title for {video_with_meta['_id']} ({video['channel_id']})")
            return None, None, None

        video_filename = os.path.join(settings.youtube_channels_dir,
                                      channel_title,
                                      files['video_filename']) if 'video_filename' in files else None
        if not video_filename:
            Logger().error(
                f"No video filename for {video_with_meta['_id']}")
            return None, None, None

        if not os.path.exists(video_filename):
            Logger().error(
                f"Video file {video_filename} does not exist")
            return None, None, None

        if not video['title']:
            Logger().error(
                f"No video title for {video_with_meta}")
            return None, None, None

        video_title = video['title']
        video_title = video_title.replace('/', '-')
        video_title = video_title.replace('\\', '-')
        video_title = video_title.replace('*', '-')
        video_title = video_title.replace('?', '-')
        video_title = video_title.replace('"', '-')
        info_filename = os.path.join(settings.youtube_channels_dir,
                                     channel_title,
                                     files['info_filename'])
        thumbnail_filename = os.path.join(settings.youtube_channels_dir,
                                          channel_title,
                                          files['thumbnail_filename'])

        return video_filename, info_filename, thumbnail_filename

    except Exception as e:
        Logger().error(e)
        return None, None, None


def audit_video_files(video_with_meta):

    try:

        video_id = video_with_meta['_id']

        video_filename, info_filename, thumbnail_filename = _expand_video_files(
            video_with_meta)

        if not video_filename or not os.path.exists(video_filename):
            Logger().error(f"Missing video file for {video_id}")
            return False

        if not info_filename or not os.path.exists(info_filename):
            Logger().error(f"Missing info file for {video_id}")
            return False

        if not thumbnail_filename or not os.path.exists(thumbnail_filename):
            Logger().error(f"Missing thumbnail fil for {video_id}")
            return False

        return True

    except Exception as e:
        Logger().error(e)
        return False


def delete_video_files(video_with_meta):

    try:

        video_filename, info_filename, thumbnail_filename = _expand_video_files(
            video_with_meta)

        if os.path.exists(video_filename):
            os.remove(video_filename)

        if os.path.exists(info_filename):
            os.remove(info_filename)

        if os.path.exists(thumbnail_filename):
            os.remove(thumbnail_filename)

    except Exception as e:
        Logger().error(e)


def _resolve_multiple_files(file1, file2):
    size1 = os.path.getsize(file1)
    size2 = os.path.getsize(file2)
    if size2 > size1:
        os.remove(file1)
        return file2
    return file1


def resolve_video_files(json_filename):

    # expecting filenames of the form: <path>/<title>.info.json
    # and the corresponding thumbnail and video files (with the same title)
    # in the same directory

    filepath, ext = os.path.splitext(json_filename)
    if ext != '.json':
        Logger().error(f"unexpected file extension {ext=} (expecting '.json')")
        return None

    filepath, ext = os.path.splitext(filepath)
    if ext != '.info':
        Logger().error(f"unexpected file extension {ext=} (expecting '.info')")
        return None

    # find the corresponding thumbnail and video files
    glob_path = re.sub('([\[\]])', '[\\1]', filepath)
    files = glob.glob(f"{glob_path}.*")

    thumbnail_filename = None
    video_filename = None
    info_filename = json_filename.split('/')[-1]

    for file in files:
        _, ext = os.path.splitext(file)
        if ext in IMAGE_EXTS:
            if thumbnail_filename is None:
                thumbnail_filename = file
            else:
                thumbnail_filename = _resolve_multiple_files(
                    thumbnail_filename, file)
        elif ext in VIDEO_EXTS:
            if video_filename is None:
                video_filename = file
            else:
                video_filename = _resolve_multiple_files(video_filename, file)

    if video_filename is None:
        Logger().error(f"no video file found for {info_filename=} | {files=}")
        return None

    if thumbnail_filename is None:
        Logger().warning(
            f"no thumbnail file found for {info_filename=} | {files=}")

    video_filename = video_filename.split('/')[-1]
    thumbnail_filename = thumbnail_filename.split(
        '/')[-1] if thumbnail_filename else None
    return {
        'thumbnail_filename': thumbnail_filename,
        'video_filename': video_filename,
        'info_filename': info_filename
    }


def extract_video_info_from_file(json_filename, files=None):

    try:

        if files is None:
            files = resolve_video_files(json_filename)
            if files is None:
                return None

        with open(json_filename, 'r') as f:
            data = json.load(f)

        if 'id' not in data:
            Logger().error(f"no id in {json_filename=}: {data=}")
            return None

        return extract_video_info(data, files)

    except Exception as e:
        Logger().error(e)


def extract_video_info(data, files=None):

    try:

        uploader = data['uploader'] if 'uploader' in data else ''
        channel_title = data['channel'] if 'channel' in data else uploader

        info_with_meta = {
            '_id': data['id'],
            'as_of': str(datetime.now()),
            'video': {
                'title': data['title'] if 'title' in data else id,
                'description': data['description'] if 'description' in data else '',
                'tags': data['tags'] if 'tags' in data else [],
                'categories': data['categories'] if 'categories' in data else [],
                'channel_id': data['channel_id'] if 'channel_id' in data else '',
                'channel_title': channel_title,
                'uploader': uploader,
                'uploader_id': data['uploader_id'] if 'uploader_id' in data else '',
                'upload_date': str(datetime.strptime(data['upload_date'], "%Y%m%d")) if 'upload_date' in data else '',
                'duration_seconds': data['duration'] if 'duration' in data else 0,
                'view_count': data['view_count'] if 'view_count' in data else 0,
                'like_count': data['like_count'] if 'like_count' in data else 0,
                'dislike_count': data['dislike_count'] if 'dislike_count' in data else 0,
                'average_rating': data['average_rating'] if 'average_rating' in data else 0,
                'files': files
            }
        }
        return info_with_meta

    except Exception as e:
        Logger().error(e)


class MyLogger:

    def __init__(self, show_progress=True):
        self._show_progress = show_progress
        self._progress_bar = None
        self._last_progress = 0
        self._progress_length = 0

    def debug(self, msg):
        # For compatability with youtube-dl, both debug and info are passed into debug
        # You can distinguish them by the prefix '[debug] '
        if not self._show_progress and not msg.startswith('[download] '):
            Logger().debug(msg)
        elif msg.startswith('[debug] ') or msg.startswith('[info] ') or msg.startswith('[download] '):
            pass
        else:
            self.info(msg)

    def info(self, msg):
        Logger().info(msg)

    def warning(self, msg):
        Logger().warning(msg)

    def error(self, msg):
        Logger().error(msg)

    def progress_hook(self, d):

        def _total_bytes():
            return int(d['total_bytes'] if 'total_bytes' in d else d['total_bytes_estimate'])

        if d['status'] == 'downloading':

            if self._progress_bar is None:
                self._progress_length = _total_bytes()
                self._progress_bar = typer.progressbar(
                    length=self._progress_length, fill_char=typer.style(
                        "█", fg="green"),
                    show_percent=True, show_pos=True, show_eta=True)

            self._progress_bar.label = f"{d['_speed_str']:14s}"

            length = _total_bytes()
            if self._progress_length != length:
                self._progress_length = length
                self._progress_bar.length = self._progress_length

            update = int(d['downloaded_bytes']) - self._last_progress
            self._last_progress += update
            self._progress_bar.update(update)


def get_video_from_youtube(video_url, show_progress=True):

    try:

        yt_logger = MyLogger(show_progress)
        ydl_opts = {
            'logger': yt_logger,
            'writethumbnail': True,
            'paths': {
                'home': settings.tmp_dir,
            },
            'outtmpl': '%(title)s-%(id)s.%(ext)s',
            'progress_hooks': [yt_logger.progress_hook] if show_progress else [],
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url)
            info = ydl.sanitize_info(info)

            # find the thumbnail file
            for thumbnail in info['thumbnails']:
                if "filepath" in thumbnail:
                    thumbnail_filename = thumbnail["filepath"].split('/')[-1]
                    break

            # derive the other filenames
            basename, _ = os.path.splitext(thumbnail_filename)
            video_filename = f"{basename}.{info['ext']}"
            info_filename = f"{basename}.info.json"

            # write info to file
            with open(os.path.join(settings.tmp_dir, info_filename), 'w') as f:
                json.dump(info, f, indent=2)

            dst_dir = os.path.join(
                settings.youtube_channels_dir, info['channel'])
            os.makedirs(dst_dir, exist_ok=True)

            # move files to destination
            move_file(settings.tmp_dir, dst_dir, thumbnail_filename)
            move_file(settings.tmp_dir, dst_dir, video_filename)
            move_file(settings.tmp_dir, dst_dir, info_filename)

            return extract_video_info(info, {
                'video_filename': video_filename,
                'info_filename': info_filename,
                'thumbnail_filename': thumbnail_filename,
            })

    except Exception as e:
        Logger().error(e)


def get_video_from_db(video_id):

    try:

        return get_db().yt_videos.find_one({'_id': video_id})

    except Exception as e:
        Logger().error(e)


def get_video_from_db_batch(video_id_batch):

    try:

        videos_cursor = get_db().yt_videos.find(
            {'_id': {'$in': video_id_batch}})
        return list(videos_cursor)

    except Exception as e:
        Logger().error(e)


def delete_video_from_db(video_id):

    try:

        x = get_db().yt_videos.delete_one({'_id': video_id})
        return x.deleted_count

    except Exception as e:
        Logger().error(e)
        return 0


def delete_video_from_db_batch(video_id_batch):

    try:

        x = get_db().yt_videos.delete_many({'_id': {'$in': video_id_batch}})
        return x.deleted_count

    except Exception as e:
        Logger().error(e)
        return 0


def get_videos_for_audit():

    try:

        videos_cursor = get_db().yt_videos.find()
        for video_with_meta in videos_cursor:
            id = video_with_meta['_id']
            video = video_with_meta['video']
            channel_title = video['channel_title']
            uploader = video['uploader']
            files = video['files']
            yield {
                '_id': id,
                'video': {
                    'channel_title': channel_title if channel_title else uploader,
                    'files': files
                }
            }

    except Exception as e:
        Logger().error(e)


def save_video_to_db(video_with_meta, to_disk=False):

    try:

        id = video_with_meta['_id']

        if to_disk:
            with open(os.path.join(videos_bkp_dir, f'video_{id}.json'), 'w') as f:
                json.dump(video_with_meta, f, indent=2)

        get_db().yt_videos.update_one(
            {'_id': id}, {'$set': video_with_meta}, upsert=True)

    except Exception as e:
        Logger().error(e)


def save_video_to_db_batch(video_with_meta_batch, to_disk=True):

    try:

        for video_with_meta in video_with_meta_batch:
            save_video_to_db(video_with_meta, to_disk)

    except Exception as e:
        Logger().error(e)


def search_db(query):

    try:

        videos_cursor = get_db().yt_videos.find(
            {'$text': {'$search': query}})  # .sort({'score': {'$meta': "textScore"}})
        return list(videos_cursor)

    except Exception as e:
        Logger().error(e)


if __name__ == "__main__":
    cli()
