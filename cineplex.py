import os
import json
from datetime import datetime
import ray
import click
import typer
from typing import List
from cineplex.config import Settings
import cineplex.db as db
from cineplex.utils import get_all_files
import cineplex.youtube as yt

settings = Settings()

app = typer.Typer()

os.makedirs(settings.data_dir, exist_ok=True)


#
# Printing
#


def green(text):
    return typer.style(text, fg=typer.colors.GREEN, bold=True)


def blue(text):
    return typer.style(text, fg=typer.colors.BRIGHT_BLUE, bold=True)


def red(text):
    return typer.style(text, fg=typer.colors.RED, bold=True)


def yellow(text):
    return typer.style(text, fg=typer.colors.YELLOW, bold=True)


def magenta(text):
    return typer.style(text, fg=typer.colors.MAGENTA, bold=True)


def print_yt_channel(channel_with_meta):
    channel_id = channel_with_meta['_id']
    as_of = channel_with_meta['as_of']

    typer.echo(f"📺 YouTube channel {green(channel_id)} as of {green(as_of)}:")
    channel = channel_with_meta['channel']
    snippet = channel['snippet']
    typer.echo(f'- Title       : {green(snippet["title"])}')
    typer.echo(
        f'- Description : {green(snippet["description"]) if snippet["description"] else "None"}')
    typer.echo(f'- Published at: {green(snippet["publishedAt"])}')

    statistics = channel['statistics']
    typer.echo(f'- Subscribers : {green(statistics["subscriberCount"])}')
    typer.echo(f'- Views       : {green(statistics["viewCount"])}')
    typer.echo(f'- Videos      : {green(statistics["videoCount"])}')

    brandingSettings = channel['brandingSettings']
    branding_channel = brandingSettings['channel']
    if 'title' in branding_channel:
        typer.echo(
            f"- Title (B)   : {green(branding_channel['title'])}")
    if 'description' in branding_channel:
        typer.echo(
            f"- Desc. (B)   : {green(branding_channel['description'])}")
    if 'keywords' in branding_channel:
        typer.echo(
            f"- Keywords (B): {green(branding_channel['keywords'])}")


def print_yt_channel_batch(channel_with_meta_batch):
    for channel_with_meta in channel_with_meta_batch:
        print_yt_channel(channel_with_meta)


def print_yt_channel_playlists(channel_playlists_with_meta):
    channel_id = channel_playlists_with_meta['_id']
    as_of = channel_playlists_with_meta['as_of']
    playlists = channel_playlists_with_meta['playlists']

    typer.echo(
        f"📝 YouTube playlists for {green(channel_id)} as of {green(as_of)}:")

    # sort by item count
    playlists.sort(key=lambda x: x['contentDetails']
                   ['itemCount'], reverse=True)

    for playlist in playlists:
        id = playlist['id']
        snippet = playlist['snippet']
        contentDetails = playlist['contentDetails']
        typer.echo(
            f'{id}: {green(snippet["title"])} ({blue(contentDetails["itemCount"])})')


def print_yt_channel_playlists_batch(channel_playlists_with_meta_batch):
    for channel_playlists_with_meta in channel_playlists_with_meta_batch:
        print_yt_channel_playlists(channel_playlists_with_meta)


def print_yt_playlist(playlist_with_meta):
    playlist_id = playlist_with_meta['_id']
    as_of = playlist_with_meta['as_of']
    playlist = playlist_with_meta['playlist']
    snippet = playlist['snippet']
    contentDetails = playlist['contentDetails']

    typer.echo(
        f"📝 YouTube playlist {green(playlist_id)} as of {green(as_of)}:")
    typer.echo(f"- Title        : {green(snippet['title'])}")
    typer.echo(f"- Published at : {green(snippet['publishedAt'])}")
    typer.echo(f"- Description  : {green(snippet['description'])}")
    typer.echo(f"- Channel title: {green(snippet['channelTitle'])}")
    typer.echo(f"- Item count   : {green(contentDetails['itemCount'])}")


def print_yt_playlist_batch(playlist_with_meta_batch):
    for playlist_with_meta in playlist_with_meta_batch:
        print_yt_playlist(playlist_with_meta)


def print_yt_playlist_items(playlist_items_with_meta):
    playlist_id = playlist_items_with_meta['_id']
    as_of = playlist_items_with_meta['as_of']
    items = playlist_items_with_meta['items']

    typer.echo(
        f"📝 YouTube playlist items for {green(playlist_id)} as of {blue(as_of)}:")

    # sort items by position
    items.sort(key=lambda x: x['snippet']['position'])

    for item in items:
        snippet = item['snippet']
        pos = blue(f"{snippet['position']:04d}")
        video_id = green(snippet['resourceId']['videoId'])
        channel_title = yellow(snippet['channelTitle'])
        title = green(snippet['title'])
        published_at = blue(f"{snippet['publishedAt']}")
        typer.echo(
            f"{pos}) {video_id}: {channel_title}: {title} @ {published_at}")


def print_yt_playlist_items_batch(playlist_items_with_meta_batch):
    for playlist_items_with_meta in playlist_items_with_meta_batch:
        print_yt_playlist_items(playlist_items_with_meta)


def print_yt_video(video_with_meta):

    id = green(video_with_meta['_id'])
    video = video_with_meta['video']
    files = video['files']
    title = green(video['title'])
    description = green(video['description'])
    tags = green(video['tags'])
    categories = green(video['categories'])
    channel_id = green(video['channel_id'])
    channel_title = green(video['channel_title'])
    uploader = green(video['uploader'])
    uploader_id = green(video['uploader_id'])
    upload_date = blue(video['upload_date'])
    duration_seconds = green(video['duration_seconds'])
    view_count = green(video['view_count'])
    like_count = green(video['like_count'])
    dislike_count = green(video['dislike_count'])
    average_rating = green(video['average_rating'])
    video_filename = green(files['video_filename'])
    info_filename = green(files['info_filename'])
    thumbnail_filename = green(files['thumbnail_filename'])

    typer.echo(
        f"📼 {id}: {title} @ {upload_date}")
    typer.echo(f"- Description  : {description}")
    typer.echo(f"- Tags         : {tags}")
    typer.echo(f"- Categories   : {categories}")
    typer.echo(f"- Channel      : {channel_id} ({channel_title})")
    typer.echo(f"- Uploader     : {uploader} ({uploader_id})")
    typer.echo(f"- Duration     : {duration_seconds}s")
    typer.echo(f"- Views        : {view_count}")
    typer.echo(f"- Likes        : 👍🏻 {like_count} / 👎🏻 {dislike_count}")
    typer.echo(f"- Rating       : {average_rating}")
    typer.echo(f"- Video file   : {video_filename}")
    typer.echo(f"- Info file    : {info_filename}")
    typer.echo(f"- Thumbnail    : {thumbnail_filename}")


def print_yt_video_batch(video_with_meta_batch):
    for video_with_meta in video_with_meta_batch:
        print_yt_video(video_with_meta)


def print_auto_offline_batch(item_with_meta_batch, kind: str):
    if not item_with_meta_batch:
        typer.echo(f"💡 {yellow('You have no auto offline ' + kind + 's')}")
        return

    # item_with_meta_batch.sort(key=lambda x: x['offline_as_of'])

    for item_with_meta in item_with_meta_batch:
        id = item_with_meta['_id']
        title = item_with_meta[kind]['snippet']['title']
        as_of = item_with_meta['offline_as_of']
        typer.echo(f"{magenta(as_of)} {blue(id)} {green(title)}")


#
# Helpers
#


def _missing_found(id_batch, meta_batch):
    if not meta_batch:
        return id_batch, []
    found_id_batch = [x['_id'] for x in meta_batch]
    missing_id_batch = [x for x in id_batch if x not in found_id_batch]
    return missing_id_batch, found_id_batch


def ensure_batch(id_batch, db_fn, sync_fn):
    """ Get metadata for a batch of IDs from the database or from YouTube. """
    meta_batch = db_fn(id_batch)
    missing_ids, _ = _missing_found(id_batch, meta_batch)
    if missing_ids:
        missing_meta = sync_fn(missing_ids)
        if missing_meta:
            meta_batch.extend(missing_meta)
    return meta_batch


def offline_from_file(file: str, ensure_fn, save_fn):
    """Get items to offline from file"""
    with open(file, 'r') as infile:
        offline_items = json.load(infile)

    id_batch = []
    id_index = {}
    for x in offline_items:
        id = x['id']
        id_batch.append(id)
        id_index[id] = datetime.strptime(
            x['lastUpdated'], '%Y-%m-%dT%H:%M:%S.%fZ')

    item_with_meta_batch = ensure_fn(id_batch)
    if not item_with_meta_batch:
        typer.echo(f"No items to offline")
        return

    updated = []
    for item_with_meta in item_with_meta_batch:
        id = item_with_meta['_id']
        last_updated = id_index[id]
        try:
            save_fn(id, last_updated)
            updated.append(id)
        except Exception as e:
            typer.echo(
                f"💡 {yellow('Error saving offline item')} {green(id)}: {e}")

    typer.echo(f"✅ {blue(len(updated))} items set for offline")

#
# Channels
#


def ensure_youtube_channel_batch(id_batch):
    return ensure_batch(id_batch, yt.get_channel_from_db_batch, sync_youtube_channel)


@app.command()
def sync_youtube_channel(channel_id_batch: List[str]):
    """Get a channel from YouTube and save it to the database."""
    channel_id_batch = list(channel_id_batch)
    channel_with_meta_batch = yt.get_channel_from_youtube_batch(
        channel_id_batch)
    if not channel_with_meta_batch:
        plural = 's' if len(channel_id_batch) > 1 else ''
        typer.echo(
            f"❗ {red('Channel' + plural + ' not found')}: {green(channel_id_batch)}")
        return

    yt.save_channel_to_db_batch(channel_with_meta_batch)
    print_yt_channel_batch(channel_with_meta_batch)

    return channel_with_meta_batch


@app.command()
def sync_my_youtube_channel():
    """Get my channel from YouTube and save it to the database."""
    return sync_youtube_channel([settings.my_youtube_channel_id])


@app.command()
def show_youtube_channel(channel_id_batch: List[str]):
    """Show a channel from the database."""
    channel_id_batch = list(channel_id_batch)
    channel_with_meta_batch = ensure_youtube_channel_batch(channel_id_batch)
    if channel_with_meta_batch:
        print_yt_channel_batch(channel_with_meta_batch)


@app.command()
def show_my_youtube_channel():
    """Show my channel from the database."""
    show_youtube_channel([settings.my_youtube_channel_id])


#
# Channel playlists
#


def ensure_youtube_channel_playlists_batch(id_batch):
    return ensure_batch(id_batch, yt.get_channel_playlists_from_db_batch, sync_youtube_channel_playlists)


@app.command()
def sync_youtube_channel_playlists(channel_id_batch: List[str], with_items: bool = False):
    """Get playlists for a channel from YouTube and save them to the database."""
    channel_id_batch = list(channel_id_batch)
    channel_playlists_with_meta_batch = []
    for channel_id in channel_id_batch:
        playlists_with_meta = yt.get_channel_playlists_from_youtube(
            channel_id)
        if not playlists_with_meta:
            msg = "Channel doesn't have any playlists"
            typer.echo(f"❗ {red(msg)}: {green(channel_id)}")
            return

        yt.save_channel_playlists_to_db(playlists_with_meta)
        print_yt_channel_playlists(playlists_with_meta)

        # for each of the playlists identified for the channel, fetch
        # the playlist info and possibly the items in the playlist
        playlist_id_batch = [x['id'] for x in playlists_with_meta['playlists']]

        sync_youtube_playlist(playlist_id_batch, with_items)

        channel_playlists_with_meta_batch.append(playlists_with_meta)

    return channel_playlists_with_meta_batch


@app.command()
def sync_my_youtube_playlists(with_items: bool = False):
    """Get my playlists from YouTube and save them to the database."""
    channel_playlists_with_meta_batch = sync_youtube_channel_playlists(
        [settings.my_youtube_channel_id])
    if channel_playlists_with_meta_batch:
        return channel_playlists_with_meta_batch[0]


@app.command()
def show_youtube_channel_playlists(channel_id_batch: List[str]):
    """List playlists for a channel from the database."""
    channel_id_batch = list(channel_id_batch)
    channel_playlists_with_meta_batch = ensure_youtube_channel_playlists_batch(
        channel_id_batch)
    if channel_playlists_with_meta_batch:
        print_yt_channel_playlists_batch(channel_playlists_with_meta_batch)


@app.command()
def show_my_youtube_playlists():
    """List my playlists from the database."""
    show_youtube_channel_playlists([settings.my_youtube_channel_id])


@app.command()
def show_youtube_channel_videos(channel_id_batch: List[str]):
    """List videos for a channel from the database."""
    for channel_id in channel_id_batch:
        channel_with_meta = yt.get_channel_from_db(channel_id)
        if not channel_with_meta:
            msg = "Channel not found"
            typer.echo(f"❗ {red(msg)}: {green(channel_id)}")
            continue
        video_with_meta_batch = yt.get_channel_videos_from_db(channel_id)
        print_yt_video_batch(video_with_meta_batch)


@app.command()
def sync_youtube_channel_uploads(channel_id_batch: List[str]):
    channel_id_batch = list(channel_id_batch)
    channel_with_meta_batch = ensure_youtube_channel_batch(channel_id_batch)
    upload_playlist_id_batch = [
        x['channel']['contentDetails']['relatedPlaylists']['uploads'] for x in channel_with_meta_batch]
    return sync_youtube_playlist(upload_playlist_id_batch, with_items=True)


@app.command()
def sync_my_youtube_channel_uploads():
    """Get my uploads from YouTube and save them to the database."""
    return sync_youtube_channel_uploads([settings.my_youtube_channel_id])


@app.command()
def show_youtube_channel_uploads(channel_id_batch: List[str]):
    channel_id_batch = list(channel_id_batch)
    channel_with_meta_batch = ensure_youtube_channel_batch(channel_id_batch)
    upload_playlist_id_batch = [
        x['channel']['contentDetails']['relatedPlaylists']['uploads'] for x in channel_with_meta_batch]
    return show_youtube_playlist_items(upload_playlist_id_batch)


@app.command()
def show_my_youtube_channel_uploads():
    """Get my uploads from YouTube and save them to the database."""
    return show_youtube_channel_uploads([settings.my_youtube_channel_id])


@app.command()
def offline_youtube_channel_uploads(channel_id_batch: List[str] = [], sync: bool = False, auto: bool = False):
    """Get offline channel"""
    channel_id_batch = list(channel_id_batch)

    if not channel_id_batch and auto:
        channel_id_batch = [x['_id']
                            for x in yt.get_offline_channels_from_db()]

    channel_with_meta_batch = sync_my_youtube_channel(
        channel_id_batch) if sync else ensure_youtube_channel_batch(channel_id_batch)

    if not channel_with_meta_batch:
        return

    playlist_id_batch = [x['channel']['contentDetails']
                         ['relatedPlaylists']['uploads'] for x in channel_with_meta_batch]

    res = offline_youtube_playlist(playlist_id_batch, sync)

    for id in [x['_id'] for x in channel_with_meta_batch]:
        yt.save_offline_channel_to_db(id, is_auto=auto)

    return res


@app.command()
def offline_my_youtube_channel_uploads(sync: bool = False):
    return offline_youtube_channel_uploads([settings.my_youtube_channel_id], sync)


@app.command()
def offline_youtube_channels_from_file(channels_file: str):
    """Get channels to offline from file"""
    offline_from_file(channels_file, ensure_youtube_channel_batch,
                      yt.save_offline_channel_to_db)


@app.command()
def show_auto_offline_youtube_channels():
    """List auto offline channels"""
    print_auto_offline_batch(yt.get_offline_channels_from_db(), 'channel')


#
# Subscriptions
#


#
# Playlists
#


def ensure_youtube_playlist_batch(id_batch):
    return ensure_batch(id_batch, yt.get_playlist_from_db_batch, sync_youtube_playlist)


@app.command()
def sync_youtube_playlist(playlist_id_batch: List[str], with_items: bool = False):
    """Get playlist info from YouTube and save it to the database."""
    playlist_id_batch = list(playlist_id_batch)
    print(f"🔄 Syncing {len(playlist_id_batch)} playlists from YouTube...")
    return
    playlist_with_meta_batch = yt.get_playlist_from_youtube_batch(
        playlist_id_batch)
    if not playlist_with_meta_batch:
        plural = 's' if len(playlist_id_batch) > 1 else ''
        msg = 'Playlist' + plural + ' not found'
        typer.echo(f"❗ {red(msg)}: {green(playlist_id_batch)}")
        return

    yt.save_playlist_to_db_batch(playlist_with_meta_batch)
    typer.echo(f"✅ Playlist {green(playlist_id_batch)} synced")

    if with_items:
        return sync_youtube_playlist_items(playlist_id_batch)

    return playlist_with_meta_batch


@app.command()
def show_youtube_playlist(playlist_id_batch: List[str]):
    """List playlist info from the database."""
    playlist_id_batch = list(playlist_id_batch)
    playlist_with_meta_batch = ensure_youtube_playlist_batch(
        playlist_id_batch)
    if playlist_with_meta_batch:
        print_yt_playlist_batch(playlist_with_meta_batch)


@app.command()
def offline_youtube_playlist(playlist_id_batch: List[str] = [], sync: bool = False, auto: bool = False):
    """Get offline playlist"""
    playlist_id_batch = list(playlist_id_batch)

    if not playlist_id_batch and auto:
        playlist_id_batch = [x['_id']
                             for x in yt.get_offline_playlists_from_db()]

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

    res = offline_youtube_video(video_id_batch)

    for id in [x['_id'] for x in playlist_items_with_meta_batch]:
        yt.save_offline_playlist_to_db(id, is_auto=auto)

    return res


@app.command()
def offline_my_youtube_playlists(sync: bool = False):
    """Get my playlists"""

    playlist_with_meta_batch = sync_my_youtube_playlists() if sync else ensure_youtube_channel_playlists_batch(
        [settings.my_youtube_channel_id])
    if not playlist_with_meta_batch:
        typer.echo(f"💡 {yellow('You have no playlists')}")
        return

    return offline_youtube_playlist([x['id'] for x in playlist_with_meta_batch['playlists']], sync)


@app.command()
def offline_youtube_playlists_from_file(playlist_file: str):
    """Get playlists to offline from file"""
    offline_from_file(playlist_file, ensure_youtube_playlist_batch,
                      yt.save_offline_playlist_to_db)


@app.command()
def show_auto_offline_youtube_playlists():
    """List auto offline playlists"""
    print_auto_offline_batch(yt.get_offline_playlists_from_db(), 'playlist')


#
# Playlist items
#


def ensure_youtube_playlist_items_batch(id_batch):
    # ensure that playlist items also has a valid playlist
    playlist_with_meta_batch = ensure_youtube_playlist_batch(id_batch)
    if playlist_with_meta_batch:
        valid_playlist_id_batch = [x['_id'] for x in playlist_with_meta_batch]
        return ensure_batch(valid_playlist_id_batch, yt.get_playlist_items_from_db_batch, sync_youtube_playlist_items)


@app.command()
def sync_youtube_playlist_items(playlist_id: List[str]):
    """Get playlist items for a playlist"""
    playlist_id_batch = list(playlist_id)
    playlist_items_with_meta_batch = []

    for playlist_id in playlist_id_batch:
        playlist_items_with_meta = yt.get_playlist_items_from_youtube(
            playlist_id)
        if not playlist_items_with_meta:
            msg = "Playlist not found"
            typer.echo(f"❗ {red(msg)}: {green(playlist_id)}")
            return

        yt.save_playlist_items_to_db(playlist_items_with_meta)
        typer.echo(f"✅ Playlist items {green(playlist_id_batch)} synced")

        playlist_items_with_meta_batch.append(playlist_items_with_meta)

    return playlist_items_with_meta_batch


@app.command()
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


@app.command()
def show_youtube_video(video_id_batch: List[str]):
    """Show a video from the database."""
    video_id_batch = list(video_id_batch)
    video_with_meta_batch = yt.get_video_from_db_batch(video_id_batch)
    missing, found = _missing_found(video_id_batch, video_with_meta_batch)
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


@app.command()
def delete_youtube_video(video_id_batch: List[str]):
    """Delete a video from the database and its files."""
    video_with_meta_batch = yt.get_video_from_db_batch(video_id_batch)
    if not video_with_meta_batch:
        typer.echo(f'{red("❗ No video(s) to delete")}')
        return

    typer.echo(
        f'deleting {blue(len(video_with_meta_batch))} video(s)')

    click.confirm('Do you want to continue?', abort=True)

    with typer.progressbar(video_with_meta_batch, label='Deleting', fill_char=click.style(
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


@app.command()
def offline_youtube_video(video_id_batch: List[str], force: bool = False):
    """Download a video from YouTube and place it in its channel's folder."""
    video_id_batch = list(video_id_batch)

    if not force:
        # verify any videos that are already in the database
        video_with_meta_batch = yt.get_video_from_db_batch(video_id_batch)
        missing, found = _missing_found(video_id_batch, video_with_meta_batch)
        with typer.progressbar(found, label='Auditing', fill_char=click.style(
                "█", fg="green"),
                show_percent=True, show_pos=True, show_eta=True) as found_bar:
            for video_id in found_bar:
                video_with_meta = [
                    x for x in video_with_meta_batch if x['_id'] == video_id][0]
                if not yt.audit_video_files(video_with_meta):
                    missing.append(video_id)

        verified_with_meta_batch = [
            x for x in video_with_meta_batch if x['_id'] not in missing]
        for verified_with_meta in verified_with_meta_batch:
            id = verified_with_meta['_id']
            video = verified_with_meta['video']
            title = video['title']
            typer.echo(f"✅ {blue(id)} {green(title)}")

        if not missing:
            return verified_with_meta_batch
    else:
        missing = video_id_batch
        verified_with_meta_batch = []

    typer.echo(
        f"💡 {yellow('Downloading')} {blue(len(missing))} video(s): {green(missing)}")

    if len(missing) > 1:
        ray.init()
        futures = [_download_youtube_video_ray.remote(x) for x in missing]
        dl_video_with_meta_batch = [x for x in ray.get(futures) if x]
    else:
        res = _download_youtube_video(missing[0])
        dl_video_with_meta_batch = [res] if res else []

    not_dl_id_batch, _ = _missing_found(missing, dl_video_with_meta_batch)

    if not_dl_id_batch:
        typer.echo(
            f"❗ {red('Unable to download')} {blue(len(not_dl_id_batch))}: {green(not_dl_id_batch)}")

    for dl_video_with_meta in dl_video_with_meta_batch:
        id = dl_video_with_meta['_id']
        video = dl_video_with_meta['video']
        title = video['title']
        typer.echo(f"⬇️ {blue(id)} {green(title)}")
        verified_with_meta_batch.append(dl_video_with_meta)

    return verified_with_meta_batch


#
# Search
#

@app.command()
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
    if not video_with_meta_batch:
        typer.echo(f'{red("❗ No video(s) to audit")}')
        return

    label = label or 'Auditing'
    with typer.progressbar(video_with_meta_batch, label=f'{yellow(label)}', fill_char=click.style("█", fg="green"), show_pos=True) as bar:

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


@app.command()
def audit_youtube_video(video_id_batch: List[str], repair: bool = False, clean: bool = False, label=None):
    """Audit videos in the database"""
    _audit_youtube_video(yt.get_video_from_db_batch(
        video_id_batch, repair, clean, label))


@app.command()
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


@app.command()
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
        with typer.progressbar(video_with_meta_batch, label=f'{yellow(channel_title)}', fill_char=click.style("█", fg="green"), show_pos=True) as bar:
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


@ app.command()
def audit_all_youtube_channel_videos():
    """Audit all videos for a channel from the database."""
    return(audit_youtube_channel_videos(yt.get_all_channel_ids_from_db()))


@ app.command()
def repair_all():

    with open(os.path.join(settings.data_dir, 'bad_db_videos.json'), 'r') as infile:
        bad = json.load(infile)

    for video_id in bad:
        audit_youtube_video([video_id], repair=True, clean=True)


@ app.command()
def audit_youtube_db(repair: bool = False, clean: bool = False):
    """Audit videos in the database"""
    for video in yt.get_videos_for_audit():
        if not yt.audit_video_files(video):
            audit_youtube_video(video['_id'], repair, clean)


#
# Administration
#

@ app.command()
def init_db():
    """Initialize the database"""
    db.create_indices()


if __name__ == "__main__":
    app()
