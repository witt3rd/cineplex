import os
import json
import pprint
import typer
from typing import List
import cineplex.youtube_playlists as ytpl
import cineplex.youtube_channels as ytch
import cineplex.youtube_videos as ytv
from cineplex.config import Settings

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


def print_yt_video_batch(video_with_meta_batch):
    for video_with_meta in video_with_meta_batch:
        print_yt_video(video_with_meta)


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

#
# Helpers
#


def _missing_found(id_batch, meta_batch):
    found_id_batch = [x['_id'] for x in meta_batch]
    missing_id_batch = [x for x in id_batch if x not in found_id_batch]
    return missing_id_batch, found_id_batch

#
# Channels
#


def _ensure_youtube_channel_batch(channel_id_batch):
    """ Get channel metadata for a batch of channel IDs from the database or
        from YouTube. """
    channel_with_meta_batch = ytch.get_channel_from_db_batch(channel_id_batch)
    missing_channel_ids, _ = _missing_found(
        channel_id_batch, channel_with_meta_batch)
    if missing_channel_ids:
        missing_channels_with_meta = sync_youtube_channel(missing_channel_ids)
        if not missing_channels_with_meta:
            return
        channel_with_meta_batch.extend(missing_channels_with_meta)
    return channel_with_meta_batch


@app.command()
def sync_my_youtube_channel():
    """Get my channel from YouTube and save it to the database."""
    return sync_youtube_channel([settings.my_youtube_channel_id])


@app.command()
def show_my_youtube_channel():
    """Show my channel from the database."""
    show_youtube_channel([settings.my_youtube_channel_id])


@app.command()
def sync_youtube_channel(channel_id_batch: List[str]):
    """Get a channel from YouTube and save it to the database."""
    channel_id_batch = list(channel_id_batch)
    channel_with_meta_batch = ytch.get_channel_from_youtube_batch(
        channel_id_batch)
    if not channel_with_meta_batch:
        plural = 's' if len(channel_id_batch) > 1 else ''
        typer.echo(
            f"❗ {red('Channel' + plural + ' not found')}: {green(channel_id_batch)}")
        return

    ytch.save_channel_to_db_batch(channel_with_meta_batch)
    print_yt_channel_batch(channel_with_meta_batch)

    return channel_with_meta_batch


@app.command()
def show_youtube_channel(channel_id_batch: List[str]):
    """Show a channel from the database."""
    channel_id_batch = list(channel_id_batch)
    channel_with_meta_batch = _ensure_youtube_channel_batch(channel_id_batch)
    print_yt_channel_batch(channel_with_meta_batch)

#
# Channel playlists
#


@app.command()
def sync_my_youtube_playlists():
    """Get my playlists from YouTube and save them to the database."""
    return sync_youtube_channel_playlists([settings.my_youtube_channel_id])


@app.command()
def show_my_youtube_playlists():
    """List my playlists from the database."""
    show_youtube_channel_playlists([settings.my_youtube_channel_id])


@app.command()
def sync_youtube_channel_playlists(channel_id_batch: List[str], with_items: bool = False):
    """Get playlists for a channel from YouTube and save them to the database."""
    channel_id_batch = list(channel_id_batch)
    channel_playlists_with_meta_batch = []
    for channel_id in channel_id_batch:
        playlists_with_meta = ytch.get_channel_playlists_from_youtube(
            channel_id)
        if not playlists_with_meta:
            msg = "That channel doesn't have any playlists"
            typer.echo(f"❗ {red(msg)}: {green(channel_id)}")
            return

        ytch.save_channel_playlists(playlists_with_meta)
        print_yt_channel_playlists(playlists_with_meta)

        # for each of the playlists identified for the channel, fetch
        # the playlist info and possibly the items in the playlist
        playlist_id_batch = [x['id'] for x in playlists_with_meta['playlists']]

        # N at a time
        N = 50
        for i in range(0, len(playlist_id_batch), N):
            sync_youtube_playlist(playlist_id_batch[i:i+N], with_items)

        channel_playlists_with_meta_batch.append(playlists_with_meta)

    return channel_playlists_with_meta_batch


@app.command()
def show_youtube_channel_playlists(channel_id_batch: List[str]):
    """List playlists for a channel from the database."""
    channel_id_batch = list(channel_id_batch)
    for channel_id_batch in channel_id_batch:
        playlists_with_meta = ytch.get_channel_playlists_from_db(
            channel_id_batch)
        if playlists_with_meta is None:
            typer.echo(
                f"💡 {yellow('Channel playlists not in db; try')} {blue('update-youtube-channel-playlists')} {green(channel_id_batch)}")
            return

        print_yt_channel_playlists(playlists_with_meta)

        playlist_id_batch = [x['id'] for x in playlists_with_meta['playlists']]

        in_db = ytpl.get_playlist_from_db_batch(playlist_id_batch)
        if len(in_db) == len(playlist_id_batch):
            typer.echo(f"✅ {green('All playlists are in the db')}")
        else:
            typer.echo(
                f"💡 {red(len(in_db))} of {red(len(playlist_id_batch))} {yellow('playlists in the db')}")


@app.command()
def sync_my_youtube_channel_uploads():
    """Get my uploads from YouTube and save them to the database."""
    return sync_youtube_channel_uploads([settings.my_youtube_channel_id])


@app.command()
def show_my_youtube_channel_uploads():
    """Get my uploads from YouTube and save them to the database."""
    return show_youtube_channel_uploads([settings.my_youtube_channel_id])


@app.command()
def sync_youtube_channel_uploads(channel_id_batch: List[str]):
    channel_id_batch = list(channel_id_batch)
    channel_with_meta_batch = _ensure_youtube_channel_batch(channel_id_batch)
    upload_playlist_id_batch = [
        x['channel']['contentDetails']['relatedPlaylists']['uploads'] for x in channel_with_meta_batch]
    return sync_youtube_playlist(upload_playlist_id_batch, with_items=True)


@app.command()
def show_youtube_channel_uploads(channel_id_batch: List[str]):
    channel_id_batch = list(channel_id_batch)
    channel_with_meta_batch = _ensure_youtube_channel_batch(channel_id_batch)
    upload_playlist_id_batch = [
        x['channel']['contentDetails']['relatedPlaylists']['uploads'] for x in channel_with_meta_batch]
    return show_youtube_playlist_items(upload_playlist_id_batch)


#
# Playlists
#


@app.command()
def sync_youtube_playlist(playlist_id_batch: List[str], with_items: bool = False):
    """Get playlist info from YouTube and save it to the database."""
    playlist_id_batch = list(playlist_id_batch)
    playlist_with_meta_batch = ytpl.get_playlist_from_youtube_batch(
        playlist_id_batch)
    if not playlist_with_meta_batch:
        plural = 's' if len(playlist_id_batch) > 1 else ''
        msg = 'Playlist' + plural + ' not found'
        typer.echo(f"❗ {red(msg)}: {green(playlist_id_batch)}")
        return

    ytpl.save_playlist_to_db_batch(playlist_with_meta_batch)
    print_yt_playlist_batch(playlist_with_meta_batch)

    if with_items:
        sync_youtube_playlist_items(playlist_id_batch)

    return playlist_with_meta_batch


@app.command()
def show_youtube_playlist(playlist_id_batch: List[str]):
    """List playlist info from the database."""
    playlist_id_batch = list(playlist_id_batch)
    playlist_with_meta_batch = ytpl.get_playlist_from_db_batch(
        playlist_id_batch)
    if playlist_with_meta_batch is None:
        plural = 's' if len(playlist_id_batch) > 1 else ''
        typer.echo(
            f"💡 {yellow('Playlist' + plural + ' not in db; try')} {blue('update-youtube-playlist')} {green(playlist_id_batch)}")
        return

    print_yt_playlist_batch(playlist_with_meta_batch)

#
# Playlist items
#


@app.command()
def sync_youtube_playlist_items(playlist_id: List[str]):
    """Get playlist items for a playlist"""
    playlist_id_batch = list(playlist_id)
    playlist_items_with_meta_batch = []

    for playlist_id in playlist_id_batch:
        playlist_items_with_meta = ytpl.get_playlist_items_from_youtube(
            playlist_id)
        if not playlist_items_with_meta:
            msg = "Playlist not found"
            typer.echo(f"❗ {red(msg)}: {green(playlist_id)}")
            return

        ytpl.save_playlist_items_to_db(playlist_items_with_meta)
        print_yt_playlist_items(playlist_items_with_meta)

        playlist_items_with_meta_batch.append(playlist_items_with_meta)

    return playlist_items_with_meta_batch


@app.command()
def show_youtube_playlist_items(playlist_id_batch: List[str]):
    """List playlist items for a playlist"""
    playlist_id_batch = list(playlist_id_batch)
    for playlist_id in playlist_id_batch:
        playlist_items_with_meta = ytpl.get_playlist_items_from_db(playlist_id)
        if playlist_items_with_meta is None:
            typer.echo(
                f"💡 {yellow('Playlist has no items in db; try')} {blue('update-youtube-playlist-items')} {green(playlist_id)}")
            return

        print_yt_playlist_items(playlist_items_with_meta)

#
# Videos
#


@app.command()
def show_youtube_video(video_id_batch: List[str]):
    """Show a video from the database."""
    video_id_batch = list(video_id_batch)
    video_with_meta_batch = ytv.get_video_from_db_batch(video_id_batch)
    if not video_with_meta_batch:
        plural = 's' if len(video_id_batch) > 1 else ''
        typer.echo(
            f"💡 {yellow('Video' + plural + ' not in db; try')} {blue('update-youtube-playlist')} {green(video_id_batch)}")
        return

    print_yt_video_batch(video_with_meta_batch)


@app.command()
def offline_my_youtube_playlists():
    """Get my playlists"""
    playlists_with_meta = get_channel_playlists_from_db(
        settings.my_youtube_channel_id)
    print_yt_channel_playlists(playlists_with_meta)


@app.command()
def offline_youtube_playlist(playlist_id: str):
    """Get offline playlist"""
    pass


@app.command()
def offline_youtube_channel(channel_id: str):
    """Get offline channel"""
    pass


@app.command()
def offline_youtube_video(video_id_batch: List[str]):
    """Download a video from YouTube and place it in its channel's folder."""
    video_id_batch = list(video_id_batch)
    video_with_meta_batch = []
    for video_id in video_id_batch:
        video_url = f'https://www.youtube.com/watch?v={video_id_batch[0]}'
        video_with_meta = ytv.get_video_from_youtube(video_url)
        if not video_with_meta:
            typer.echo(
                f"❗ {red('Unable to download video')}: {green(video_url)}")
            return
        video_with_meta_batch.append(video_with_meta)

    ytv.save_video_to_db_batch(video_with_meta_batch)
    print_yt_video_batch(video_with_meta_batch)
    # channel_id = video_with_meta['channel_id']
    # channel_with_meta = get_channel_from_db(channel_id)
    # channel_title = channel_with_meta['channel']['snippet']['title']
    # video_dir = os.path.join(settings.youtube_channels_dir, channel_title)
    # video_path = os.path.join(video_dir, video_with_meta['video_file'])

    # if not os.path.exists(video_path):
    #     typer.echo(
    #         f'Video file {video_path} does not exist; try "update-video"')
    #     return
    # print(f"Video file {video_path} exists")

    # # print_video(video_with_meta)


if __name__ == "__main__":
    app()
