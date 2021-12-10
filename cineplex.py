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
    print(snippet.keys())

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


def print_yt_video(video_with_meta):

    print(video_with_meta.keys())
    return
    typer.echo(
        f"Video {video_with_meta['_id']=} as of {video_with_meta['as_of']}:")
    video = video_with_meta['video']

    typer.echo(video_with_meta)

#
# Channels
#


@app.command()
def sync_my_youtube_channel():
    """Get my channel from YouTube and save it to the database."""
    sync_youtube_channel([settings.my_youtube_channel_id])


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


@app.command()
def show_youtube_channel(channel_id_batch: List[str]):
    """Show a channel from the database."""
    channel_id_batch = list(channel_id_batch)
    channel_with_meta_batch = ytch.get_channel_from_db_batch(channel_id_batch)
    if not channel_with_meta_batch:
        plural = 's' if len(channel_id_batch) > 1 else ''
        typer.echo(
            f"💡 {yellow('Channel'+plural+' not in db; try')} {blue('update-channel')} {green(channel_id_batch)}")
        return

    print_yt_channel_batch(channel_with_meta_batch)

#
# Channel playlists
#


@app.command()
def sync_my_youtube_playlists():
    """Get my playlists from YouTube and save them to the database."""
    sync_youtube_channel_playlists([settings.my_youtube_channel_id])


@app.command()
def show_my_youtube_playlists():
    """List my playlists from the database."""
    show_youtube_channel_playlists([settings.my_youtube_channel_id])


@app.command()
def sync_youtube_channel_playlists(channel_id_batch: List[str], with_items: bool = False):
    """Get playlists for a channel from YouTube and save them to the database."""
    channel_id_batch = list(channel_id_batch)
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


@app.command()
def show_youtube_channel_playlists(channel_id_batch: List[str]):
    """List playlists for a channel from the database."""
    channel_id_batch = list(channel_id_batch)
    for channel_id_batch in channel_id_batch:
        playlists_with_meta = ytch.get_channel_playlists_from_db(
            channel_id_batch)
        if playlists_with_meta is None:
            typer.echo(
                f"💡 {yellow('Channel playlists not in db; try')} {blue('update-channel-playlists')} {green(channel_id_batch)}")
            return

        print_yt_channel_playlists(playlists_with_meta)

        playlist_id_batch = [x['id'] for x in playlists_with_meta['playlists']]

        in_db = ytpl.get_playlist_from_db_batch(playlist_id_batch)
        if len(in_db) == len(playlist_id_batch):
            typer.echo(f"✅ {green('All playlists are in the db')}")
        else:
            typer.echo(
                f"💡 {red(len(in_db))} of {red(len(playlist_id_batch))} {yellow('playlists in the db')}")

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


@app.command()
def show_youtube_playlist(playlist_id_batch: List[str]):
    """List playlist info from the database."""
    playlist_id_batch = list(playlist_id_batch)
    playlist_with_meta_batch = ytpl.get_playlist_from_db_batch(
        playlist_id_batch)
    if playlist_with_meta_batch is None:
        plural = 's' if len(playlist_id_batch) > 1 else ''
        typer.echo(
            f"💡 {yellow('Playlist' + plural + ' not in db; try')} {blue('update-playlist')} {green(playlist_id_batch)}")
        return

    print_yt_playlist_batch(playlist_with_meta_batch)

#
# Playlist items
#


@app.command()
def sync_youtube_playlist_items(playlist_id: List[str]):
    """Get playlist items for a playlist"""
    playlist_id_batch = list(playlist_id)
    for playlist_id in playlist_id_batch:
        playlist_items_with_meta = ytpl.get_playlist_items_from_youtube(
            playlist_id)
        if not playlist_items_with_meta:
            msg = "Playlist not found"
            typer.echo(f"❗ {red(msg)}: {green(playlist_id)}")
            return

        ytpl.save_playlist_items_to_db(playlist_items_with_meta)
        print_yt_playlist_items(playlist_items_with_meta)


@app.command()
def show_youtube_playlist_items(playlist_id: List[str]):
    """List playlist items for a playlist"""
    playlist_id_batch = list(playlist_id)
    for playlist_id in playlist_id_batch:
        playlist_items_with_meta = ytpl.get_playlist_items_from_db(playlist_id)
        if playlist_items_with_meta is None:
            typer.echo(
                f"💡 {yellow('Playlist has no items in db; try')} {blue('update-playlist-items')} {green(playlist_id)}")
            return

        print_yt_playlist_items(playlist_items_with_meta)

#
# Videos
#


@app.command()
def show_youtube_video(video_id: str):
    """Show a video from the database."""
    video_with_meta = ytv.get_video_from_db(video_id)
    if not video_with_meta:
        typer.echo(
            f"That video is not in the database; try 'update-video'")
        return

    pprint(video_with_meta, indent=2)
    return

    channel_id = video_with_meta['channel_id']
    channel_with_meta = get_channel_from_db(channel_id)
    channel_title = channel_with_meta['channel']['snippet']['title']
    video_dir = os.path.join(settings.youtube_channels_dir, channel_title)
    video_path = os.path.join(video_dir, video_with_meta['video_file'])

    if not os.path.exists(video_path):
        typer.echo(
            f'Video file {video_path} does not exist; try "update-video"')
        return
    print(f"Video file {video_path} exists")

    # print_video(video_with_meta)


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
def offline_youtube_vidoo(video_id: str):
    """Download a video from YouTube and place it in its channel's folder."""

    # get the video's metadata
    video_with_meta = get_video_from_db(video_id)
    video_with_meta = get_video_from_youtube(video_id)

    pass


if __name__ == "__main__":
    app()
