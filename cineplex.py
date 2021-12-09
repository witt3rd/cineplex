import os
import json
import pprint
import typer
from cineplex.youtube_playlists import (
    get_playlist_from_youtube_batch,
    get_playlist_from_db_batch,
    get_playlist_from_db,
    save_playlist_to_db_batch,
    save_playlist_to_db,
    get_playlist_items_from_db,
    get_playlist_items_from_youtube,
    save_playlist_items_to_db,
)
from cineplex.youtube_channels import (
    get_channel_from_youtube_batch,
    get_channel_from_db_batch,
    get_channel_from_db,
    get_channel_videos_from_db,
    save_channel_to_db_batch,
    get_channel_playlists_from_youtube,
    get_channel_playlists_from_db,
    save_channel_playlists,
)
from cineplex.youtube_videos import (
    get_video_from_db_batch,
    get_video_from_db,
    save_video_to_db_batch,
    download_video
)
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


def print_channel(channel_with_meta):
    channel_id = channel_with_meta['_id']
    as_of = channel_with_meta['as_of']
    # playlists = channel_with_meta['playlists']

    typer.echo(f"📺 Channel {green(channel_id)} as of {green(as_of)}:")
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


def print_channels(channels_with_meta):
    for channel_with_meta in channels_with_meta:
        print_channel(channel_with_meta)


def print_channel_playlists(channel_playlists_with_meta):
    channel_id = channel_playlists_with_meta['_id']
    as_of = channel_playlists_with_meta['as_of']
    playlists = channel_playlists_with_meta['playlists']

    typer.echo(f"📝 Playlists for {green(channel_id)} as of {green(as_of)}:")

    # sort by item count
    playlists.sort(key=lambda x: x['contentDetails']
                   ['itemCount'], reverse=True)

    for playlist in playlists:
        id = playlist['id']
        snippet = playlist['snippet']
        contentDetails = playlist['contentDetails']
        typer.echo(
            f'{id}: {green(snippet["title"])} ({green(contentDetails["itemCount"])})')


def print_playlist(playlist_with_meta):
    playlist_id = playlist_with_meta['_id']
    as_of = playlist_with_meta['as_of']
    playlist = playlist_with_meta['playlist']
    snippet = playlist['snippet']
    contentDetails = playlist['contentDetails']
    print(snippet.keys())

    typer.echo(f"📝 Playlist {green(playlist_id)} as of {green(as_of)}:")
    typer.echo(f"- Title        : {green(snippet['title'])}")
    typer.echo(f"- Published at : {green(snippet['publishedAt'])}")
    typer.echo(f"- Description  : {green(snippet['description'])}")
    typer.echo(f"- Channel title: {green(snippet['channelTitle'])}")
    typer.echo(f"- Item count   : {green(contentDetails['itemCount'])}")


def print_playlist_items(playlist_items_with_meta):
    playlist_id = playlist_items_with_meta['_id']
    as_of = playlist_items_with_meta['as_of']
    items = playlist_items_with_meta['items']

    typer.echo(
        f"📝 Playlist items for {green(playlist_id)} as of {green(as_of)}:")

    # sort items by position
    items.sort(key=lambda x: x['snippet']['position'])

    for item in items:
        id = item['id']
        snippet = item['snippet']
        pos = blue(f"{snippet['position']:04d}")
        video_id = green(snippet['resourceId']['videoId'])
        channel_title = yellow(snippet['channelTitle'])
        title = green(snippet['title'])
        published_at = green(f"{snippet['publishedAt']}")
        typer.echo(
            f"{pos}) {video_id}: {channel_title}: {title} @ {published_at}")


def print_video(video_with_meta):

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
def update_my_channel():
    """Get my channel from YouTube and save it to the database."""
    channel_id = settings.youtube_my_channel_id

    channels_with_meta = get_channel_from_youtube_batch([channel_id])
    if not channels_with_meta:
        typer.echo(f"❗ {red('Channel not found')}")
        return

    save_channel_to_db_batch(channels_with_meta)
    print_channels(channels_with_meta)


@app.command()
def show_my_channel():
    """Show my channel from the database."""
    channel_id = settings.youtube_my_channel_id

    channel_with_meta = get_channel_from_db(channel_id)
    if not channel_with_meta:
        typer.echo(
            f"💡 {yellow('You have no channel in the db; try')} {blue('update-my-channel')}")
        return

    print_channel(channel_with_meta)


@app.command()
def update_channel(channel_id: str):
    """Get a channel from YouTube and save it to the database."""

    channels_with_meta = get_channel_from_youtube_batch([channel_id])
    if not channels_with_meta:
        typer.echo(f"❗ {red('Channel not found')}")
        return

    save_channel_to_db_batch(channels_with_meta)
    print_channels(channels_with_meta)


@app.command()
def show_channel(channel_id: str):
    """Show a channel from the database."""
    channel_with_meta = get_channel_from_db(channel_id)
    if not channel_with_meta:
        typer.echo(
            f"💡 {yellow('That channel is not in the db; try')} {blue('update-channel')} {green(channel_id)}")
        return

    print_channel(channel_with_meta)

#
# Channel playlists
#


@app.command()
def update_my_playlists():
    """Get my playlists from YouTube and save them to the database."""
    channel_id = settings.youtube_my_channel_id

    playlists_with_meta = get_channel_playlists_from_youtube(
        channel_id)
    if not playlists_with_meta:
        msg = "Your channel doesn't have any playlists"
        typer.echo(f"❗ {red(msg)}")
        return

    save_channel_playlists(playlists_with_meta)
    print_channel_playlists(playlists_with_meta)


@app.command()
def show_my_playlists():
    """List my playlists from the database."""
    playlists_with_meta = get_channel_playlists_from_db(
        settings.youtube_my_channel_id)
    if playlists_with_meta is None:
        # TODO
        typer.echo(
            f"💡 {yellow('You have no playlists in the db; try')} {blue('update-my-playlists')}")
        return

    print_channel_playlists(playlists_with_meta)


@app.command()
def update_channel_playlists(channel_id: str):
    """Get playlists for a channel from YouTube and save them to the database."""
    playlists_with_meta = get_channel_playlists_from_youtube(channel_id)
    if not playlists_with_meta:
        msg = "That channel doesn't have any playlists"
        typer.echo(f"❗ {red(msg)}")
        return

    save_channel_playlists(playlists_with_meta)
    print_channel_playlists(playlists_with_meta)


@app.command()
def show_channel_playlists(channel_id: str):
    """List playlists for a channel from the database."""
    playlists_with_meta = get_channel_playlists_from_db(channel_id)
    if playlists_with_meta is None:
        typer.echo(
            f"💡 {yellow('That channel playlists is not in the db; try')} {blue('update-channel-playlists')} {green(channel_id)}")
        return

    print_channel_playlists(playlists_with_meta)

#
# Playlists
#


@app.command()
def update_playlist(playlist_id: str):
    """Get playlist info from YouTube and save it to the database."""
    playlists_with_meta = get_playlist_from_youtube_batch([playlist_id])
    if not playlists_with_meta:
        msg = "That playlist doesn't exist."
        typer.echo(f"❗ {red(msg)}")
        return

    save_playlist_to_db_batch(playlists_with_meta)
    print_playlist(playlists_with_meta[0])


@app.command()
def show_playlist(playlist_id: str):
    """List playlist info from the database."""
    playlist_with_meta = get_playlist_from_db(playlist_id)
    if playlist_with_meta is None:
        typer.echo(
            f"💡 {yellow('That playlist is not in the db; try')} {blue('update-playlist')} {green(playlist_id)}")
        return

    print_playlist(playlist_with_meta)

#
# Playlist items
#


@app.command()
def update_playlist_items(playlist_id: str):
    """Get playlist items for a playlist"""
    playlist_items_with_meta = get_playlist_items_from_youtube(playlist_id)
    if not playlist_items_with_meta:
        msg = "That playlist doesn't exist."
        typer.echo(f"❗ {red(msg)}")
        return

    save_playlist_items_to_db(playlist_items_with_meta)
    print_playlist_items(playlist_items_with_meta)


@app.command()
def show_playlist_items(playlist_id: str):
    """List playlist items for a playlist"""
    playlist_items_with_meta = get_playlist_items_from_db(playlist_id)
    if playlist_items_with_meta is None:
        typer.echo(
            f"💡 {yellow('That playlist has no items in the db; try')} {blue('update-playlist-items')} {green(playlist_id)}")
        return

    print_playlist_items(playlist_items_with_meta)

#
# Videos
#


@app.command()
def show_video(video_id: str):
    """Show a video from the database."""
    video_with_meta = get_video_from_db(video_id)
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
def offline_my_playlists():
    """Get my playlists"""
    playlists_with_meta = get_channel_playlists_from_db(
        settings.youtube_my_channel_id)
    print_channel_playlists(playlists_with_meta)


@app.command()
def offline_playlist(playlist_id: str):
    """Get offline playlist"""
    pass


@app.command()
def offline_channel(channel_id: str):
    """Get offline channel"""
    pass


@app.command()
def offline_vidoo(video_id: str):
    """Download a video from YouTube and place it in its channel's folder."""

    # get the video's metadata
    video_with_meta = get_video_from_db(video_id)
    video_with_meta = get_video_from_youtube(video_id)

    pass


if __name__ == "__main__":
    app()
