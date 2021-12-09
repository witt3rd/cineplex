import os
import json
import pprint
import typer
from cineplex.youtube_playlists import (
    get_playlist_items_from_db,
    get_playlist_items_from_youtube,
    save_playlist_items,
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


def val(text):
    return typer.style(text, fg=typer.colors.GREEN, bold=True)


def print_channel(channel_with_meta):
    channel_id = channel_with_meta['_id']
    as_of = channel_with_meta['as_of']
    # playlists = channel_with_meta['playlists']

    typer.echo(f"📺 Channel {val(channel_id)} as of {val(as_of)}:")
    channel = channel_with_meta['channel']
    snippet = channel['snippet']
    typer.echo(f'- Title       : {val(snippet["title"])}')
    typer.echo(
        f'- Description : {val(snippet["description"]) if snippet["description"] else "None"}')
    typer.echo(f'- Published on: {val(snippet["publishedAt"])}')

    statistics = channel['statistics']
    typer.echo(f'- Subscribers : {val(statistics["subscriberCount"])}')
    typer.echo(f'- Views       : {val(statistics["viewCount"])}')
    typer.echo(f'- Videos      : {val(statistics["videoCount"])}')

    brandingSettings = channel['brandingSettings']
    branding_channel = brandingSettings['channel']
    if 'title' in branding_channel:
        typer.echo(
            f"- Title (B)   : {val(branding_channel['title'])}")
    if 'description' in branding_channel:
        typer.echo(
            f"- Desc. (B)   : {val(branding_channel['description'])}")
    if 'keywords' in branding_channel:
        typer.echo(
            f"- Keywords (B): {val(branding_channel['keywords'])}")


def print_channels(channels_with_meta):
    for channel_with_meta in channels_with_meta:
        print_channel(channel_with_meta)


def print_channel_playlists(channel_playlists_with_meta):
    channel_id = channel_playlists_with_meta['_id']
    as_of = channel_playlists_with_meta['as_of']
    playlists = channel_playlists_with_meta['playlists']

    typer.echo(f"Playlists for {channel_id=} as of {as_of}:")

    # sort by item count
    playlists.sort(key=lambda x: x['contentDetails']
                   ['itemCount'], reverse=True)

    for playlist in playlists:
        id = playlist['id']
        snippet = playlist['snippet']
        contentDetails = playlist['contentDetails']
        typer.echo(
            f'{id}: {snippet["title"]} ({contentDetails["itemCount"]})')


def print_playlist(playlist_with_meta):
    playlist_id = playlist_with_meta['_id']
    as_of = playlist_with_meta['as_of']
    playlist = playlist_with_meta['playlist']

    typer.echo(f"Playlist {playlist_id=} as of {as_of}:")
    pprint(playlist, indent=2)


def print_playlist_items(items_with_meta):
    playlist_id = items_with_meta['playlist_id']
    as_of = items_with_meta['as_of']
    items = items_with_meta['items']

    typer.echo(f"Playlist items for {playlist_id=} as of {as_of}:")

    # sort items by position
    items.sort(key=lambda x: x['snippet']['position'])

    for item in items:
        id = item['id']
        snippet = item['snippet']
        video_id = snippet['resourceId']['videoId']
        typer.echo(
            f'{snippet["position"]:04d}) {video_id}: {snippet["channelTitle"]}: {snippet["title"]} @ {snippet["publishedAt"]}')


def print_video(video_with_meta):

    print(video_with_meta.keys())
    return
    typer.echo(
        f"Video {video_with_meta['_id']=} as of {video_with_meta['as_of']}:")
    video = video_with_meta['video']

    typer.echo(video_with_meta)


@app.command()
def update_my_channel():
    """Get my channel from YouTube and save it to the database."""
    channel_id = settings.youtube_my_channel_id

    channels_with_meta = get_channel_from_youtube_batch([channel_id])
    if not channels_with_meta:
        typer.echo(f'No channel found for {channel_id=}')
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
            f"You have no channels in the database (try 'update-my-channel')")
        return

    print_channel(channel_with_meta)


@app.command()
def update_channel(channel_id: str):
    """Get a channel from YouTube and save it to the database."""

    channels_with_meta = get_channel_from_youtube_batch([channel_id])
    if not channels_with_meta:
        typer.echo(f'No channel found for {channel_id}')
        return

    save_channel_to_db_batch(channels_with_meta)
    print_channels(channels_with_meta)


@app.command()
def show_channel(channel_id: str):
    """Show a channel from the database."""
    channel_with_meta = get_channel_from_db(channel_id)
    if not channel_with_meta:
        typer.echo(
            f"That channel is not in the database; try 'update-channel'")
        return

    print_channel(channel_with_meta)


@ app.command()
def update_my_playlists():
    """Get my playlists from YouTube and save them to the database."""
    channel_id = settings.youtube_my_channel_id

    playlists_with_meta = get_channel_playlists_from_youtube(
        channel_id)
    if not playlists_with_meta:
        typer.echo(f"You don't have any playlists in your channel.")
        return

    save_channel_playlists(playlists_with_meta)
    print_channel_playlists(playlists_with_meta)


@ app.command()
def show_my_playlists():
    """List my playlists from the database."""
    playlists_with_meta = get_channel_playlists_from_db(
        settings.youtube_my_channel_id)
    if playlists_with_meta is None:
        typer.echo(
            f"You have no playlists in the database (try 'update-my-playlists').")
        return

    print_channel_playlists(playlists_with_meta)


@ app.command()
def update_channel_playlists(channel_id: str):
    """Get playlists for a channel from YouTube and save them to the database."""
    playlists_with_meta = get_channel_playlists_from_youtube(channel_id)
    if not playlists_with_meta:
        typer.echo(f"That channel doesn't have any playlists.")
        return

    save_channel_playlists(playlists_with_meta)
    print_channel_playlists(playlists_with_meta)


@ app.command()
def show_channel_playlists(channel_id: str):
    """List playlists for a channel from the database."""
    playlists_with_meta = get_channel_playlists_from_db(channel_id)
    if playlists_with_meta is None:
        typer.echo(
            f"There are no playlists in the database for that channel; try 'update-channel-playlists'")
        return

    print_channel_playlists(playlists_with_meta)


@ app.command()
def update_playlist(playlist_id: str):
    """Get playlist info from YouTube and save it to the database."""
    playlists_with_meta = get_playlists_from_youtube_batch([playlist_id])
    if not playlists_with_meta:
        typer.echo(f"That playlist doesn't exist.")
        return

    save_playlists_batch(playlists_with_meta)
    print_playlist([playlists_with_meta])


@ app.command()
def show_playlist(playlist_id: str):
    """List playlist info from the database."""
    playlist_with_meta = get_playlist_from_db(playlist_id)
    if playlist_with_meta is None:
        typer.echo(
            f"There is no such playlist in the database; try 'update-playlist'")
        return

    print_playlist(playlist_with_meta)


@ app.command()
def update_playlist_items(playlist_id: str):
    """Get playlist items for a playlist"""
    items_with_meta = get_playlist_items_from_youtube(playlist_id)
    save_playlist_items(playlist_id, items_with_meta)
    print_playlist_items(items_with_meta)


@ app.command()
def show_playlist_items(playlist_id: str):
    """List playlist items for a playlist"""
    items_with_meta = get_playlist_items_from_db(playlist_id)
    print_playlist_items(items_with_meta)


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


@ app.command()
def offline_my_playlists():
    """Get my playlists"""
    playlists_with_meta = get_channel_playlists_from_db(
        settings.youtube_my_channel_id)
    print_channel_playlists(playlists_with_meta)


@ app.command()
def offline_playlist(playlist_id: str):
    """Get offline playlist"""
    pass


@ app.command()
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
