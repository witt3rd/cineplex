import os
import json
from aiohttp import content_disposition_filename
import typer
from cineplex.youtube_playlists import (
    get_channel_playlists_from_db,
    get_channel_playlists_from_youtube,
    save_channel_playlists,
    get_playlist_items_from_db,
    get_playlist_items_from_youtube,
    save_playlist_items,
)
from cineplex.youtube_channels import (
    get_channels_from_youtube,
    get_channel_from_db,
    save_channels,
)

from cineplex.config import Settings

settings = Settings()

app = typer.Typer()

if not os.path.isdir(settings.data_dir):
    os.mkdir(settings.data_dir)


def print_channel(channel_with_meta):
    channel_id = channel_with_meta['_id']
    retrieved_on = channel_with_meta['retrieved_on']
    # playlists = channel_with_meta['playlists']

    typer.echo(f"Channel {channel_id=} as of {retrieved_on}:")
    channel = channel_with_meta['channel']
    snippet = channel['snippet']
    typer.echo(f'- Title       : {snippet["title"]}')
    typer.echo(
        f'- Description : {snippet["description"] if snippet["description"] else "None"}')
    typer.echo(f'- Published on: {snippet["publishedAt"]}')

    statistics = channel['statistics']
    typer.echo(f'- Subscribers : {statistics["subscriberCount"]}')
    typer.echo(f'- Views       : {statistics["viewCount"]}')
    typer.echo(f'- Videos      : {statistics["videoCount"]}')

    brandingSettings = channel['brandingSettings']
    branding_channel = brandingSettings['channel']
    if 'title' in branding_channel:
        typer.echo(
            f"- Title (B)   : {branding_channel['title']}")
    if 'description' in branding_channel:
        typer.echo(
            f"- Desc. (B)   : {branding_channel['description']}")
    if 'keywords' in branding_channel:
        typer.echo(
            f"- Keywords (B): {branding_channel['keywords']}")


def print_channels(channels_with_meta):
    for channel_with_meta in channels_with_meta:
        print_channel(channel_with_meta)


def print_channel_playlists(playlists_with_meta):
    channel_id = playlists_with_meta['_id']
    retrieved_on = playlists_with_meta['retrieved_on']
    playlists = playlists_with_meta['playlists']

    typer.echo(f"Playlists for {channel_id=} as of {retrieved_on}:")

    # sort by item count
    playlists.sort(key=lambda x: x['contentDetails']
                   ['itemCount'], reverse=True)

    for playlist in playlists:
        id = playlist['id']
        snippet = playlist['snippet']
        contentDetails = playlist['contentDetails']
        typer.echo(
            f'{id}: {snippet["title"]} ({contentDetails["itemCount"]})')


def print_playlist_items(items_with_meta):
    playlist_id = items_with_meta['playlist_id']
    retrieved_on = items_with_meta['retrieved_on']
    items = items_with_meta['items']

    typer.echo(f"Playlist items for {playlist_id=} as of {retrieved_on}:")

    # sort items by position
    items.sort(key=lambda x: x['snippet']['position'])

    for item in items:
        id = item['id']
        snippet = item['snippet']
        video_id = snippet['resourceId']['videoId']
        typer.echo(
            f'{snippet["position"]:04d}) {video_id}: {snippet["channelTitle"]}: {snippet["title"]} @ {snippet["publishedAt"]}')


@app.command()
def update_my_channel():
    """Get my channel from YouTube and save it to the database."""
    channel_id = settings.youtube_my_channel_id

    channels_with_meta = get_channels_from_youtube([channel_id])
    if not channels_with_meta:
        typer.echo(f'No channel found for {channel_id=}')
        return

    save_channels(channels_with_meta)
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

    channels_with_meta = get_channels_from_youtube([channel_id])
    if not channels_with_meta:
        typer.echo(f'No channel found for {channel_id}')
        return

    save_channels(channels_with_meta)
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
def list_my_playlists():
    """List my playlists from the database."""
    playlists_with_meta = get_channel_playlists_from_db(
        settings.youtube_my_channel_id)
    if playlists_with_meta is None:
        typer.echo(
            f"You have no playlists in the database (try 'update-my-playlists').")
        return

    print_channel_playlists(playlists_with_meta)


@ app.command()
def update_playlists(channel_id: str):
    """Get playlists for a channel from YouTube and save them to the database."""
    playlists_with_meta = get_channel_playlists_from_youtube(channel_id)
    if not playlists_with_meta:
        typer.echo(f"That channel doesn't have any playlists.")
        return

    save_channel_playlists(playlists_with_meta)
    print_channel_playlists(playlists_with_meta)


@ app.command()
def list_playlists(channel_id: str):
    """List playlists for a channel from the database."""
    playlists_with_meta = get_channel_playlists_from_db(channel_id)
    if playlists_with_meta is None:
        typer.echo(
            f"There are no playlists in the database for that channel; try 'update-playlists'")
        return

    print_channel_playlists(playlists_with_meta)


@ app.command()
def update_playlist_items(playlist_id: str):
    """Get playlist items for a playlist"""
    items_with_meta = get_playlist_items_from_youtube(playlist_id)
    save_playlist_items(playlist_id, items_with_meta)
    print_playlist_items(items_with_meta)


@ app.command()
def list_playlist_items(playlist_id: str):
    """List playlist items for a playlist"""
    items_with_meta = get_playlist_items_from_db(playlist_id)
    print_playlist_items(items_with_meta)


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


if __name__ == "__main__":
    app()
