import os
import json
import typer
from cineplex.youtube_playlists import (
    get_channel_playlists_from_db,
    get_channel_playlists_from_youtube,
    save_channel_playlists,
    get_playlist_items_from_db,
    get_playlist_items_from_youtube,
    save_playlist_items,
)
from cineplex.config import Settings

settings = Settings()

app = typer.Typer()

if not os.path.isdir(settings.data_dir):
    os.mkdir(settings.data_dir)


def print_channel_playlists(playlists_with_meta):
    channel_id = playlists_with_meta['_id']
    retrieved_on = playlists_with_meta['retrieved_on']
    playlists = playlists_with_meta['playlists']

    print(f"Playlists for {channel_id=} as of {retrieved_on}:")

    # sort by item count
    playlists.sort(key=lambda x: x['contentDetails']
                   ['itemCount'], reverse=True)

    for playlist in playlists:
        id = playlist['id']
        snippet = playlist['snippet']
        contentDetails = playlist['contentDetails']
        print(
            f'{id}: {snippet["title"]} ({contentDetails["itemCount"]})')


def print_playlist_items(items_with_meta):
    playlist_id = items_with_meta['playlist_id']
    retrieved_on = items_with_meta['retrieved_on']
    items = items_with_meta['items']

    print(f"Playlist items for {playlist_id=} as of {retrieved_on}:")

    # sort items by position
    items.sort(key=lambda x: x['snippet']['position'])

    for item in items:
        id = item['id']
        snippet = item['snippet']
        video_id = snippet['resourceId']['videoId']
        print(
            f'{snippet["position"]:04d}) {video_id}: {snippet["channelTitle"]}: {snippet["title"]} @ {snippet["publishedAt"]}')


@app.command()
def update_my_playlists():
    """Get my playlists"""
    channel_id = settings.youtube_my_channel_id

    playlists_with_meta = get_channel_playlists_from_youtube(
        channel_id)

    save_channel_playlists(playlists_with_meta)
    print_channel_playlists(playlists_with_meta)


@app.command()
def list_my_playlists():
    """List my playlists"""
    playlists_with_meta = get_channel_playlists_from_db(
        settings.youtube_my_channel_id)
    if playlists_with_meta is None:
        print(f"You have no playlists in the database; try 'update-my-playlists'")
    else:
        print_channel_playlists(playlists_with_meta)


@app.command()
def update_playlists(channel_id: str):
    """Get playlists for a channel"""
    playlists_with_meta = get_channel_playlists_from_youtube(channel_id)
    save_channel_playlists(playlists_with_meta)
    print_channel_playlists(playlists_with_meta)


@app.command()
def list_playlists(channel_id: str):
    """List playlists for a channel"""
    playlists_with_meta = get_channel_playlists_from_db(channel_id)
    if playlists_with_meta is None:
        print(f"There are no playlists in the database for that channel; try 'update-playlists'")
    else:
        print_channel_playlists(playlists_with_meta)


@app.command()
def update_playlist_items(playlist_id: str):
    """Get playlist items for a playlist"""
    items_with_meta = get_playlist_items_from_youtube(playlist_id)
    save_playlist_items(playlist_id, items_with_meta)
    print_playlist_items(items_with_meta)


@app.command()
def list_playlist_items(playlist_id: str):
    """List playlist items for a playlist"""
    items_with_meta = get_playlist_items_from_db(playlist_id)
    print_playlist_items(items_with_meta)


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


if __name__ == "__main__":
    app()
