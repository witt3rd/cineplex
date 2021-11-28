import os
import typer
from cineplex.playlists import (
    get_playlists_from_db,
    get_playlists_from_youtube
)
from cineplex.settings import (
    DATA_DIR,
    YOUTUBE_MY_CHANNEL_ID,
)

app = typer.Typer()

if not os.path.isdir(DATA_DIR):
    os.mkdir(DATA_DIR)


def print_playlists(playlists_with_meta):
    channel_id = playlists_with_meta['channel_id']
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


@app.command()
def update_my_playlists():
    """Get my playlists"""
    playlists_with_meta = get_playlists_from_youtube(YOUTUBE_MY_CHANNEL_ID)
    print_playlists(playlists_with_meta)


@app.command()
def list_my_playlists():
    """List my playlists"""
    playlists_with_meta = get_playlists_from_db(YOUTUBE_MY_CHANNEL_ID)
    print_playlists(playlists_with_meta)


@app.command()
def update_playlists(channel_id: str):
    """Get playlists for a channel"""
    playlists_with_meta = get_playlists_from_youtube(channel_id)
    print_playlists(playlists_with_meta)


@app.command()
def list_playlists(channel_id: str):
    """List playlists for a channel"""
    playlists_with_meta = get_playlists_from_db(channel_id)
    print_playlists(playlists_with_meta)


@app.command()
def offline_my_playlists():
    """Get my playlists"""
    playlists_with_meta = get_playlists_from_db(YOUTUBE_MY_CHANNEL_ID)
    print_playlists(playlists_with_meta)


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
