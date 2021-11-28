import json
from youtube import youtube_api
from db import get_db
from logger import Logger


def get_playlists(channel_id):

    logger = Logger()
    logger.debug(f"getting playlists for {channel_id=}")

    youtube = youtube_api()

    request = youtube.playlists().list(
        channelId=channel_id,
        part="snippet",
        maxResults=50,
        fields='nextPageToken,items(snippet,contentDetails)'
    )

    playlists = []

    while request:
        response = request.execute()
        playlists.extend(response['items'])
        request = youtube.playlists().list_next(request, response)

    save_playlists(channel_id, playlists)

    logger.info(
        f"retrieved and saved {len(playlists)} playlists for {channel_id=}")


def save_playlists(channel_id, playlists, to_disk=True):

    logger = Logger()
    logger.debug(f"saving playlists for {channel_id=}")

    if to_disk:
        with open(f"playlists_{channel_id}.json", "w") as result:
            json.dump(playlists, result, indent=2)

    get_db().set(channel_id, json.dumps(playlists))


if __name__ == "__main__":
    channel_id = "UCqsUJL5xIWuidR7sIrPLhAw"
    get_playlists(channel_id)
