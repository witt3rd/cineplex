# Cineplex

## Database

We use [MongoDB](https://www.mongodb.com/)

### Collections

Within the `cineplex` database,

#### YouTube

In general, all YouTube related data are stored in collections prefixed by `yt_<entity>`, where `<entity>` is one of the below.

##### YouTube Playlists

YouTube playlists are associated with a YouTube channel and have a ordered collection of items. We thus maintain three separate entity types: channel playlists, playlist info, and playlist items. Playlist related collections are prefixed with `yt_playlist_<entity>`, where `<entity>` is one of the below.

See also [Channel Playlists](#channel-playlists)

###### YouTube Playlist Info

- Collection: `yt_playlist_info`
- `_id`: YouTube `playlist_id`

###### YouTube Playlist Items

- Collection: `yt_playlist_items`
- `_id`: YouTube `playlist_id`

#### YouTube Channels

YouTube channels ... Channel related collections are prefixed with `yt_channel_<entity>`, where `<entity>` is one of the below.

##### Channel Info

- Collection: `yt_channel_info`
- `_id`: YouTube `channel_id`

##### Channel Playlists

- Collection: `yt_channel_playlists`
- `_id`: YouTube `channel_id`

##### Channel Videos

- Collection: `yt_channel_videos`
- `_id`: YouTube `channel_id`

#### YouTube Videos

#### YouTube Video Info

- Collection: `yt_video_info`
- `_id`: YouTube `video_id`

## Dependencies

- [ffmpeg](http://ffmpeg.org/)

## CLI

```bash
python cineplex.py --help
```
