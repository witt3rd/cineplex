import os
import json

YOUTUBE_VIDEO_DIR = "/Volumes/media/youtube/thumbnails"
files = os.listdir(YOUTUBE_VIDEO_DIR)
print(f'found {len(files)} files in {YOUTUBE_VIDEO_DIR}')
with open('data/files.json', 'w') as outfile:
    json.dump(files, outfile)
