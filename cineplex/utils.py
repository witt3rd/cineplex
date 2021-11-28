from enum import Enum
import json
import hashlib
# --

FILE_BLACKLIST = set([".DS_Store"])
VIDEO_EXTS = set([".mkv", ".webm", ".mp4"])
THUMBNAIL_EXTS = set([".jpg", ".webp", ".png"])


class MediaType(Enum):
    METADATA = "meta"
    THUMBNAIL = "thumb"
    UNKNOWN = "unk"
    VIDEO = "vid"


def get_media_type_from_ext(ext):
    if ext == ".json":
        type = MediaType.METADATA
    elif ext in THUMBNAIL_EXTS:
        type = MediaType.THUMBNAIL
    elif ext in VIDEO_EXTS:
        type = MediaType.VIDEO
    else:
        type = MediaType.UNKNOWN
    return type


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


# Python program to find SHA256 hash string of a file
def calc_file_sha256(filename):
    sha256_hash = hashlib.sha256()
    with open(filename, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
