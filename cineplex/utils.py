from enum import Enum
import json
import os
import shutil
import hashlib
# --

FILE_BLACKLIST = set([".DS_Store"])
VIDEO_EXTS = set([".mkv", ".webm", ".mp4"])
IMAGE_EXTS = set([".jpg", ".webp", ".png"])


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


# build a recursive list of all files in a directory
def get_all_files(dir_path):
    all_files = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            all_files.append(os.path.join(root, file))
    return all_files


def move_file(src_dir, dst_dir, filename):
    src_filename = os.path.join(src_dir, filename)
    dst_filename = os.path.join(dst_dir, filename)
    shutil.move(src_filename, dst_filename)
