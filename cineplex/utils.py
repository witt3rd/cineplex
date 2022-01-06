from enum import Enum
import json
import os
import shutil
import hashlib
import typer

# --

FILE_BLACKLIST = set([".DS_Store"])
VIDEO_EXTS = set([".mkv", ".webm", ".mp4"])
IMAGE_EXTS = set([".jpg", ".webp", ".png"])

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


def magenta(text):
    return typer.style(text, fg=typer.colors.MAGENTA, bold=True)


class SetEncoder(json.JSONEncoder):
    """ JSON serialization for sets """

    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


def calc_file_sha256(filename):
    """ Python program to find SHA256 hash string of a file """
    sha256_hash = hashlib.sha256()
    with open(filename, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()


def get_all_files(dir_path):
    """ build a recursive list of all files in a directory """
    all_files = []
    for root, _, files in os.walk(dir_path):
        for file in files:
            all_files.append(os.path.join(root, file))
    return all_files


def move_file(src_dir, dst_dir, filename):
    """ Move a file from src_dir to dst_dir """
    src_filename = os.path.join(src_dir, filename)
    dst_filename = os.path.join(dst_dir, filename)
    shutil.move(src_filename, dst_filename)


def missing_found(id_batch, meta_batch):
    """ 
    Check if any of the IDs in the batch are missing from the metadata 
    and return the missing and found IDs.
    """
    if not meta_batch:
        return id_batch, []
    found_id_batch = [x['_id'] for x in meta_batch]
    missing_id_batch = [x for x in id_batch if x not in found_id_batch]
    return missing_id_batch, found_id_batch


def ensure_batch_impl(id_batch, db_fn, sync_fn, force: bool = False):
    """ Get metadata for a batch of IDs from the database or from YouTube. """
    if force:
        return sync_fn(id_batch)

    meta_batch = db_fn(id_batch)
    missing_ids, _ = missing_found(id_batch, meta_batch)
    if missing_ids:
        missing_meta = sync_fn(missing_ids)
        if missing_meta:
            meta_batch.extend(missing_meta)
    return meta_batch
