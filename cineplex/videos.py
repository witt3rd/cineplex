import json
import os
from datetime import datetime
import yt_dlp
from cineplex.youtube import youtube_api
from cineplex.db import get_db
from cineplex.logger import Logger
from cineplex.config import Settings

settings = Settings()


# # ℹ️ See the docstring of yt_dlp.postprocessor.common.PostProcessor
# class MyCustomPP(yt_dlp.postprocessor.PostProcessor):
#     # ℹ️ See docstring of yt_dlp.postprocessor.common.PostProcessor.run
#     def run(self, info):
#         self.to_screen('Doing stuff')
#         self.to_screen(f'--{info.keys()}')
#         print(f"_fiilename: {info['_filename']}")
#         print(f"__real_download: {info['__real_download']}")
#         print(f"__finaldir: {info['__finaldir']}")
#         print(f"filpath: {info['filepath']}")
#         print(f"__files_to_move: {info['__files_to_move']}")
#         return [{'qwerty': 'qwerty'}], info


# # ℹ️ See "progress_hooks" in the docstring of yt_dlp.YoutubeDL
# def my_hook(d):
#     if d['status'] == 'finished':
#         print('Done downloading, now converting ...')
#         print(f"{d['filename']}")


def format_selector(ctx):
    """ Select the best video and the best audio that won't result in an mkv.
    This is just an example and does not handle all cases """

    # formats are already sorted worst to best
    formats = ctx.get('formats')[::-1]

    # acodec='none' means there is no audio
    best_video = next(f for f in formats
                      if f['vcodec'] != 'none' and f['acodec'] == 'none')

    # find compatible audio extension
    audio_ext = {'mp4': 'm4a', 'webm': 'webm'}[best_video['ext']]
    # vcodec='none' means there is no video
    best_audio = next(f for f in formats if (
        f['acodec'] != 'none' and f['vcodec'] == 'none' and f['ext'] == audio_ext))

    yield {
        # These are the minimum required fields for a merged format
        'format_id': f'{best_video["format_id"]}+{best_audio["format_id"]}',
        'ext': best_video['ext'],
        'requested_formats': [best_video, best_audio],
        # Must be + seperated list of protocols
        'protocol': f'{best_video["protocol"]}+{best_audio["protocol"]}'
    }


# ℹ️ See docstring of yt_dlp.YoutubeDL for a description of the options
ydl_opts = {
    # 'format': format_selector,
    # 'postprocessors': [{
    #     # Embed metadata in video using ffmpeg.
    #     # ℹ️ See yt_dlp.postprocessor.FFmpegMetadataPP for the arguments it accepts
    #     'key': 'FFmpegMetadata',
    #     'add_chapters': True,
    #     'add_metadata': True,
    # }],
    'logger': Logger(),
    # 'progress_hooks': [my_hook],
    'writethumbnail': True,
    'paths': {
        'home': settings.tmp_dir,
    }
}


# Add custom headers
yt_dlp.utils.std_headers.update({'Referer': 'https://www.google.com'})


def download_video(video_url):
    logger = Logger()
    logger.debug(f"downloading video {video_url=}")

    try:

        # ℹ️ See the public functions in yt_dlp.YoutubeDL for for other available functions.
        # Eg: "ydl.download", "ydl.download_with_info_file"
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # ydl.add_post_processor(MyCustomPP())
            info = ydl.extract_info(video_url)
            # ℹ️ ydl.sanitize_info makes the info json-serializable
            info = ydl.sanitize_info(info)

            # find the thumbnail file
            for t in info['thumbnails']:
                if "filepath" in t:
                    thumbnail_filename = t["filepath"]
                    break

            # use it to derive the other filenames
            basename, _ = os.path.splitext(thumbnail_filename)
            video_filename = f"{basename}.{info['ext']}"
            info_filename = f"{basename}.info.json"

            # write info to file
            with open(info_filename, 'w') as f:
                json.dump(info, f, indent=2)

            return {
                'video_filename': video_filename,
                'info_filename': info_filename,
                'thumbnail_filename': thumbnail_filename,
                'info': info
            }

    except Exception as e:
        logger.error(
            f"unhandled exception downloading video {video_url=}", exc_info=True)
        raise e
