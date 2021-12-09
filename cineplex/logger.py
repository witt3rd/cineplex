import logging
import os
import datetime
from cineplex.config import Settings


class Logger:
    _logger = None

    def __new__(cls, *args, **kwargs):
        if cls._logger is None:
            cls._logger = super().__new__(cls, *args, **kwargs)

            settings = Settings()

            cls._logger = logging.getLogger(settings.log_name)
            cls._logger.setLevel(settings.log_level)
            formatter = logging.Formatter(
                '%(asctime)-23s | %(levelname)-8s| %(filename)s:%(lineno)s | %(message)s')

            now = datetime.datetime.now()

            if not os.path.isdir(settings.log_dir):
                os.mkdir(settings.log_dir)
            fileHandler = logging.FileHandler(
                os.path.join(settings.log_dir, f'{settings.log_name}.{now.strftime("%Y-%m-%d")}.log'))
            fileHandler.setFormatter(formatter)
            cls._logger.addHandler(fileHandler)

            if settings.log_to_console:
                consoleHandler = logging.StreamHandler()
                consoleHandler.setFormatter(formatter)
                cls._logger.addHandler(consoleHandler)

        return cls._logger

    # Helpers to allow static calls
    @staticmethod
    def debug(msg):
        Logger().debug(msg)

    @staticmethod
    def info(msg):
        Logger().info(msg)

    @staticmethod
    def warning(msg):
        Logger().warning(msg)

    @staticmethod
    def error(msg):
        Logger().error(msg)

    @staticmethod
    def critical(msg):
        Logger().critical(msg)

    @staticmethod
    def exception(e):
        Logger().exception(e)

    # Helpers for uniform reporting

    @staticmethod
    def log_get(what, where, id, detail=None):
        if detail is None:
            Logger().debug(f"getting {what} {id=} from {where}")
        else:
            Logger().debug(f"getting {what} {id=} ({detail}) from {where}")

    @staticmethod
    def log_got(what, where, id, detail=None):
        if detail is None:
            Logger().debug(f"got {what} {id=} from {where}")
        else:
            Logger().debug(f"got {what} {id=} ({detail}) from {where}")

    @staticmethod
    def log_get_batch(what, where, ids, detail=None):
        if detail is None:
            Logger().debug(
                f"getting batch of {len(ids)} {what} from {where}: {ids=}")
        else:
            Logger().debug(
                f"getting batch of {len(ids)} {what} from {where}: {ids=} ({detail})")

    @staticmethod
    def log_got_batch(what, where, batch, detail=None):
        if detail is None:
            Logger().debug(
                f"got batch of {len(batch)} {what} from {where}")
        else:
            Logger().debug(
                f"got batch of {len(batch)} {what} from {where} ({detail})")

    @staticmethod
    def log_save(what, where, id, detail=None):
        if detail is None:
            Logger().debug(f"saving {what} {id=} to {where}")
        else:
            Logger().debug(f"saving {what} {id=} to {where} ({detail})")

    @staticmethod
    def log_saved(what, where, id,  detail=None):
        if detail is None:
            Logger().debug(f"saved {what} {id=} to {where}")
        else:
            Logger().debug(f"saved {what} {id=} to {where} ({detail})")

    @staticmethod
    def log_save_batch(what, where, batch, detail=None):
        if detail is None:
            Logger().debug(
                f"saving batch of {len(batch)} {what} to {where}")
        else:
            Logger().debug(
                f"saving batch of {len(batch)} {what} to {where} ({detail})")

    @staticmethod
    def log_saved_batch(what, where, batch, detail=None):
        if detail is None:
            Logger().debug(
                f"saved batch of {len(batch)} {what} to {where}")
        else:
            Logger().debug(
                f"saved batch of {len(batch)} {what} to {where} ({detail})")


# a simple usecase
if __name__ == "__main__":
    logger = Logger()
    logger.info("Hello, Logger")
    Logger.debug("bug occured")
    Logger.warning("warning")
    try:
        0 / 0
    except Exception as e:
        Logger.exception(e)
