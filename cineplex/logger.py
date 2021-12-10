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
