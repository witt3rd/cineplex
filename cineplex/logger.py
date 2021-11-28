import logging
import os
import datetime

from settings import (
    LOG_NAME,
    LOG_DIR,
    LOG_LEVEL
)


class Logger:
    _logger = None

    def __new__(cls, *args, **kwargs):
        if cls._logger is None:
            cls._logger = super().__new__(cls, *args, **kwargs)

            cls._logger = logging.getLogger(LOG_NAME)
            cls._logger.setLevel(LOG_LEVEL)
            formatter = logging.Formatter(
                '%(asctime)-23s | %(levelname)-8s| %(filename)s:%(lineno)s | %(message)s')

            now = datetime.datetime.now()

            if not os.path.isdir(LOG_DIR):
                os.mkdir(LOG_DIR)
            fileHandler = logging.FileHandler(
                os.path.join(LOG_DIR, f'{LOG_NAME}.{now.strftime("%Y-%m-%d")}.log'))

            streamHandler = logging.StreamHandler()

            fileHandler.setFormatter(formatter)
            streamHandler.setFormatter(formatter)

            cls._logger.addHandler(fileHandler)
            cls._logger.addHandler(streamHandler)

        return cls._logger


# a simple usecase
if __name__ == "__main__":
    logger = Logger()
    logger.info("Hello, Logger")
    logger = Logger()
    logger.debug("bug occured")
    logger.warning("warning")
