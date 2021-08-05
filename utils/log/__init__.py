"""
Depend on loguru.
https://github.com/Delgan/loguru
"""
import sys
import logging
from functools import partial
from os.path import splitext
from yaml import safe_load
from constants import LOG_DIR, LOG_CONFIG_PATH
from utils.config import debug

DEFAULT_FORMAT = ('PID: {process: <6} |'
                  ' <green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> |'
                  ' <level>{level: <8}</level> |'
                  ' <cyan>{name}</cyan>:<cyan>{line}</cyan>'
                  ' - <level>{message}</level>')


def _log_filter(record, filter=None):
    if not filter:
        return True
    # filter log by logger name
    logger = record['extra']['_logger_name']
    if logger == filter:
        return True
    elif logger.startswith(filter):
        filter_acceptsub = LOGGER_CONFIG_EXTRA[filter]['acceptsub']
        try:
            logger_propagate = LOGGER_CONFIG_EXTRA[logger]['propagate']
        except KeyError:
            # wildcard match
            if wildcards := sorted([
                    filter for filter in LOGGER_CONFIG_EXTRA.keys()
                    if filter.endswith('.*') and logger.startswith(filter[:-2])
            ]):
                logger_propagate = LOGGER_CONFIG_EXTRA[
                    wildcards[-1]]['propagate']
            else:
                logger_propagate = False
        return filter_acceptsub & logger_propagate
    else:
        return False


LOGGER_CONFIG = {}
LOGGER_CONFIG_EXTRA = {}

for k, v in safe_load(LOG_CONFIG_PATH.read_text()).items():
    if (format := v.get('format', 'default')).lower() == 'default':
        format = DEFAULT_FORMAT
    filter = v.get('filter', k)
    key = filter if filter else k
    if file := v.get('file', None):
        if (rotation := v.get('rotation', None)) and '/' not in file:
            file = splitext(file)[0] + '/' + file
        sink = LOG_DIR / file
        LOGGER_CONFIG[key] = dict(
            sink=sink,
            filter=partial(_log_filter, filter=filter),
            level=v.get('level', 'DEBUG' if debug else 'INFO').upper(),
            format=format,
            encoding=v.get('encoding', 'utf-8'),
            rotation=rotation,
            enqueue=v.get('enqueue', True),
            compression=v.get('compression', 'zip'),
        )
    LOGGER_CONFIG_EXTRA[key] = dict(
        acceptsub=v.get('acceptsub', True),
        propagate=v.get('propagate', False),
    )


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        if (logger := get_logger()) is None:
            from loguru import logger
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level,
            record.getMessage(),
            _logger_name=record.name,  # add logger name to extra parameter
        )


def init_logging():
    intercepts = LOGGER_CONFIG.keys()
    if intercepts:
        # setup handlers to accept logs
        intercept_handler = InterceptHandler()
        for intercept in intercepts:
            logger = logging.getLogger(intercept)
            logger.setLevel(LOGGER_CONFIG[intercept]['level'])
            logger.handlers = [intercept_handler]
            logger.propagate = False
    # add sys.stdout
    get_logger().add(sink=sys.stdout, format=DEFAULT_FORMAT)


def setup_main_logger():
    from loguru import logger
    set_global_logger(logger)
    logger.remove()  # disable sys.stderr
    for _config in LOGGER_CONFIG.values():
        logger.add(**_config, diagnose=debug)


_logger = None


def set_global_logger(logger):
    global _logger
    _logger = logger


def get_logger():
    return _logger


__all__ = ('InterceptHandler', 'init_logging', 'setup_main_logger',
           'set_global_logger', 'get_logger')
