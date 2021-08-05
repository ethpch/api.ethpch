import os
import sys
import logging
from uvicorn.server import Server
from uvicorn.config import Config
from uvicorn.supervisors import ChangeReload, Multiprocess
from utils.log import init_logging, set_global_logger, setup_main_logger
from utils.asyncio import auto_event_loop_policy


class Config(Config):
    def __init__(self, *args, logger=None, **kwargs):
        self._logger = logger
        super().__init__(*args, **kwargs)

    def configure_logging(self):
        super().configure_logging()
        set_global_logger(self._logger)
        init_logging()

    def setup_event_loop(self):
        auto_event_loop_policy()


def run(app, **kwargs):
    from loguru import logger
    logger.remove()
    config = Config(app, logger=logger, **kwargs)
    server = Server(config=config)

    if (config.reload or config.workers > 1) and not isinstance(app, str):
        logger = logging.getLogger("uvicorn.error")
        logger.warning(
            "You must pass the application as an import string to enable "
            "'reload' or 'workers'.")
        sys.exit(1)

    setup_main_logger()
    if config.should_reload:
        sock = config.bind_socket()
        ChangeReload(config, target=server.run, sockets=[sock]).run()
    elif config.workers > 1:
        sock = config.bind_socket()
        Multiprocess(config, target=server.run, sockets=[sock]).run()
    else:
        server.run()
    if config.uds:
        os.remove(config.uds)


__all__ = ('Config', 'run')
