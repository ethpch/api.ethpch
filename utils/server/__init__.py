# Every server should implement "run" function.
# If need reload feature, server should implement "reload" function.
# "reload" function in this module is designed for reloading programtically.
# And this "reload" will only work in the main process.
# "reload" feature for APP should be implement manually.
# Example for uvicorn and fastapi in ./uvicorn.py line 32-39, 187-188, 216-224.

from logging import getLogger
from importlib import import_module
from utils.config import asgi_framework, allow_reload as c_a_reload

logger = getLogger('api_ethpch')
_reload: bool = False


def run(allow_reload: bool = False):
    global _reload
    _reload = allow_reload or c_a_reload
    try:
        module = import_module(f'utils.server.{asgi_framework}')
        run_function = module.run
    except ModuleNotFoundError:
        logger.error(f'Server {asgi_framework} is not implemented!')
    except AttributeError:
        logger.error(
            f'Server {asgi_framework} doesn\'t implement "run" method!')
    else:
        from utils import log
        log.setup_main_logger()
        log.init_logging()
        log.add_stdout()
        if _reload:
            logger.info('Running server with "reload" feature.')
        run_function()


def reload():
    if _reload is True:
        try:
            module = import_module(f'utils.server.{asgi_framework}')
            reload_function = module.reload
        except ModuleNotFoundError:
            logger.error(f'Server {asgi_framework} is not implemented!')
        except AttributeError:
            logger.error(
                f'Server {asgi_framework} doesn\'t implement "reload" method!')
        else:
            reload_function()
    else:
        logger.warning('Reload is not allowed!')
