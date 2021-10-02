import os
import sys
import signal
import logging
import multiprocessing
from threading import Thread
from typing import Union
from uvicorn.server import Server
from uvicorn.config import Config
from uvicorn.supervisors import ChangeReload, Multiprocess
from uvicorn.middleware.debug import DebugMiddleware
from constants import ASGI
from utils import log
from utils.config import server as _server, debug as _debug
from utils.asyncio import auto_event_loop_policy


class Config(Config):
    def __init__(self, *args, logger=None, **kwargs):
        self._logger = logger
        super().__init__(*args, **kwargs)

    def configure_logging(self):
        super().configure_logging()
        log.set_global_logger(self._logger)
        log.init_logging()
        log.add_stdout()

    def setup_event_loop(self):
        auto_event_loop_policy()

    def load(self):
        super().load()
        # access config to FastAPI
        app = self.loaded_app.app
        if isinstance(app, DebugMiddleware):
            app.app._config = self
        else:
            app._config = self


class Server(Server):
    async def startup(self, sockets):
        self._sockets = sockets
        if multiprocessing.parent_process() is None:
            from utils.schedule.apscheduler import (
                scheduler,
                import_scheduled_job,
            )
            import_scheduled_job()
            self._scheduler = scheduler
            self._scheduler.start()
        await super().startup(sockets=self._sockets)

    def handle_exit(self, sig, frame):
        if hasattr(self, '_scheduler') and self._scheduler.running:
            self._scheduler.shutdown()
        super().handle_exit(sig, frame)


class ChangeReload(ChangeReload):
    def startup(self):
        from uvicorn.supervisors.basereload import (
            logger,
            HANDLED_SIGNALS,
            get_subprocess,
        )
        import click

        message = (f"Started reloader process [{self.pid}] using"
                   f" {self.reloader_name}")
        color_message = "Started reloader process [{}] using {}".format(
            click.style(str(self.pid), fg="cyan", bold=True),
            click.style(str(self.reloader_name), fg="cyan", bold=True),
        )
        logger.info(message, extra={"color_message": color_message})

        for sig in HANDLED_SIGNALS:
            signal.signal(sig, self.signal_handler)

        log.remove_sinks('stdout')  # avoid pickle error
        self.process = get_subprocess(config=self.config,
                                      target=self.target,
                                      sockets=self.sockets)
        self.process.start()
        log.add_stdout()

        from utils.schedule.apscheduler import scheduler, import_scheduled_job
        import_scheduled_job()
        self._scheduler = scheduler
        self._scheduler.start()

    def restart(self):
        log.remove_sinks('stdout')
        super().restart()
        log.add_stdout()

    def reload(self):
        self.restart()

    def signal_handler(self, sig, frame):
        if self._scheduler.running:
            self._scheduler.shutdown()
        super().signal_handler(sig, frame)


class Multiprocess(Multiprocess):
    def startup(self):
        from uvicorn.supervisors.multiprocess import (
            logger,
            HANDLED_SIGNALS,
            get_subprocess,
        )
        import click

        message = "Started parent process [{}]".format(str(self.pid))
        color_message = "Started parent process [{}]".format(
            click.style(str(self.pid), fg="cyan", bold=True))
        logger.info(message, extra={"color_message": color_message})

        for sig in HANDLED_SIGNALS:
            signal.signal(sig, self.signal_handler)

        log.remove_sinks('stdout')  # avoid pickle error
        for idx in range(self.config.workers):
            process = get_subprocess(config=self.config,
                                     target=self.target,
                                     sockets=self.sockets)
            process.start()
            self.processes.append(process)
        log.add_stdout()

        from utils.schedule.apscheduler import scheduler, import_scheduled_job
        import_scheduled_job()
        self._scheduler = scheduler
        self._scheduler.start()

    def reload(self):
        from uvicorn.supervisors.multiprocess import logger, get_subprocess
        logger.info('Start server process reloading.')
        for process in self.processes:
            process.terminate()
            process.join()
            logger.info(f'Stopping server process {process.pid}.')
        self.processes = []
        log.remove_sinks('stdout')  # avoid pickle error
        for idx in range(self.config.workers):
            process = get_subprocess(config=self.config,
                                     target=self.target,
                                     sockets=self.sockets)
            process.start()
            self.processes.append(process)
        log.add_stdout()
        logger.info('Reload server process completed.')

    def signal_handler(self, sig, frame):
        if self._scheduler.running:
            self._scheduler.shutdown()
        super().signal_handler(sig, frame)


ProcessManager: Union[ChangeReload, Multiprocess, Server, None] = None


def run():
    log.setup_main_logger()
    app = ':'.join(ASGI)
    kwargs = dict(
        host=_server.host,
        port=_server.port,
        debug=_debug,
        logger=log.get_logger(),
        workers=os.cpu_count(),
    )
    if _server.ssl.enable is True:
        kwargs.update(_server.ssl.dict(exclude='enable'))
    config = Config(app, **kwargs)
    server = Server(config=config)

    if (config.reload or config.workers > 1) and not isinstance(app, str):
        logger = logging.getLogger("uvicorn.error")
        logger.warning(
            "You must pass the application as an import string to enable "
            "'reload' or 'workers'.")
        sys.exit(1)

    from utils.server import _reload
    config._allow_reload = _reload
    global ProcessManager
    if config.should_reload:
        sock = config.bind_socket()
        ProcessManager = ChangeReload(
            config,
            target=server.run,
            sockets=[sock],
        )
    elif config.workers > 1:
        sock = config.bind_socket()
        ProcessManager = Multiprocess(
            config,
            target=server.run,
            sockets=[sock],
        )
    else:
        if _reload is True:
            sock = config.bind_socket()
            Reload = type('Reload', (ChangeReload, ),
                          {'should_restart': lambda self: False})
            ProcessManager = Reload(
                config,
                target=server.run,
                sockets=[sock],
            )
        else:
            ProcessManager = server
    if _reload is True:
        ProcessManager.config._reload_event = event = multiprocessing.Event()

        def reload_listener(event: multiprocessing.Event):
            while event.wait():
                reload()
                event.clear()

        Thread(target=reload_listener, args=(event, ), daemon=True).start()
    ProcessManager.run()
    if config.uds:
        os.remove(config.uds)


def reload():
    if isinstance(ProcessManager, (ChangeReload, Multiprocess)):
        ProcessManager.reload()
    else:
        logging.getLogger('api_ethpch').warning('ProcessManager is unbound.')


__all__ = ('Config', 'ChangeReload', 'Multiprocess', 'run', 'reload',
           'ProcessManager')
