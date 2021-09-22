import os
import sys
import logging
from uvicorn.server import Server
from uvicorn.config import Config
from uvicorn.supervisors import ChangeReload, Multiprocess
from utils import log
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


class Server(Server):
    async def startup(self, sockets):
        from utils.schedule.apscheduler import scheduler, import_scheduled_job
        import_scheduled_job()
        self._scheduler = scheduler
        self._scheduler.start()
        await super().startup(sockets=sockets)

    def handle_exit(self, sig, frame):
        if self._scheduler.running:
            self._scheduler.shutdown()
        super().handle_exit(sig, frame)


class ChangeReload(ChangeReload):
    def startup(self):
        import signal
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

    def signal_handler(self, sig, frame):
        if self._scheduler.running:
            self._scheduler.shutdown()
        super().signal_handler(sig, frame)


class Multiprocess(Multiprocess):
    def startup(self):
        import signal
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

    def signal_handler(self, sig, frame):
        if self._scheduler.running:
            self._scheduler.shutdown()
        super().signal_handler(sig, frame)


def run(app, **kwargs):
    log.setup_main_logger()
    kwargs['logger'] = log.get_logger()
    config = Config(app, **kwargs)
    server = Server(config=config)

    if (config.reload or config.workers > 1) and not isinstance(app, str):
        logger = logging.getLogger("uvicorn.error")
        logger.warning(
            "You must pass the application as an import string to enable "
            "'reload' or 'workers'.")
        sys.exit(1)

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


__all__ = ('Config', 'ChangeReload', 'Multiprocess', 'run')
