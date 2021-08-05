import asyncio
from hashlib import md5
from functools import partial
from inspect import iscoroutinefunction
from logging import getLogger
from typing import Coroutine, Dict
from utils.syncutils import run_sync

logger = getLogger('api_ethpch')


class ConcurrencyScheduler(object):
    instances: Dict[str, 'ConcurrencyScheduler'] = {}

    def __init__(self, name: str, limit: int = 10):
        type(self).instances[name] = self
        self.name = name
        self.limit = limit
        self._tasks: Dict[str, asyncio.Task] = {}
        self._pending: Dict[str, Coroutine] = {}
        self.__running = False

    def start(self):
        if self.__running is False:
            if '_limiter' not in self._tasks.keys():
                self._tasks['_limiter'] = asyncio.create_task(self._limiter())
            self.__running = True
            logger.info(f'ConcurrencyScheduler {self.name} start.')

    @classmethod
    def start_all(cls):
        for instance in cls.instances.values():
            instance.start()

    def stop(self):
        if self.__running is True:
            self.__running = False
            logger.info(f'Concurrency Scheduler "{self.name}" stop.')

    @classmethod
    def stop_all(cls):
        for instance in cls.instances.values():
            instance.stop()

    def shutdown(self):
        self.stop()
        self._pending.clear()
        for task in self._tasks.values():
            task.cancel()
        self._tasks.clear()

    @classmethod
    def shutdown_all(cls):
        for instance in cls.instances.values():
            instance.shutdown()

    @property
    def running(self):
        return self.__running

    def add_job(self,
                func,
                id: str = None,
                args: tuple = (),
                kwargs: dict = {}):
        if id is None:
            id = func.__module__ + '.' + func.__name__
        arg_identifier = '-'.join([str(i) for i in args])
        for k, v in kwargs.items():
            arg_identifier += f'({k}, {v})'
        if arg_identifier:
            id = id + '-' + md5(arg_identifier.encode()).hexdigest()
        if id not in self._pending.keys() and id not in self._tasks.keys():
            if iscoroutinefunction(func) is False:
                func = run_sync(func)
            self._pending[id] = func(*args, **kwargs)
            logger.debug(f'Concurrency Scheduler "{self.name}" is '
                         f'pending task {id}.')
            if not self.__running:
                self.start()

    def get_pending(self, id):
        return self._pending[id]

    def del_pending(self, id):
        self._pending.pop(id, None)

    def get_pendings(self):
        return self._pending.copy()

    def get_running(self, id):
        return self._tasks[id]

    def del_running(self, id):
        task = self._tasks.pop(id, None)
        if task and task.done() is False:
            task.cancel()

    def get_runnings(self):
        return self._tasks.copy()

    async def _limiter(self):
        count = 0
        while True:
            if self.__running:
                _to_run = max(self.limit - len(self._tasks.keys()) + 1, 0)
                _scheduled = []
                for id, coro in self._pending.items():
                    if _to_run <= 0:
                        break
                    self._tasks[id] = asyncio.create_task(coro)
                    self._tasks[id].add_done_callback(
                        partial(self._remove_task, id))
                    logger.debug(f'Concurrency Scheduler "{self.name}" is '
                                 f'running task {id}.')
                    _scheduled.append(id)
                    _to_run -= 1
                for id in _scheduled:
                    self._pending.pop(id)
            await asyncio.sleep(1)
            count += 1
            if count % 600 == 0:
                for k, v in self._tasks.items():
                    if v.done():
                        self._tasks.pop(k)

    def _remove_task(self, id: str, *args, **kwargs):
        try:
            self._tasks[id].result()
        except asyncio.CancelledError:
            logger.warning(f'Task {id} is cancelled.')
        except asyncio.TimeoutError:
            logger.warning(f'Task {id} runs timeout.')
        except Exception as e:
            logger.error(
                f'Task {id} raises unexpected exception: '
                f'error type: {type(e)}, '
                f'error message: {e}. ')
        self._tasks.pop(id, None)
        logger.debug(f'Concurrency Scheduler "{self.name}" '
                     f'completes task {id}.')


__all__ = ('ConcurrencyScheduler', )
